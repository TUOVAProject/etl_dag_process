from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import logging
import json
import pandas as pd
import numpy as np
from dateutil.parser import parse
import re
from collections import defaultdict
from babel.numbers import parse_decimal

from decimal import Decimal
from typing import Optional, Dict, List, Mapping, Sequence, Tuple
from pathlib import Path
from airflow.utils.context import Context

from enum import Enum

from database.connection import get_engine
from database.writers import save_df, truncate_fact_tables, clear_table
from fetchers.common import fetch_common
from fetchers.inventory import fetch_inventory
from fetchers.manufacturing import fetch_manufacturing
from fetchers.sales import fetch_sales
from fetchers.purchases import fetch_purchases
from fetchers.time_recording import fetch_time_recording
from auth.auth import get_token

from config.erp_config import (
    ERP_SOURCE_CONFIG,
    combine_inventory_tables,
    combine_manufacturing_tables,
    combine_sales_tables,combine_purchase_tables,
    combine_work_interval_tables
)

FACT_TABLES = [
    "fact_order_rows",
    "fact_workphases",
    "fact_machine_events",
]


logger = logging.getLogger("custom_logger")
########## Global Google Cloud Setup ##########

LOCAL_BASE_PATH = "results/raw"
LOCAL_STAGED_PATH = Path("results/staged")
LOCAL_COMBINED_PATH = Path("results/combined")


DATE_FMT = "%Y-%m-%d"
LOG_FILE = Path(__file__).resolve().parent / "app.log"

# Airflow related configs

"""SNOWFLAKE_CONN_ID = "tuova_conn"
DB = "TUOVA_DB"
SCHEMA = "ETL_STAGE"
CONTROL_TABLE = "FILE_LOAD_MAP"               # TUOVA_DB.ETL_STAGE.FILE_LOAD_MAP
EXT_STAGE = "EXT_GCS_TUOVA"                   # TUOVA_DB.ETL_STAGE.EXT_GCS_TUOVA
FILE_FORMAT = "PARQUET"                       # TUOVA_DB.ETL_STAGE.PARQUET
"""

class LoadMode(Enum):
    STAGED = "staged"
    COMBINED = "combined"

UNKNOWN_DATE_KEY = 0
MAX_DATE_KEY = 20351231

def get_date_key(dt):
    """
    Convert to integer YYYYMMDD with <NA> for missing.
    Accepts scalar or pandas Series. Handles mixed timezones safely.
    """

    if isinstance(dt, pd.Series):
        s = dt.astype("string")
        s = pd.to_datetime(s, errors="coerce", utc=True)
        s = s.dt.tz_convert(None)

        out = pd.to_numeric(
            s.dt.strftime("%Y%m%d"),
            errors="coerce"
        ).astype("Int64")

        # Virheelliset päivämäärät -> Unknown
        out = out.where(
            (out.isna()) | (out <= MAX_DATE_KEY),
            UNKNOWN_DATE_KEY,
        )

        return out

    if pd.isnull(dt):
        return pd.NA

    if isinstance(dt, str):
        ts = pd.to_datetime(dt, errors="coerce", utc=True)

        if pd.isnull(ts):
            return pd.NA

        ts = ts.tz_convert(None)

        key = int(ts.strftime("%Y%m%d"))

        return (
            UNKNOWN_DATE_KEY
            if key > MAX_DATE_KEY
            else key
        )

    if hasattr(dt, "tzinfo"):
        dt = dt.tz_convert("UTC").tz_localize(None)

    key = int(pd.Timestamp(dt).strftime("%Y%m%d"))

    return (
        UNKNOWN_DATE_KEY
        if key > MAX_DATE_KEY
        else key
    )


def to_hhmmss(series):
    ts = pd.to_datetime(series, errors="coerce", utc=True)

    return (
        ts.dt.strftime("%H%M%S")
        .pipe(pd.to_numeric, errors="coerce")
        .astype("Int64")
    )


def to_yyyymmdd(series):
    ts = pd.to_datetime(series, errors="coerce", utc=True)

    return (
        ts.dt.strftime("%Y%m%d")
        .pipe(pd.to_numeric, errors="coerce")
        .astype("Int64")
    )

def ticks_to_minutes(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    return (numeric / 600_000_000).round()


def to_int64(series: pd.Series) -> pd.Series:
    
    series = series.astype("string").str.strip()

    series = series.replace("", pd.NA)

    return pd.to_numeric(series, errors="coerce").astype("Int64")

def to_int32(series: pd.Series) -> pd.Series:
    
    series = series.astype("string").str.strip()

    series = series.replace("", pd.NA)

    return pd.to_numeric(series, errors="coerce").astype("Int32")

def to_float64(series: pd.Series) -> pd.Series:
    if pd.api.types.is_string_dtype(series) or pd.api.types.is_object_dtype(series):
        series = series.astype("string").str.strip().replace("", pd.NA)
    return pd.to_numeric(series, errors="coerce").astype("Float64")


def load_json_with_converters(file_path, euro_columns=list(), dtypes=dict()):
    """
    Loads a single JSON file and converts euro-formatted numbers and dtypes.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in {file_path}: {e}")
        return None

    # If the JSON is a single object, wrap into list for DataFrame compatibility
    if isinstance(data, dict):
        data = [data]

    df = pd.DataFrame(data)

    # Handle euro-format columns: "1.234,56" → 1234.56
    for col in euro_columns:
        if col in df:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(".", "", regex=False)   # thousands
                .str.replace(",", ".", regex=False)  # decimal
                .astype(float)
            )

    # Cast datatypes if provided
    for col, dtype in dtypes.items():
        if col in df:
            try:
                df[col] = df[col].astype(dtype)
            except Exception as e:
                logger.warning(f"Failed to convert column {col} to {dtype} in {file_path}: {e}")

    return df


###### Airflow things ##########

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
########## Tasks ##########


def replace_nan(obj):
    if isinstance(obj, float) and pd.isna(obj):
        return None
    if isinstance(obj, dict):
        return {k: replace_nan(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [replace_nan(v) for v in obj]
    return obj


def save_clean_parquet(endpoint: str, df: pd.DataFrame, base_path: Path):
    endpoint_name = endpoint.lstrip("/").replace("/", "-") or "root"

    base_path.mkdir(parents=True, exist_ok=True)

    date_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"{date_str}-{endpoint_name}.parquet"
    full_path = base_path / filename

    df.to_parquet(full_path,index=False)

    logger.info(f"Saved cleaned data: {full_path}")
    return full_path


def clean_parquet_history(endpoint: str, base_path: Path, days_to_keep: int) -> None:
    """Keep only the last N parquet files for the given endpoint."""
    endpoint_name = endpoint.lstrip("/").replace("/", "-") or "root"
    folder = Path(base_path)
    filename_pattern = re.compile(
        rf"(?P<date>\d{{4}}-\d{{2}}-\d{{2}})-{re.escape(endpoint_name)}\.parquet$"
    )

    if not folder.exists():
        return

    files = []
    for f in folder.glob(f"*-{endpoint_name}.parquet"):
        try:
            match = filename_pattern.match(f.name)
            if match is None:
                continue

            date_part = match.group("date")
            dt = datetime.strptime(date_part, "%Y-%m-%d").date()
            files.append((dt, f))
        except Exception:
            continue

    if not files:
        return

    files.sort(key=lambda x: x[0], reverse=True)

    keep = files[:days_to_keep]
    delete = files[days_to_keep:]

    logger.info(f"[clean_parquet_history] Keeping: {[f.name for _, f in keep]}")
    logger.info(f"[clean_parquet_history] Deleting: {[f.name for _, f in delete]}")

    for _, old_file in delete:
        old_file.unlink(missing_ok=True)


def extract_source_and_endpoint(file_path: Path) -> Tuple[str, str]:
    endpoint = file_path.parent.parent.name
    source_type = file_path.parent.parent.parent.name
    return source_type, endpoint

def process_erp_files(file_path: Path):
    """Process ERP workcenter files."""
    source_type, endpoint = extract_source_and_endpoint(file_path)

    df = pd.read_parquet(file_path)

    try:
        columns = ERP_SOURCE_CONFIG[source_type][endpoint]
    except KeyError:
        raise ValueError(
            f"No schema for source_type={source_type}, endpoint={endpoint}"
        )

    df = df[[c for c in columns if c in df.columns]]
    logger.info("Saveing parquet : ", LOCAL_STAGED_PATH / source_type)
    saved_parquet = save_clean_parquet(endpoint, df, LOCAL_STAGED_PATH / source_type)
    clean_parquet_history(endpoint, LOCAL_STAGED_PATH / source_type, days_to_keep=2)
    return saved_parquet


def _parse_date_dir(name: str):
    """Palauttaa datetime-olion jos nimi on YYYY-MM-DD, muuten None."""
    try:
        return datetime.strptime(name, DATE_FMT).date()
    except ValueError:
        return None

@task(multiple_outputs=True,trigger_rule=TriggerRule.ALL_SUCCESS)
def get_latest_endpoint_files_task(endpoint : str) -> Dict[str,List[str]]:
    raw = get_latest_endpoint_files(endpoint)
    return {ep: [str(p) for p in paths] for ep, paths in raw.items()}


def get_latest_endpoint_files(endpoint: str) -> Dict[str, List[Path]]:
    
    dir = Path(LOCAL_BASE_PATH) / endpoint
    if not dir.is_dir():
        return {}

    files_by_endpoint: Dict[str, List[Path]] = defaultdict(list)

    for endpoint_dir in dir.iterdir():
        if not endpoint_dir.is_dir():
            continue

        endpoint_name = endpoint_dir.name

        date_dirs = []
        for child in endpoint_dir.iterdir():
            if child.is_dir():
                d = _parse_date_dir(child.name)
                if d is not None:
                    date_dirs.append((d, child))

        if not date_dirs:
            continue

        latest_date, latest_dir = max(date_dirs, key=lambda t: t[0])

        for file_path in latest_dir.iterdir():
            if file_path.is_file():
                files_by_endpoint[endpoint_name].append(file_path)

    return dict(files_by_endpoint)


def load_parquet_safely(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    return df


def load_and_restrict(path: Path, required_cols: List[str]) -> pd.DataFrame:
    df = load_parquet_safely(path)
    keep = [col for col in required_cols if col in df.columns]
    return df[keep].copy()


def load_latest(
    base: Path,
    prefix: str,
    mode: LoadMode = LoadMode.STAGED,
    required_cols: Optional[List[str]] = None
):
    patt = re.compile(r"(\d{4}-\d{2}-\d{2})-" + re.escape(prefix) + r"\.parquet$")
    candidates = []

    for file in base.glob("*.parquet"):
        m = patt.match(file.name)
        if m:
            candidates.append((m.group(1), file))

    if not candidates:
        raise FileNotFoundError(f"No staged files for {prefix}")

    candidates.sort(key=lambda x: x[0])
    latest_date, filepath = candidates[-1]

    if mode == LoadMode.STAGED:
        if required_cols is not None:
            df = load_and_restrict(filepath, required_cols)
        else:
            df = load_parquet_safely(filepath)
    elif mode == LoadMode.COMBINED:
        df = load_parquet_safely(filepath)
    else:
        raise ValueError(f"Unknown mode '{mode}'. Use 'staged' or 'combined'.")

    return latest_date, df

def save_combined_parquet(df: pd.DataFrame, date_str: str, outdir: Path, endpoint :str):
    outdir.mkdir(parents=True, exist_ok=True)
    file_name = f"Combined{endpoint.capitalize()}.parquet"
    output_file = outdir / f"{date_str}-{file_name}"
    df.to_parquet(output_file,index=False)
    logger.info("Saved combined:", output_file)
    return str(output_file)


"""For error handling in debugging, might delete later
def check_duplicates(df: pd.DataFrame):
    dups = df.columns[df.columns.duplicated(keep=False)]
    logger.info("Duplikaattikolumnit:", list(dups))"""

def fill_value_from_parent_row(
    df: pd.DataFrame,
    id_col: str = "OrderRowId",
    parent_col: str = "ParentRowId",
    value_col: str = "PartId",
    max_iterations: int = 5
) -> pd.DataFrame:
    """
    Täyttää puuttuvat PartId-arvot hakemalla ne emo-riviltä ParentOrderRowId:n
    kautta (esim. kokoonpanon alarivit, jotka eivät kanna omaa PartId:tä).

    Iteroi useamman kierroksen, jos ketju on moniportainen (rivi -> emorivi ->
    isoisorivi jne.), ja pysähtyy kun mitään uutta ei enää saada täytettyä.
    """
    if id_col not in df.columns or parent_col not in df.columns or value_col not in df.columns:
        logger.info(
            "fill_partid_from_parent_row: sarake puuttuu (%s/%s/%s), fallback ohitetaan",
            id_col, parent_col, value_col
        )
        return df

    df = df.copy()

    for _ in range(max_iterations):
        missing_mask = df[value_col].isna() & df[parent_col].notna()
        if not missing_mask.any():
            break

        lookup = df.set_index(id_col)[value_col]
        filled = df.loc[missing_mask, parent_col].map(lookup)

        if filled.notna().sum() == 0:
            break

        df.loc[missing_mask, value_col] = filled

    return df

@task(trigger_rule=TriggerRule.ALL_SUCCESS)
def combine_sales_data_task(staged_path: str, manufacturing_path: str, mapping: dict):
    return combine_sales_data(Path(staged_path),Path(manufacturing_path),mapping)


def combine_sales_data(staged_path: Path,manufacturing_path : Path, mapping: dict):
    date_rows, rows = load_latest(staged_path, "CustomerOrderRows",LoadMode.STAGED, mapping["CustomerOrderRows"])
    date_orders, orders = load_latest(staged_path, "CustomerOrders",LoadMode.STAGED, mapping["CustomerOrders"])
    date_customers, customers = load_latest(staged_path, "Customers",LoadMode.STAGED, mapping["Customers"])
    _, invoices = load_latest(staged_path, "InvoiceLogs",LoadMode.STAGED, mapping["InvoiceLogs"])
    _, manufacturing = load_latest(manufacturing_path, "ManufacturingOrders",LoadMode.STAGED,mapping["ManufacturingOrders"])
    _, manufacturing_operations = load_latest(manufacturing_path, "ManufacturingOrderOperations",LoadMode.STAGED,mapping["ManufacturingOrderOperations"])

    if len({date_rows, date_orders, date_customers}) > 1:
        logger.info("⚠ WARNING: Source dates differ! %s %s %s", date_rows, date_orders, date_customers)


    rows = rows.rename(columns={
        "Id": "OrderRowId",
        "OrderDate": "OrderRowDate",
        "Status" : "CustomerOrderRowStatus"
    })

    rows = fill_value_from_parent_row(rows)
    rows = fill_value_from_parent_row(rows, value_col="LinkedManufacturingOrderId")
    rows = rows[rows["OrderRowType"] != 4]
    orders = orders.rename(columns={
        "Id": "OrderId",
        "Status" : "CustomerOrderStatus"
    })

    customers = customers.rename(columns={
        "Id": "CustomerRecordId"
    })

    invoices = invoices.rename(columns={
        "Id" : "InvoiceId",
        "CustomerOrderId" : "InvoiceOrderId",
        "Price" : "InvoicePrice",
        "LoggingTimestamp" : "InvoiceTimestamp"
    })

    manufacturing = manufacturing.rename(columns={
        "Id" : "ManufacturingOrderId",
        "CustomerOrderId" : "ManufacturingCustomerOrderId",
        "OrderNumber" : "ManufacturingOrderNumber",
        "Status" : "ManufacturingOrderStatus"
    })

    manufacturing_operations = manufacturing_operations.rename(columns={
        "Id" : "OperationId",
        "ManufacturingOrderId" : "OperationManufacturingOrderId"
    })

    
    invoice_agg = (
        invoices
        .groupby("InvoiceOrderId", as_index=False)
        .agg({
            "InvoiceDate" : "first",
            "InvoicePrice": "sum",
            "InvoicedQuantity": "sum",
            "InvoiceTimestamp" : "max"
        })
    )
    
    manufacturing_agg = (
        manufacturing
        .groupby("ManufacturingCustomerOrderId", as_index=False)
        .agg({
            "ManufacturingOrderNumber" : "first",
            "ManufacturingOrderStatus" : "first",
            "StartDate": "max",
            "FinishDate": "max",
            "InitialFinishDate": "max"
        })
    )
    
    manufacturing_operations["ActualStartDate"] = pd.to_datetime(
        manufacturing_operations["ActualStartDate"],
        errors="coerce",
        utc=True
    )

    manufacturing_operations["ActualFinishDate"] = pd.to_datetime(
        manufacturing_operations["ActualFinishDate"],
        errors="coerce",
        utc=True
    )

    manufacturing_operation_agg = (
        manufacturing_operations
        .groupby("OperationManufacturingOrderId", as_index=False)
        .agg({
            "ActualStartDate": "max",
            "ActualFinishDate": "max"
        })
    )

    manufacturing_with_ops = manufacturing.merge(
        manufacturing_operation_agg,
        left_on="ManufacturingOrderId",
        right_on="OperationManufacturingOrderId",
        how="left"
    )

    # Huom: manufacturing_agg lasketaan jo yllä (rivit ~590-598) täsmälleen
    # samalla groupby/agg-logiikalla, joten toista, identtistä laskentaa
    # ei tarvita tässä uudelleen.

    manufacturing_full_agg = (
        manufacturing_with_ops
        .groupby("ManufacturingCustomerOrderId", as_index=False)
        .agg({
            "ActualStartDate": "max",
            "ActualFinishDate": "max"
        })
    )

    merged = rows.merge(
        orders,
        left_on="ParentOrderId",
        right_on="OrderId"
    )

    merged = merged.merge(
        manufacturing_agg,
        left_on="OrderId",
        right_on="ManufacturingCustomerOrderId",
        how="left"
    )

    merged = merged.merge(
        manufacturing_full_agg,
        left_on="OrderId",
        right_on="ManufacturingCustomerOrderId",
        how="left"
    )

    merged = merged.merge(
        invoice_agg,
        left_on="OrderId",
        right_on="InvoiceOrderId",
        how="left"
    )


    merged = merged.merge(
        customers,
        left_on="CustomerId",
        right_on="CustomerRecordId"
    )
    
    for col in merged.select_dtypes(include=["datetimetz"]).columns:
        merged[col] = merged[col].dt.tz_localize(None)

    date_str = datetime.now().strftime("%Y-%m-%d")
    endpoint = "sales"
    saved_parquet =  save_combined_parquet(merged,date_str,LOCAL_COMBINED_PATH / endpoint,endpoint)
    clean_parquet_history(endpoint, LOCAL_COMBINED_PATH / endpoint, days_to_keep=2)
    return saved_parquet


@task(trigger_rule=TriggerRule.ALL_SUCCESS)
def combine_material_data_task(combined_sales_path:str,staged_path: str, mapping: dict):
    return combine_material_data(Path(combined_sales_path),Path(staged_path),mapping)


def combine_material_data(combined_sales_path: Path,staged_path: Path, mapping: dict):
    combined_sales = pd.read_parquet(combined_sales_path)
    date_materials, materials = load_latest(staged_path, "StockTransactions",LoadMode.STAGED, mapping["StockTransactions"])

    if len({date_materials}) > 1:
        logger.info("⚠ WARNING: Source dates differ!", date_materials)


    merged = materials.merge(
        combined_sales,
        left_on="ParentOrderId",
        right_on="OrderId"
    )

    date_str = datetime.now().strftime("%Y-%m-%d")
    endpoint = "sales"
    saved_parquet =  save_combined_parquet(merged,date_str,LOCAL_COMBINED_PATH / endpoint,endpoint)
    clean_parquet_history(endpoint, LOCAL_COMBINED_PATH / endpoint, days_to_keep=2)
    return saved_parquet



    

@task(trigger_rule=TriggerRule.ALL_SUCCESS)
def combine_inventory_data_task(staged_path: str, mapping: dict):
    return combine_inventory_data(Path(staged_path),mapping)

def combine_inventory_data(staged_path: Path, mapping: dict):

    date_codes, part_codes = load_latest(staged_path, "PartCodes",LoadMode.STAGED, mapping["PartCodes"])
    date_parts, parts = load_latest(staged_path, "Parts",LoadMode.STAGED, mapping["Parts"])

    if len({date_codes, date_parts}) > 1:
        logger.info("⚠ WARNING: Source dates differ! %s %s", date_codes, date_parts)

    merged = parts.merge(
        part_codes,
        left_on="PartCodeId",
        right_on="Id",
        how="left",
        suffixes=("_part", "_code")
    )

    merged = merged.drop(columns=["Id_code"], errors="ignore")

    date_str = datetime.now().strftime("%Y-%m-%d")
    endpoint = "inventory"

    saved_parquet =  save_combined_parquet(merged,date_str,LOCAL_COMBINED_PATH / endpoint,endpoint)
    clean_parquet_history(endpoint, LOCAL_COMBINED_PATH / endpoint, days_to_keep=2)
    return saved_parquet

@task(trigger_rule=TriggerRule.ALL_SUCCESS)
def combine_manufacturing_data_task(staged_manufacturing_path: str,staged_commmon_path : str,staged_sales_path:str, mapping: dict):
    return combine_manufacturing_data(Path(staged_manufacturing_path),Path(staged_commmon_path),Path(staged_sales_path), mapping)

def combine_manufacturing_data(staged_manufacturing_path: Path, staged_common_path: Path, staged_sales_path: Path, mapping: dict):

    _, manufacturing_orders = load_latest(
        staged_manufacturing_path, "ManufacturingOrders", LoadMode.STAGED, mapping["ManufacturingOrders"]
    )
    _, orderOperations = load_latest(
        staged_manufacturing_path, "ManufacturingOrderOperations", LoadMode.STAGED, mapping["ManufacturingOrderOperations"]
    )
    _, operationRows = load_latest(
        staged_manufacturing_path, "OperationRows", LoadMode.STAGED, mapping["OperationRows"]
    )

    _, workCenters = load_latest(
        staged_manufacturing_path, "WorkCenters", LoadMode.STAGED, mapping["WorkCenters"]
    )

    _, extraFields = load_latest(
        staged_common_path, "ExtraFields", LoadMode.STAGED, mapping["ExtraFields"]
    )

    _, extraFieldGroups = load_latest(
        staged_common_path, "ExtraFieldGroups", LoadMode.STAGED, mapping["ExtraFieldGroups"]
    )

    _, extraFieldOptionTemplates = load_latest(
        staged_common_path, "ExtraFieldOptionTemplates", LoadMode.STAGED, mapping["ExtraFieldOptionTemplates"]
    )

    _, extraFieldTemplates = load_latest(
        staged_common_path, "ExtraFieldTemplates", LoadMode.STAGED, mapping["ExtraFieldTemplates"]
    )
    _, rows = load_latest(
        staged_sales_path, "CustomerOrderRows",LoadMode.STAGED, mapping["CustomerOrderRows"]
    )

    manufacturing_orders = manufacturing_orders.rename(columns={
        "Id": "ManufacturingOrderId"
    })

    orderOperations = orderOperations.rename(columns={
        "Id": "ManufacturingOrderOperationId",
        "ReportedQuantity": "OperationReportedQuantity",
        "PlannedQuantity": "OperationPlannedQuantity",
        "PartId": "OperationPartId"
    })

    operationRows = operationRows.rename(columns={
        "Id": "OperationRowId"
    })

    
    workCenters = workCenters.rename(columns={
        "Id": "WorkCenterId",
        "Number": "WorkCenterNumber"
    })

    rows = rows.rename(columns={
        "Id": "CustomerOrderRowId"
    })

    rows = (
        rows[["CustomerOrderRowId", "LinkedManufacturingOrderId"]]
        .drop_duplicates(subset=["LinkedManufacturingOrderId"])
    )

    manufacturing_orders = manufacturing_orders.drop_duplicates(subset=["ManufacturingOrderId"])

    manufacturing_orders = manufacturing_orders.merge(
        rows[["CustomerOrderRowId", "LinkedManufacturingOrderId"]],
        left_on="ManufacturingOrderId",
        right_on="LinkedManufacturingOrderId",
        how="left"
    )


    part_extrafields = resolve_part_extrafields_by_group_name(
        extraFieldGroups=extraFieldGroups,
        extraFieldTemplates=extraFieldTemplates,
        extraFieldOptionTemplates=extraFieldOptionTemplates,
        extraFields=extraFields,
        group_name="lisätiedot"
    )

    manufacturing_orders = manufacturing_orders.merge(
    part_extrafields,
    on="PartId",
    how="left"
    )

    merged = (
        orderOperations
        .merge(
            manufacturing_orders,
            on="ManufacturingOrderId",
            how="left"
        )
        .merge(
            operationRows,
            on="OperationRowId",
            how="left"
        )
        .merge(
            workCenters,
            on="WorkCenterId",
            how="left"

        )
    )

    merged["ActualFinishDate"] = pd.to_datetime(
        merged["ActualFinishDate"],
        errors="coerce",
        utc=True
    )

    merged["FinishYear"] = merged["ActualFinishDate"].dt.year

    time_cols = [
        "ReportedUnitTime",
        "ReportedSetupTime",
        "PlannedUnitTime",
        "PlannedSetupTime",
    ]

    for col in time_cols:
        if col in merged.columns:
            merged[col] = (
                merged[col]
                .astype("string")
                .str.strip()
                .str.replace(",", ".", regex=False)
            )
            merged[col] = pd.to_numeric(merged[col], errors="coerce")


    date_str = datetime.now().strftime("%Y-%m-%d")

    merged["ActualFinishDate"] = merged["ActualFinishDate"].dt.tz_localize(None)

    stamping_agg = (
        merged
        .groupby("ManufacturingOrderOperationId", as_index=False)
        .agg(
            PlannedUnitTimeSum=("PlannedUnitTime", "sum"),
            PlannedSetupTimeSum=("PlannedSetupTime", "sum"),
            ReportedUnitTimeSum=("ReportedUnitTime", "sum"),
            ReportedSetupTimeSum=("ReportedSetupTime", "sum")
        )
    )

    
    merged = merged.merge(
        stamping_agg,
        on="ManufacturingOrderOperationId",
        how="left"
    )

    endpoint = "manufacturing"
    saved_parquet = save_combined_parquet(
        merged,
        date_str,
        LOCAL_COMBINED_PATH / endpoint,
        endpoint
    )

    clean_parquet_history(endpoint, LOCAL_COMBINED_PATH / endpoint, days_to_keep=2)


    return saved_parquet


def resolve_part_extrafields_by_group_name(
    extraFieldGroups: pd.DataFrame,
    extraFieldTemplates: pd.DataFrame,
    extraFieldOptionTemplates: pd.DataFrame,
    extraFields: pd.DataFrame,
    group_name: str
) -> pd.DataFrame:
    """
    Palauttaa DataFrame:n, jossa:
      - PartId
      - ExtraFieldTemplateId
      - ExtraFieldOptionTemplateId
      - (optionaalisesti option nimi)

    ParentId tulkitaan PartId:ksi metadataketjun perusteella.
    """

    #ExtraFieldGroup
    groups = extraFieldGroups[
        extraFieldGroups["Name"].str.lower() == group_name.lower()
    ]
    if groups.empty:
        raise ValueError(f"ExtraFieldGroup '{group_name}' ei löytynyt")

    group_ids = groups["Id"]

    #ExtraFieldTemplates ryhmän alta
    templates = extraFieldTemplates[
        extraFieldTemplates["ParentId"].isin(group_ids)
    ]
    if templates.empty:
        raise ValueError(f"ExtraFieldTemplates puuttuu ryhmälle '{group_name}'")


    #OptionTemplatet
    options = extraFieldOptionTemplates[
        extraFieldOptionTemplates["ExtraFieldTemplateId"].isin(templates["Id"])
    ]
    if options.empty:
        raise ValueError(f"ExtraFieldOptionTemplates puuttuu ryhmälle '{group_name}'")

    #ExtraFields SelectedOptionId:n kautta
    fields = extraFields[
        extraFields["SelectedOptionId"].isin(options["Id"])
    ].copy()

    if fields.empty:
        return pd.DataFrame(
            columns=[
                "PartId",
                "ExtraFieldTemplateId",
                "ExtraFieldOptionTemplateId"
            ]
        )

    #varmistetaan että ryhmä kohdistuu yhteen rekisteriin
    parent_classes = fields["ParentClass"].unique()
    if len(parent_classes) != 1:
        raise ValueError(
            f"Ryhmä '{group_name}' kohdistuu useaan ParentClassiin: {parent_classes}"
        )
    
    fields = fields.rename(columns={
        "ParentId": "PartId",
        "SelectedOptionId": "ExtraFieldOptionTemplateId"
    })

    fields = fields.merge(
        options[["Id", "ExtraFieldTemplateId", "Description","Code"]],
        left_on="ExtraFieldOptionTemplateId",
        right_on="Id",
        how="left",
        suffixes=("", "_Option")
    )

    return fields[[
        "PartId",
        "ExtraFieldTemplateId",
        "ExtraFieldOptionTemplateId",
        "Description",
        "Code"
    ]]


@task(trigger_rule=TriggerRule.ALL_SUCCESS)
def combine_purchase_data_task(staged_path: str, mapping: dict):
    return combine_purchase_data(Path(staged_path),mapping)

def combine_purchase_data(staged_path: Path, mapping: dict):
    _, purchase_orders = load_latest(
        staged_path,
        "PurchaseOrders",
        LoadMode.STAGED,
        mapping["PurchaseOrders"]
    )

    _, purchase_order_rows = load_latest(
        staged_path,
        "PurchaseOrderRows",
        LoadMode.STAGED,
        mapping["PurchaseOrderRows"]
    )


    purchase_orders = purchase_orders.rename(columns={
        "Id": "PurchaseOrderId",
        "OrderDate" : "PurchaseOrderDate",
        "WarehouseId" : "PurchaseOrderWarehouseId",
        "IsConfirmed" : "PurchaseOrderIsConfirmed"
    })

    purchase_order_rows = purchase_order_rows.rename(columns={
        "Id": "PurchaseOrderRowId",
        "OrderDate" : "PurchaseOrderRowDate",
        "WarehouseId" : "PurchaseOrderRowWarehouseId",
        "IsConfirmed" : "PurchaseOrderRowIsConfirmed"
    })

    
    merged = purchase_order_rows.merge(
        purchase_orders,
        left_on="ParentOrderId",
        right_on="PurchaseOrderId",
        how="left"
    )

    date_str = datetime.now().strftime("%Y-%m-%d")
    endpoint = "purchase"

    saved = save_combined_parquet(
        merged,
        date_str,
        LOCAL_COMBINED_PATH / endpoint,
        endpoint
    )

    clean_parquet_history(endpoint, LOCAL_COMBINED_PATH / endpoint, days_to_keep=7)

    return saved

TIME_DIVISOR = 36_000_000_000


def _series_or_zero(df: pd.DataFrame, col: str) -> pd.Series:
    """Palauttaa sarakkeen Seriesinä tai nollasarjan samalle indeksille."""
    if col in df.columns:
        return pd.to_numeric(df[col], errors="coerce").fillna(0)
    return pd.Series(0, index=df.index, dtype="float64")


def _clean_datetime_column(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """Muuttaa sarakkeen datetimeksi ja poistaa timezone-tiedon Excel-yhteensopivuuden vuoksi."""
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce")

        try:
            if getattr(df[col].dt, "tz", None) is not None:
                df[col] = df[col].dt.tz_localize(None)
        except Exception:
            pass

    return df

def _clean_key_series(series: pd.Series) -> pd.Series:
    s = series.astype("string").str.strip()
    s = s.replace({
        "": np.nan,
        "nan": np.nan,
        "None": np.nan,
        "NULL": np.nan,
        "null": np.nan
    })
    return s


@task(trigger_rule=TriggerRule.ALL_SUCCESS)
def combine_work_interval_data_task(staged_path: str, staged_manufacturing_path : str, mapping: dict):
    return combine_work_interval_data(Path(staged_path),Path(staged_manufacturing_path),mapping)

def combine_work_interval_data(
        staged_work_interval_path: Path,
        staged_manufacturing_path: Path,
        mapping: dict):

    _, manufacturing_orders = load_latest(
            staged_manufacturing_path,
            "ManufacturingOrders",
            LoadMode.STAGED,
            mapping["ManufacturingOrders"]
        )
    
    _, order_operations = load_latest(
            staged_manufacturing_path,
            "ManufacturingOrderOperations",
            LoadMode.STAGED,
            mapping["ManufacturingOrderOperations"]
        )
    
    _, work_intervals = load_latest(
            staged_work_interval_path,
            "WorkIntervals",
            LoadMode.STAGED,
            mapping["WorkIntervals"]
        )

    order_operations = order_operations.rename(columns={
        "Id": "OrderOperationId",
        "PartId" : "OrderOperationPartId",
        
        })
    manufacturing_orders = manufacturing_orders.rename(columns={
        "Id": "Id_manufacturing_order",
        "PartId" : "ManufacturingOrderPartId"
            
        })

    manufacturing_orders = manufacturing_orders.drop_duplicates(subset=["Id_manufacturing_order"])
    
    work_fact = (
        work_intervals
        .merge(
            order_operations,
            left_on="OperationId",
            right_on="OrderOperationId",
            how="left"
        )
        .merge(
            manufacturing_orders,
            left_on="ManufacturingOrderId",
            right_on="Id_manufacturing_order",
            how="left",
            suffixes=("_op", "_order")
        )
    )
    date_str = datetime.now().strftime("%Y-%m-%d")
    endpoint = "time_recording"
    saved_parquet = save_combined_parquet(
        work_fact,
        date_str,
        LOCAL_COMBINED_PATH / endpoint,
        endpoint
    )
    
    clean_parquet_history(endpoint, LOCAL_COMBINED_PATH / endpoint, days_to_keep=2)
    
    
    return saved_parquet


@task(trigger_rule=TriggerRule.ALL_SUCCESS)
def process_and_stage_fact_order_rows_task(combined_sales_path):
    path = Path(combined_sales_path)
    return process_and_stage_fact_order_rows(path)



def process_and_stage_fact_order_rows(combined_sales_path: Path):

    combined_sales = pd.read_parquet(combined_sales_path)

    ordered_qty = pd.to_numeric(combined_sales["OrderedQuantity"],errors="coerce")
    unit_price = pd.to_numeric(combined_sales["Price"],errors="coerce").astype("float")

    new_article = combined_sales["PartStatus"] == 3
    order_complete = combined_sales["CustomerOrderRowStatus"] == 9

    fact_order_rows = pd.DataFrame({
        #Tilausrivin uniikki tunniste. Voi olla sijaisavain tai yhdistelmä tilausnumerosta ja rivistä.
        "order_row_id": to_int64(combined_sales["OrderRowId"]),
        #Tilauksen uniikki tunniste. Tilauksille ei ole omaa taulua tässä tietomallissa, joten kyseessä "degenoitunut dimensio"
        "order_number": combined_sales["OrderNumber"],
        #Tuotantotilauksen uniikki tunniste. Tuotantotilauksille ei ole omaa taulua tässä tietomallissa, joten kyseessä "degenoitunut dimensio"
        "work_number": combined_sales["ManufacturingOrderNumber"],
        #Asiakastaulun viiteavain. Yhdistyy dim_customer tauluun.
        "customer_id": to_int64(combined_sales["CustomerId"]),
        #Myyjän tunniste. Voi olla tyhjä. Yhdistyy dim_user dimensiotauluun.
        "seller_id": to_int64(combined_sales["SellerId"]),
        #Nimikkeen/tuotteen tunniste. Yhdistyy dim_article dimensiotauluun.
        "article_id": to_int64(combined_sales["PartId"]),
        #ERP-järjestelmässä tilauksen luontipäivä. Käyttää semanttista avainformaattia (YYYMMDD).
        "order_created_date": get_date_key(combined_sales["OrderRowDate"]),
        #Suunniteltu tuotannon aloituspäivä, jos kyseiseen tilaukseen liittyy tuotantotilaus.
        "production_start_date_planned" : get_date_key(combined_sales["StartDate"]),
        #Tuotannon toteutunut aloituspäivä. Ensimmäisen tuotantovaiheen aikaleimasta.
        "production_start_date_actual" : get_date_key(combined_sales["ActualStartDate"]),
        #Suunniteltu tuotannon päättymispäivä ennen tuotantoa.
        "production_end_date_planned" : get_date_key(combined_sales["InitialFinishDate"]),
        #Tuotannon toteutunut päättymispäivä. Viimeisen tuotantovaiheen aikaleimasta.
        "production_end_date_actual" : get_date_key(combined_sales["ActualFinishDate"]),     
        #Alustava toimituspäivä, joka asetettu tilauksen luonnin tai työn suunnittelun yhteydessä.   
        "delivery_date_planned":  get_date_key(combined_sales["DesiredDeliveryDate"]),
        #Toteutunut toimituspäivä.
        "delivery_date_actual":   get_date_key(combined_sales["DeliveryDate"]),
        #Laskun lähetyksen päivä laskutustiedoista.
        "invoice_date_actual" : get_date_key(combined_sales["InvoiceDate"]),
        #Laskun maksupäivä päivä laskutustiedoista.
        "payment_date_actual" : pd.NA, #UUSI
        #Tilauksen tila. Alustavat vaihtoehdot: PLACED, PROCESSING, COMPLETE, CANCELED (CONFIRMED? PAID? DELIVERED?)
        "order_state_id": pd.to_numeric(combined_sales["ManufacturingOrderStatus"],errors="coerce"),
        #Tilauksen tyyppi. Mainly SALES_ORDER, but can be e.g. "OTHER" or "WARRANTY_WORK".
        "order_type": pd.to_numeric(combined_sales["OrderRowType"],errors="coerce"),
        #Tilattu määrä tuotetta. Oletuksena toimitettu määrä saman verran, joten ei erikseen tietomallissa. Eri työvaiheissa tuotetut määrät fact_workphases taulussa.
        "ordered_qty": ordered_qty,
        #Tilausrivin tuotteen yhden kappaleen hinta.
        "unit_price": unit_price,
        #Tilausrivin arvo valuutassa ilman arvonlisäveroa. Saadaan yleensä kertomalla unit_price * ordered_qty
        "total_price": ordered_qty * unit_price,
        #Suunniteltu suoran työn kustannus valmistuskustannuksissa. On eri tapoja laskea. Voidaan summata eri vaiheiden konetuntihinnoista laskettuja arvoja tai jyvittämällä henkilöiden työn osuutta (jos konekustannukset yleiskustannuksissa).
        "direct_work_cost_planned" : pd.NA,
        #Toteutunut suoran työn kustannus valmistuskustannuksissa.
        "direct_work_cost_actual" : pd.NA, #UUSI
        #Valmistuksen suunnitellut suorat materiaalikulut. Voidaan laskea esim. materiaalien alkuvaraston arvo + ostot - loppuvaraston arvo.
        "material_cost_planned" : pd.NA,
        #Valmistuksen toteutuneet suorat materiaalikulut.
        "material_cost_actual" : pd.NA,
        #Alihankinnan suunnitellut suorat kulut. Voi sisältää myös alihankintaan liittyvät logistiikkakulut
        "subcontracting_cost_planned" : pd.NA,
        #Alihankinnan toteutuneet suorat kulut. Voi sisältää myös alihankintaan liittyvät logistiikkakulut.
        "subcontracting_cost_actual" : pd.NA,
        #Muut muuttuvat kustannukset, kuten rahti, pakkaus, menetelmälisät…
        "other_direct_costs_planned" : pd.NA,
        #Toteutuneet muut muuttuvat kustannukset, kuten rahti, pakkaus, menetelmälisät…
        "other_direct_costs_actual" : pd.NA,
        #Tuotannon suunnitellut suorat tuotantokustannukset (välittömät kustannukset) ilman yleiskustannuslisää eli summattuna suora työ + materiaalit + alihankinta + muut suorat kustannukset.
        "prime_cost_planned_sum":  pd.NA,
        #Tuotannon toteutuneet suorat tuotantokustannukset (välittömät kustannukset) ilman yleiskustannuslisää eli summattuna suora työ + materiaalit + alihankinta + muut suorat kustannukset.
        "prime_cost_actual_sum" : pd.NA, #UUSI
        #Suunnitellut suorat tuotantokustannukset kappaletta kohti.
        "unit_prime_cost_planned": pd.NA,
        #Toteutuneet suorat tuotantokustannukset kappaletta kohti.
        "unit_prime_cost_actual" : pd.NA,
        #Tarjousvaiheessa laskettu myyntikate sisältäen yleiskustannuslisän ja voitto-osuuden. Voidaan laskea esim. total_price - prime_cost_planned_sum
        "sales_margin_planned":   pd.NA,
        #Toteutunut myyntikate sisältäen yleiskustannuslisän ja voitto-osuuden. Voidaan laskea esim. total_price - prime_cost_actual_sum
        "sales_margin_actual" : pd.NA,
        #Hintaan laskettu yleiskustannuslisä eli tilaukselle määritetty  välillisten kustannusten osuus. Yleensä laskettu jonain lisäprosenttina tuotantokustannusten päälle.
        "overhead_costs_planned" : pd.NA,
        #Totetunut yleiskustannuslisä eli tilaukselle määritetty välillisten kustannusten osuus.
        "overhead_costs_actual" : pd.NA, #UUSI
        #Suunnitellut täydet valmistuskustannukset yhteensä. Sisältää suorat tuotantokustannukset ja yleiskustannukset. Synonyyminä usein "omakustannehinta"
        "manufacturing_cost_planned" : pd.NA,
        #Toteutuneet täydet valmistuskustannukset yhteensä. Sisältää suorat tuotantokustannukset ja yleiskustannukset. Synonyyminä usein "omakustannehinta"
        "manufacturing_cost_actual" : pd.NA, #UUSI
        #Suunnitellut valmistuskustannukset kappaletta kohti.
        "unit_manufacturing_cost_planned" : pd.NA,
        #Toteutuneet valmistuskustannukset kappaletta kohti.
        "unit_manufacturing_cost_actual": pd.NA, #UUSI
        #Suunniteltu voittolisä kaikkien kustannusten vähennyksen jälkeen. Lasketaan vähentämällä myyntihinnasta kaikki kulut. Lasketaan yleensä jonain prosenttina omakustannehinnan päälle.
        "profit_planned" : pd.NA,
        #Toteutunut voittolisä kaikkien kustannusten vähennyksen jälkeen. Lasketaan vähentämällä myyntihinnasta kaikki kulut. Lasketaan yleensä jonain prosenttina
        "profit_actual" : pd.NA, #UUSI
        #Tilattu yksikkö.
        "unit": to_int64(combined_sales["UnitId"]),
        #Käytetty valuutta.
        "currency": "EUR",
        #Tieto siitä onko kyseessä uusi nimike.
        "new_article": new_article,
        #Tilaus on hoidettu ja laskutettu (ei välttämättä maksettu).
        "order_complete": order_complete,
    })

    engine = get_engine()

    try:
        with engine.begin() as conn:
            save_df(fact_order_rows,"fact_order_rows",conn)
            logger.info("Fact order rows saved!")
    
    except Exception as e:
            logger.error("safe_df epäonnistui: %s", str(e))
            raise RuntimeError("save_df epäonnistui — katso app.log") from None


@task(trigger_rule=TriggerRule.ALL_SUCCESS)
def truncate_fact_tables_task(fact_tables: List[str]):
    engine = get_engine()

    try:
        with engine.begin() as conn:
            truncate_fact_tables(conn, fact_tables)

    except Exception as e:
        logger.error(
            "truncate_fact_tables epäonnistui: %s",
            str(e),
            exc_info=True
        )
        raise RuntimeError(
            "truncate_fact_tables epäonnistui — katso app.log"
        ) from e


@task(trigger_rule=TriggerRule.ALL_SUCCESS)
def process_and_stage_fact_workphases_task(combined_manufacturing_path):
    path = Path(combined_manufacturing_path)
    return process_and_stage_fact_workphases(path)

def process_and_stage_fact_workphases(combined_manufacturing_path : Path):

    combined_manufacturing = pd.read_parquet(combined_manufacturing_path)
   
    planned_unit_time_min = ticks_to_minutes(combined_manufacturing["PlannedUnitTime"])
    planned_setup_time_min = ticks_to_minutes(combined_manufacturing["PlannedSetupTime"])
       
    phase_duration_planned_min = planned_unit_time_min + planned_setup_time_min
   
    reported_unit_time_min = ticks_to_minutes(combined_manufacturing["ReportedUnitTime"])
    reported_setup_time_min = ticks_to_minutes(combined_manufacturing["ReportedSetupTime"])
       
    phase_duration_stamping_min = reported_unit_time_min + reported_setup_time_min
   
    planned_qty = pd.to_numeric(combined_manufacturing["PlannedQuantity"],errors="coerce")
   
    produced_qty = pd.to_numeric(combined_manufacturing["ReportedQuantity"],errors="coerce")
   
    rejected_qty = pd.to_numeric(combined_manufacturing["RejectedQuantity"],errors="coerce")
   
    actual_finish = combined_manufacturing["ActualFinishDate"]
    phase_duration_per_piece_min = phase_duration_stamping_min / produced_qty
   
    actual_start_derived = actual_finish - pd.to_timedelta(
           phase_duration_per_piece_min,
           unit="m"
       )
   
    start_timestamp_actual = get_date_key(actual_start_derived)
   
    start_timestamp_actual = pd.Series(start_timestamp_actual).fillna(
           get_date_key(combined_manufacturing["ActualStartDate"])
       )
   
    fact_workphases = pd.DataFrame({
           #Työvaiherivin yksilöivä uniikki avain.
           "workphase_id": to_int64(combined_manufacturing["ManufacturingOrderOperationId"]),
           #Liittää työvaiheen tilausrvitauluun. Monesta-yhteen tyyppinen yhteys.
           "order_row_id": to_int64(combined_manufacturing["CustomerOrderRowId"]),
           #Tuotantotilauksen uniikki tunniste. Tuotantotilauksille ei ole omaa taulua tässä tietomallissa, joten kyseessä "degenoitunut dimensio"
           "work_number": combined_manufacturing["OrderNumber"],
           #Työtilauksen tunniste. Ei ole erikseen dimensiotaulua.
           "work_id" : to_int64(combined_manufacturing["ManufacturingOrderId"]),
           #Yksittäisen koneen/kuormitusryhmän/muun työkeskuksen tunniste (koneet, alihankkijat, muut työvaiheet). Tässä tietomallissa erilaiset työkeskukset tulevat samalla tavalla. 
           #Yhdistyy dim_workcenter ulottuvuustauluun. Vaatii tarkempaa määritystä mm. tilanteessa, jossa työ on toteutettu eri koneella kuin alkujaan suunniteltu tai työvaihe on jaettu usean koneen kesken.
           "workcenter_id" : to_int64(combined_manufacturing["WorkCenterId"]),
           #Työkeskuksen tyypin tunniste. Yhdistyy dim_workcenter_group tauluun. 
           "workcenter_group_id" : pd.NA, #UUSI
           #Nimikkeen/tuotteen tunniste. Yhdistyy dim_article ulottuvuustauluun.
           "article_id": to_int64(combined_manufacturing["OperationPartId"]),
           #Työvaiheen numero. Tyypillisesti järjestyksessä, jos useampi vaihe työssä.
           "phase_number" : combined_manufacturing["OperationNumber"],
           #Työvaiheen kuvaus, jos saatavilla. Voidaan käyttää erottamaan eri tyyppisiä työvaiheita toisistaan (työstö, alihankinta, kokoonpano…)
           "phase_description" : combined_manufacturing["Description_x"],
           #Tieto siitä, onko kyseessä alihankintatyövaihe
           "subcontracting_phase" : combined_manufacturing["IsSubcontractor"],
           #Työvaiheen alkuperäinen suunniteltu aloituspäivä ERP/MES järjestelmästä.
           "start_date_original_planned" : pd.NA,  ##TARKISTA
           #Työvaiheen viimeisin suunniteltu aloituspäivä ERP/MES järjestelmästä. Ensimmäinen ajankohta, jos on pilkottu useaan osaan vaihe.
           "start_date_latest_planned" : get_date_key(combined_manufacturing["PlannedStartDate"]),
           #Työvaiheen toteutunut aloituspäivä (yleensä leimausdatasta).
           "start_date_actual": get_date_key(combined_manufacturing["ActualStartDate"]),
           #Työvaiheen alkuperäinen suunniteltu lopetuspäivä ERP/MES järjestelmästä.
           "end_date_original_planned": pd.NA, ##TARKISTA
           #Työvaiheen viimeisin suunniteltu lopetuspäivä ERP/MES järjestelmästä. Viimeinen ajankohta, jos on pilkottu useaan osaan vaihe.
           "end_date_latest_planned": get_date_key(combined_manufacturing["PlannedFinishDate"]),
           #Työvaiheen toteutunut lopetuspäivä (yleensä leimausdatasta).
           "end_date_actual": get_date_key(combined_manufacturing["ActualFinishDate"]),
           #Työvaiheen viimeisin suunniteltu aloitusaika ERP/MES järjestelmästä (jos saatavilla). Semanttisessa muodossa (HHMMSS). Aika on aikaleiman aikavyöhykkeessä.
           "start_time_planned": pd.NA,
           #Työvaiheen toteutunut aloitusaika (yleensä leimaustiedoista). Semanttisessa muodossa (HHMMSS). Aika on aikaleiman aikavyöhykkeessä.
           "start_time_actual": to_hhmmss(combined_manufacturing["ActualStartDate"]),
           #Työvaiheen viimeisin suunniteltu lopetusaika ERP/MES järjestelmästä (jos saatavilla). Semanttisessa muodossa (HHMMSS). Aika on aikaleiman aikavyöhykkeessä.
           "end_time_planned": pd.NA,
           #Työvaiheen toteutunut lopetusaika (yleensä leimaustiedoista). Semanttisessa muodossa (HHMMSS). Aika on aikaleiman aikavyöhykkeessä.
           "end_time_actual": to_hhmmss(combined_manufacturing["ActualFinishDate"]),
           #Työvaiheen suunnitellun aloitusajan aikaleima ISO8601-formaatissa. 
           "start_timestamp_planned": combined_manufacturing["PlannedStartDate"],
           #Työvaiheen suunnitellun lopetusajan aikaleima ISO8601-formaatissa. 
           "end_timestamp_planned": combined_manufacturing["PlannedFinishDate"],
           #Työvaiheen toteutuneen aloitusajan aikaleima ISO8601-formaatissa. 
           "start_timestamp_actual": combined_manufacturing["ActualStartDate"],
           #Työvaiheen toteutuneen lopetusajan aikaleima ISO8601-formaatissa.
           "end_timestamp_actual" : combined_manufacturing["ActualFinishDate"],
           #Vaiheessa tuotettavaksi suunniteltujen kappaleiden määrä. Huom! Voi olla eri määrä kuin yksittäisellä tilausrivillä.
           "planned_qty": planned_qty,
           #Vaiheessa kaikkiaan tuotettu kappalemäärä.
           "produced_qty": produced_qty,
           #Hylättyjen kappaleiden määrä.
           "rejected_qty": rejected_qty,
           #Työkeskuksen tuntihinta. Yleensä käytännössä koneelle määritetty konetuntihinta kyseisen työn ajankohtana.
           "workcenter_hour_rate" : pd.NA,
           #Työvaiheen suunnitellut suoran työn kustannukset. Voi olla laskettu myös workcenter_hour_rate pohjalta, jos se on kustannusmallina.
           "phase_cost_planned" : pd.NA,
           #Työvaiheen toteutuneet suoran työn kustannukset leimausten perusteella. Voi olla laskettu myös konetuntihinnan pohjalta suhteutettuna toteutuneeseen aikaan, jos se on kustannusmallina.
           "phase_cost_actual_stamping" : pd.NA,
           #Työvaiheen toteutuneet suoran työn kustannukset. Voi olla laskettu myös konetuntihinnan pohjalta suhteutettuna toteutuneeseen aikaan, jos se on kustannusmallina.
           "phase_cost_actual_machine" : pd.NA,
           #Suunniteltu asetustyön kesto minuuteissa.
           "setup_time_planned_min": planned_setup_time_min,
           #Toteutunut asetustyön kesto minuuteissa leimausdatan pohjalta (jos saatavilla)
           "setup_time_stamping_min": reported_setup_time_min,
           #Toteutunut asetustyön kesto minuuteissa konedatan pohjalta (jos saatavilla)
           "setup_time_machine_min": pd.NA,
           #Vaiheen suunniteltu työstöaika minuutteina.
           "production_time_planned_min": planned_unit_time_min,
           #Vaiheen toteutunut työstöaika leimausdatan pohjalta.
           "production_time_stamping_min": reported_unit_time_min,
           #Vaiheen toteutunut työstöaika konedatan pohjalta.
           "production_time_machine_min": pd.NA,
           #Suunniteltu yhteenlaskettu vaiheaika sisältäen asetustyön ja työstöajan.
           "phase_duration_planned_min": phase_duration_planned_min,
           #Toteutunut leimauksista yhteenlaskettu vaiheaika sisältäen asetustyön ja työstöajan. Kaikkien erillisten työstökertojen summa.
           "phase_duration_stamping_min": ticks_to_minutes(combined_manufacturing["ReportedUnitTime"]),
           #Toteutunut konedatasta yhteenlaskettu vaiheaika sisältäen asetustyön ja työstöajan. Kaikkien erillisten työstökertojen summa.
           "phase_duration_machine_min": pd.NA,
           #Yhden kappaleen suunniteltu kappaleaika. Saadaan jakamalla suunniteltu vaiheaika (phase_duration_planned_min) suunniteltujen kappaleiden määrällä.
           "unit_time_planned_min" : phase_duration_planned_min / planned_qty,
           #Yhden kappaleen leimausten perusteella kappaleaika. Saadaan jakamalla leimattu vaiheaika tuotettujen kappaleiden määrällä.
           "unit_time_stamping_min" : reported_unit_time_min / produced_qty,
           #Yhden kappaleen konedatan perusteella kappaleaika. Saadaan jakamalla laskettu vaiheaika tuotettujen kappaleiden määrällä.
           "unit_time_machine_min" : pd.NA,
           #Yhden kappaleen paras mahdollinen tavoiteltu sykliaika/jaksoaika = aika kahden valmistuneen kappaleen välillä. Voi olla eri kuin mitä suunnittelussa käytetään. Käytetään OEE laskennassa, jos saatavilla.
           "cycle_time_ideal_min" : pd.NA,
           #Yhden kappaleen suunniteltu sykliaika/jaksoaika = aika kahden valmistuneen kappaleen välillä.
           "cycle_time_planned_min" : planned_unit_time_min / planned_qty,
           #Yhden kappaleen leimausten perusteella sykliaika/jaksoaika = aika kahden valmistuneen kappaleen välillä. Paras arvio, jos on erikseen mitattu ja leimattu.
           "cycle_time_stamping_min" : pd.NA,
           #Yhden kappaleen konedatan perusteella laskettu sykliaika/jaksoaika. Laskettu keskimääräinen tai mediaaniaika kappaleen valmistumisesta seuraavan kappaleen valmistumiseen.
           "cycle_time_machine_min" : pd.NA,
           #Yhden kappaleen tavoiteltu koneistusaika = aika kappaleen koneistuksen alkamisesta päättymiseen. Voi olla eri asia kuin suunniteltu tai toteutunut koneistusaika.
           "processing_time_ideal_min" : pd.NA,
           #Yhden kappaleen suunniteltu koneistusaika = aika kappaleen koneistuksen alkamisesta päättymiseen.
           "processing_time_planned_min" : pd.NA,
           #Leimausdatan perusteella yhden kappaleen koneistusaika = aika kappaleen koneistuksen alkamisesta päättymiseen.
           "processing_time_stamping_min" : pd.NA,
           #Konedatan perusteella yhden kappaleen koneistusaika = aika kappaleen koneistuksen alkamisesta päättymiseen.
           "processing_time_machine_min" : pd.NA
       })
    
    engine = get_engine()

    try:
        with engine.begin() as conn:
            save_df(fact_workphases,"fact_workphases",conn)
            logger.info("Fact workphases saved!")
    
    except Exception as e:
            logger.error("safe_df epäonnistui: %s", str(e))
            raise RuntimeError("save_df epäonnistui — katso app.log") from None
    

@task(trigger_rule=TriggerRule.ALL_SUCCESS)
def process_and_stage_fact_machine_events_task(combined_work_interval_path):
    path = Path(combined_work_interval_path)
    return process_and_stage_fact_machine_events(path)

def process_and_stage_fact_machine_events(combined_work_interval_path: Path):

    combined_work_intervals = pd.read_parquet(combined_work_interval_path)

    duration = ticks_to_minutes(combined_work_intervals["WorkedTime"])

    fact_machine_events = pd.DataFrame({
        # Konetapahtuman yksilöivä uniikki avain.
        "machine_event_id": to_int64(combined_work_intervals["Id"]),
        #Tällä erotetaan erilaiset konetapahtumat toisistaan. Yhdistyy dim_event_type dimensiotauluun, jossa tapahtumat kuvattu. 
        "event_type_id": 1,
        #Merkataan datan lähde. Yhdistyy dim_source tauluun. Oletuksena 1 = STAMPING, 2 = MACHINE API, 3 = SENSOR
        "source_id" : 1,
        #Yhdistää konetapahtuman jonkin työtilauksen vaiheeseen eli fact_workphases taulun riviin.
        "workphase_id": to_int64(combined_work_intervals["OperationId"]),
        #Tuotantotilauksen uniikki tunniste. Tuotantotilauksille ei ole omaa taulua tässä tietomallissa, joten kyseessä "degenoitunut dimensio"
        "work_number" : combined_work_intervals["OrderNumber"],
        #Työtilauksen tunniste. Ei ole erikseen dimensiotaulua.
        "work_id" : to_int64(combined_work_intervals["ManufacturingOrderId"]),
        #Koneen/työkeskuksen tunniste. Yhdistyy dim_machine taulun konetietoihin.
        "workcenter_id" : to_int64(combined_work_intervals["WorkCenterId"]),
        #Työstettävän nimikkeen tunniste. Yhdistyy 
        "article_id" : to_int64(combined_work_intervals["ManufacturingOrderPartId"]),
        #Koneen käyttäjän tunniste.
        "user_id": to_int64(combined_work_intervals["ReportingEmployeeId"]),
        #Koneessa ajossa olevan ohjelman nimi. 
        "program_name" :pd.NA,
        #Konetapahtuman aloituspäivä semanttisessa avainformaatissa (YYYYMMDD).
        "start_date":  get_date_key(combined_work_intervals["IntervalStart"]),
        #Konetapahtuman päättymispäivä semanttisessa avainformaatissa (YYYYMMDD).
        "end_date":  get_date_key(combined_work_intervals["IntervalEnd"]),
        #Tapahtuman aloitusaika. Semanttisessa muodossa (HHMMSS). Aika on aikaleiman aikavyöhykkeessä.
        "start_time": to_hhmmss(combined_work_intervals["IntervalStart"]),
        #Tapahtuman lopetusaika. Semanttisessa muodossa (HHMMSS). Aika on aikaleiman aikavyöhykkeessä.
        "end_time": to_hhmmss(combined_work_intervals["IntervalEnd"]),
        #Tapahtuman aloituksen aikaleima ISO8601 formaatissa.
        "start_at": combined_work_intervals["IntervalStart"],
        #Tapahtuman lopetuksen aikaleima ISO8601 formaatissa.
        "end_at": combined_work_intervals["IntervalEnd"],
        #Tapahtuman kesto minuutteina. Laskettu aloitusajan ja lopetusajan erotuksesta.
        "duration_min": duration,
        #Yhden kappaleen laskettu kappaleaika. Voidaan laskea esim. jakamalla duration_min / produced_qty.
        "unit_time_min" : duration / combined_work_intervals["ReportedQuantity"],
        #Yhden kappaleen keskimääräinen sykliaika, jos on saatavilla.  Aika kappaleen valmistumisesta toisen kappaleen valmistumiseen. Usean kappaleen tapauksessa esim. keskiarvo.
        "unit_cycle_time_min" : pd.NA,
        #Yhden kappaleen työstöaika. Aika kappaleen työstön aloituksesta työstön loppumiseen. Useamman kappaleen tapauksessa esim. keskiarvo.
        "unit_processing_time_min" : pd.NA,
        #Tuotettujen kappaleiden määrä tapahtuman aikana.
        "produced_qty_stamping": combined_work_intervals["ReportedQuantity"],
        #Konedatasta laskettu tuotettujen kappaleiden määrä tapahtuman aikana.
        "produced_qty_machine":pd.NA, #UUSI
        #Hylättyjen kappaleiden määrä. Mahdollisesti saatavilla leimausdatassa.
        "rejected_qty_stamping": combined_work_intervals["RejectedQuantity"],
        #Koneen kappalelaskurin arvo tapahtuman loputtua.
        "part_counter" : pd.NA, #UUSI
        #Koneen tehokas työstöaika tapahtuman aikana johdettuna konedatasta.
        "efficient_production_min" : pd.NA,
        #Tehokkaan työstöajan osuus tapahtuman ajasta (effective_production_min/duration_min).
        "efficient_production_pct" : pd.NA,
        #Koneen tehoton työstöaika tapahtuman aikana johdettuna konedatasta. Kone käy mutta ei "täydellä teholla".
        "inefficient_production_min" : pd.NA,
        #Tehottoman työstöajan osuus tapahtuman ajasta (ineffective_production_min/duration_min).
        "inefficient_production_pct" : pd.NA,
        #Koneen varsinainen lastuamisaika aktiivisen työn aikana.
        "cutting_time_min" : pd.NA, #UUSI
        #Aktiivisen työn aikana osuus ajasta, jolloin lastu ei irtoa. Sisältää esim. työstöpäiden pikaliikkeet, työkalujen vaihdot, työstönaikaiset mittaukset jne.
        "non_cutting_time_min" : pd.NA, #UUSI
        #Keskimääräinen syötön prosenttikytkimen (feed rate override) arvo työstön aikana.
        "feed_override" : pd.NA, #UUSI
        #Keskimääräinen karan nopeuden prosenttikytkimen (spindle speed override) arvo työstön aikana.
        "spindle_override": pd.NA,
        #Aktiivisen työstön määrä minuutteina.
        "machine_active_min": pd.NA,
        #Aktiivisen työstöajan osuus tapahtuman ajasta (machine_active_min/duration_min).
        "machine_active_pct": pd.NA,
        #setusajan määrä tapahtuman aikana. On yrityskohtaista mitä asetusaikaan sisällytetään.
        "setup_time_min" : pd.NA,
        #Asetusajan osuus tapahtuman kokonaisajasta.
        "setup_time_pct" : pd.NA,
        #Kappaleen vaihtoajan määrä tapahtuman aikana. Vaihtoaika on aika kappaleen työstön päättymisestä seuraavan kappaleen työstön alkuun saman työvaiheen aikana.
        "part_change_time_min" : pd.NA,
        #Vaihtoajan osuus tapahtuman kokoanaisajasta.
        "part_change_time_pct" : pd.NA,
        #Muun tyhjäkäyntiajan määrä tapahtuman aikana. Käytetään, jos aikaa ei voi määrittää asetusajaksi tai kappaleen vaihtoajaksi.
        "undefined_idle_min" : pd.NA,
        #Muun tyhjäkäynnin osuus tapahtuman kokonaisajasta.
        "undefined_idle_pct": pd.NA,
        #Summattu koneen tyhjäkäynti minuutteina tapahtuman aikana (setup_time_min + part_change_time_min + undefimed_idle_min)
        "idle_sum_min" : pd.NA,
        #Tyhjäkäynnin osuus tapahtuman ajasta (idle_sum_min/duration_min).
        "idle_sum_pct" : pd.NA,
        #Kone poissa päällä minuutteina.
        "power_off_min": pd.NA,
        #Poissaolon aika koko ajasta (power_off_min/duration_min).
        "power_off_pct": pd.NA,
        #Aika jota ei saatu kohdistettua mihinkään tapahtuman aikana. Esimerkiksi erotus duration_min ja eri tila-aikojen summasta.
        "other_min": pd.NA,
        #Kohdistumattoman ajan osuus tapahtuman kokonaisajasta.
        "other_pct" : pd.NA,
        #Poikkeustilan (koneessa esim. virhetila päällä) kesto minuutteina.
        "exception_min": pd.NA,
        #Poikkeustilan osuus koko ajasta (exception_min/duration_min).
        "exception_pct": pd.NA,
        #Poikkeustilan yksilöivä tunniste. Yhdistyy dim_exception tauluun, jossa tarkemmat tiedot. Voidaan käyttää vaikka koneen ilmoittamien virhtilojen seurantaan.
        "exception_id": pd.NA,
        #Tapahtuman aikana kulutettu energiamäärä kWh, jos on saatavilla esim. konedatassa.
        "power_consumption_kwh": pd.NA,
        #Kuvauksia, huomautuksia tai muuta tapahtumaan liittyvää tietoa. Voidaan käyttää myös lisädatan säilömiseen, jos kentät eivät riitä.
        "notes": pd.NA,
    })
    engine = get_engine()
    
    """excel_path = append_df_to_excel(
        fact_machine_events,"fact_machine_events"
    )"""

    try:
        with engine.begin() as conn:
            save_df(fact_machine_events,"fact_machine_events",conn)
            logger.info("Fact machine events saved!")
    
    except Exception as e:
            logger.error("safe_df epäonnistui: %s", str(e))
            raise RuntimeError("save_df epäonnistui — katso app.log") from None


def shape_and_types(
    df: pd.DataFrame,
    rename_map: Mapping[str, str],
    keep_cols: Sequence[str],
    int_cols: Sequence[str] = (),
    float_cols: Sequence[str] = (),
) -> pd.DataFrame:
    """
    - Nimeää sarakkeet rename_map:in mukaan (vanha -> uusi).
    - Luo puuttuvat sarakkeet keep_cols-listasta (täyttää pd.NA:lla).
    - Rajaa outputin täsmälleen keep_cols-järjestykseen.
    - Konvertoi dtyperit:
        * int_cols  -> Pandas Int64 (nullable)
        * float_cols -> Pandas Float64 (nullable)
        * kaikki loput (keep_cols - int_cols - float_cols) -> Pandas string + strip
    """
    out = df.rename(columns=rename_map).copy()

    for c in keep_cols:
        if c not in out.columns:
            out[c] = pd.NA

    out = out[[c for c in keep_cols if c in out.columns]]

    int_set = set(int_cols)
    float_set = set(float_cols)
    str_cols_auto = [c for c in keep_cols if c not in int_set | float_set]

    for c in int_cols:
        if c in out.columns:
            out[c] = to_int64(out[c])

    for c in float_cols:
        if c in out.columns:
            out[c] = to_float64(out[c])

    for c in str_cols_auto:
        if c in out.columns:
            out[c] = out[c].astype("string").str.strip()

    return out

@task(trigger_rule=TriggerRule.ALL_SUCCESS)
def process_dim_task():
    process_dim()


def process_dim():
    _, work_centers = load_latest(LOCAL_STAGED_PATH / "manufacturing", "WorkCenters",LoadMode.STAGED)
    _, customers = load_latest(LOCAL_STAGED_PATH / "sales", "Customers",LoadMode.STAGED)
    _, order_types = load_latest(LOCAL_STAGED_PATH / "sales", "CustomerOrderTypes",LoadMode.STAGED)
    _, parts = load_latest(LOCAL_COMBINED_PATH / "inventory", "CombinedInventory",LoadMode.COMBINED)

    rename_work_centers = {
        "Id" : "workcenter_id",
        "Name" : "workcenter_name",
        "Type" : "workcenter_type",
        "Description" : "workcenter_description", 
    }

    work_center_int_cols = ["workcenter_id"]
    work_center_keep_columns = [ "workcenter_id","workcenter_name","workcenter_description", "workcenter_type"]
    work_centers = shape_and_types(work_centers,rename_work_centers,work_center_keep_columns,work_center_int_cols)


    rename_parts = {
        "Id_part" : "article_id",
        "Code" : "article_code",
        "PartNumber" : "article_name",
        "Alias_part" : "article_name_other",
        "Description_part" : "article_description",
        "ActiveRevisionId" : "revision",
        "ExtraDescription" : "notes",
    }

    part_int_columns = ["article_id","revision"]
    part_float_columns = ["prime_cost"]
    part_keep_columns = [ "article_id","article_code","article_name","article_name_other","article_description","revision","notes"]
    parts = shape_and_types(parts,rename_parts,part_keep_columns,part_int_columns,part_float_columns)

    rename_customers = {
        "Id" : "customer_id",
        "Code" : "customer_number",
        "Name" : "customer_name",
    }
    

    customer_int_columns = ["customer_id"]
    customer_keep_columns = ["customer_id","customer_number", "customer_name","location"]
    customers = shape_and_types(customers,rename_customers,customer_keep_columns,customer_int_columns)


    rename_order_types= {
        "Id" : "order_type_id",
        "Description" : "order_type_name",
    }
    
    order_type_int_columns = ["order_type_id"]
    order_types_to_keep = [ "order_type_id","order_type_name"]
    order_types = shape_and_types(order_types,rename_order_types,order_types_to_keep,order_type_int_columns)

    engine = get_engine()
    try:
        with engine.begin() as conn:
            save_df(work_centers, "dim_workcenter",conn,clear_before_insert="delete")
            save_df(parts,"dim_article",conn,clear_before_insert="delete")
            save_df(customers,"dim_customer",conn,clear_before_insert="delete")
            save_df(order_types,"dim_order_type",conn,clear_before_insert="delete")
    
    except Exception as e:
            logger.error("safe_df epäonnistui: %s", str(e))
            raise RuntimeError("save_df epäonnistui — katso app.log") from None


@task(trigger_rule=TriggerRule.ALL_SUCCESS)
def process_erp_files_task(my_files):
    logger.info("Processing files")
    for endpoint, files in my_files.items():
        for file_path in files:
            logger.info(file_path)
            process_erp_files(Path(file_path))

@task(retries=3,retry_delay=timedelta(minutes=2))
def get_token_task():
    get_token()

@task()
def fetch_common_task():
    fetch_common()

@task()
def fetch_inventory_task():
    fetch_inventory()

@task()
def fetch_manufacturing_task():
    fetch_manufacturing()

@task()
def fetch_sales_task():
    fetch_sales()

@task()
def fetch_purchases_task():
    fetch_purchases()

@task()
def fetch_time_recording_task():
    fetch_time_recording()



@dag(
    dag_id="erp_file_processor",
    default_args=default_args,
    start_date=datetime(2025, 8, 16),
    catchup=False,
    tags=["gcs", "etl", "demo"],
)

def erp_file_processor():

    authenticate = get_token_task()
    
    t_common = fetch_common_task()
    t_inventory = fetch_inventory_task()
    t_manufacturing = fetch_manufacturing_task()
    t_sales = fetch_sales_task()
    t_purchases = fetch_purchases_task()
    t_time_recordings = fetch_time_recording_task()
    
    manufacturing_files = get_latest_endpoint_files_task("manufacturing")
    common_files = get_latest_endpoint_files_task("common")
    inventory_files = get_latest_endpoint_files_task("inventory")
    sales_files = get_latest_endpoint_files_task("sales")
    purchase_files = get_latest_endpoint_files_task("purchase")
    time_recording_files = get_latest_endpoint_files_task("time_recording")

    process_manufacturing = process_erp_files_task(manufacturing_files)
    process_common = process_erp_files_task(common_files)
    process_inventory = process_erp_files_task(inventory_files)
    process_sales = process_erp_files_task(sales_files)
    process_purchases = process_erp_files_task(purchase_files)
    process_time_recordings = process_erp_files_task(time_recording_files)
    

    combined_orders = combine_manufacturing_data_task(str(LOCAL_STAGED_PATH / "manufacturing"),str(LOCAL_STAGED_PATH / "common"),str(LOCAL_STAGED_PATH / "sales"), combine_manufacturing_tables)
    combined_sales = combine_sales_data_task(str(LOCAL_STAGED_PATH / "sales"),str(LOCAL_STAGED_PATH / "manufacturing"), combine_sales_tables)
    combined_inventory = combine_inventory_data_task(str(LOCAL_STAGED_PATH / "inventory"), combine_inventory_tables)
    combined_purchases = combine_purchase_data_task(str(LOCAL_STAGED_PATH / "purchase"), combine_purchase_tables)
    combined_work_intervals = combine_work_interval_data_task(str(LOCAL_STAGED_PATH / "time_recording"),str(LOCAL_STAGED_PATH / "manufacturing"),combine_work_interval_tables)

    truncated_tables = truncate_fact_tables_task(FACT_TABLES)

    processed_orders = process_and_stage_fact_order_rows_task(combined_sales)
    processed_workphases = process_and_stage_fact_workphases_task(combined_orders)

    processed_dim = process_dim_task()

    processed_machine_events = process_and_stage_fact_machine_events_task(combined_work_intervals)

    authenticate

    authenticate >> t_manufacturing >> manufacturing_files
    authenticate >> t_common >> common_files
    authenticate >> t_inventory >> inventory_files
    authenticate >> t_sales >> sales_files
    authenticate >> t_purchases >> purchase_files
    authenticate >> t_time_recordings >> time_recording_files

    manufacturing_files >> process_manufacturing >> combined_orders #>> combined_reportings
    common_files >> process_common
    inventory_files >> process_inventory >> combined_inventory
    sales_files >> process_sales >> combined_sales
    purchase_files >> process_purchases >> combined_purchases
    time_recording_files >> process_time_recordings >> combined_work_intervals

    truncated_tables >> processed_dim
    combined_sales >> processed_dim
    combined_orders >> processed_dim

    processed_dim >> processed_orders

    processed_orders >> processed_workphases
    combined_orders >> processed_workphases

    processed_workphases >> processed_machine_events
    combined_work_intervals >> processed_machine_events

    
dag_instance = erp_file_processor()
