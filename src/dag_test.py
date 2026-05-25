from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import logging
import traceback
import json
import pandas as pd
import numpy as np
from dateutil.parser import parse
import os
import re
from collections import defaultdict
from babel.numbers import parse_decimal

from decimal import Decimal
from typing import Optional, Dict, List, Mapping, Sequence, Tuple
from pathlib import Path
from airflow.utils.context import Context

from enum import Enum


#When testing locally use src.config.erp_config import path
from src.config.erp_config import (
    ERP_COMMON_SOURCE_TYPES,ERP_INVENTORY_SOURCE_TYPES,ERP_PURCHASE_SOURCE_TYPES,ERP_MANUFACTURING_SOURCE_TYPES,
    ERP_SALES_SOURCE_TYPES,ERP_SOURCE_CONFIG,euro_columns,fact_order_rows,
    combine_inventory_tables,combine_manufacturing_tables,combine_sales_tables,combine_purchase_tables,combine_material_tables,
    combine_reporting_tables
)


#for h in logger.root.handlers[:]:
    #logger.root.removeHandler(h)

logger = logging.getLogger("custom_logger")
########## Global Google Cloud Setup ##########

LOCAL_BASE_PATH = "results/raw"
LOCAL_STAGED_PATH = Path("results/staged")
LOCAL_COMBINED_PATH = Path("results/combined")
"""OUTPUT_EXCEL_FOLDER = Path("/home/sasu/TUOVA/excel_tietomalli")
OUTPUT_EXCEL_FOLDER.mkdir(parents=True, exist_ok=True)

DATE_STR = datetime.now().strftime("%Y-%m-%d_%H%M%S")
EXCEL_PATH = OUTPUT_EXCEL_FOLDER / f"tietomalli_{DATE_STR}.xlsx"""


DATE_FMT = "%Y-%m-%d"
LOG_FILE = Path(__file__).resolve().parent / "app.log"

# Airflow related configs

SNOWFLAKE_CONN_ID = "tuova_conn"
DB = "TUOVA_DB"
SCHEMA = "ETL_STAGE"
CONTROL_TABLE = "FILE_LOAD_MAP"               # TUOVA_DB.ETL_STAGE.FILE_LOAD_MAP
EXT_STAGE = "EXT_GCS_TUOVA"                   # TUOVA_DB.ETL_STAGE.EXT_GCS_TUOVA
FILE_FORMAT = "PARQUET"                       # TUOVA_DB.ETL_STAGE.PARQUET

# ================

class LoadMode(Enum):
    STAGED = "staged"
    COMBINED = "combined"

ENTITIES = [
    ("dim_article",  "STG_DIM_ARTICLE"),
    ("dim_customer", "STG_DIM_CUSTOMER"),
    ("dim_machine", "STG_DIM_MACHINE"),
    ("dim_machine_group", "STG_DIM_MACHINE_GROUP"),
    ("dim_order_state", "STG_DIM_ORDER_STATE"),
    ("dim_order_type", "STG_DIM_ORDER_TYPE"),
    ("dim_phase", "STG_DIM_PHASE"),
    ("dim_user", "STG_DIM_USER"),
    ("fact_machine_events", "STG_FACT_MACHINE_EVENTS"),
    ("fact_order_rows", "STG_FACT_ORDER_ROWS"),
    ("fact_workphases", "STG_FACT_WORKPHASES"),
]

LINEAGE_COLS = ("BATCH_ID", "SRC_FILENAME", "SRC_FILE_LAST_MODIFIED")
SCHEMA_EXCLUDE = set(LINEAGE_COLS)  # exclude lineage from drift check

############ utility functions, this will to to the library later



def get_date_key(dt):
    """
    Convert to integer YYYYMMDD with <NA> for missing.
    Accepts scalar or pandas Series. Handles mixed timezones safely.
    """

    # --- Series input --------------------------------------------------------
    if isinstance(dt, pd.Series):
        s = dt.astype("string")  # safe base
        s = pd.to_datetime(s, errors="coerce", utc=True)

        # Drop timezone -> naive UTC datetime
        s = s.dt.tz_convert(None)

        out = pd.to_numeric(s.dt.strftime("%Y%m%d"), errors="coerce")
        return out.astype("Int64")

    # --- Scalar fallback -----------------------------------------------------
    if pd.isnull(dt):
        return pd.NA

    if isinstance(dt, str):
        ts = pd.to_datetime(dt, errors="coerce", utc=True)
        if pd.isnull(ts):
            return pd.NA
        ts = ts.tz_convert(None)
        return int(ts.strftime("%Y%m%d"))

    # Already datetime?
    if hasattr(dt, "tzinfo"):
        dt = dt.tz_convert("UTC").tz_localize(None)

    return int(pd.Timestamp(dt).strftime("%Y%m%d"))


"""def get_time_key(dt):
    
    #Convert to seconds since midnight with <NA> for missing.
    #Accepts scalar or pandas Series. Vectorized for Series input.
    
    if isinstance(dt, pd.Series):
        s = dt
        if not pd.api.types.is_datetime64_any_dtype(s):
            s = pd.to_datetime(s, errors='coerce', dayfirst=True, cache=True)
        secs = (s.dt.hour * 3600 + s.dt.minute * 60 + s.dt.second)
        # Ensure NA stays NA and use nullable integer
        secs = secs.where(~s.isna(), pd.NA).astype("Int64")
        return secs
    # --- scalar fallback (original behavior) ---
    if pd.isnull(dt):
        return pd.NA
    if isinstance(dt, str):
        dt = pd.to_datetime(dt, errors='coerce', dayfirst=True)
        if pd.isnull(dt):
            return pd.NA
    return dt.hour * 3600 + dt.minute * 60 + dt.second


def parse_euro_number(x):
    
    #Parses a European-style number string to float.
    #Returns original value if parsing fails.
    
    try:
        # Replace decimal comma with dot
        x_clean = x.replace(',', '.')
        return float(x_clean)
    except Exception:
        return x
    
def parse_erp_dates(df: pd.DataFrame, date_columns: List[str]) -> pd.DataFrame:
    for col in date_columns:
        s = pd.to_datetime(df[col], errors="coerce", dayfirst=True)

        # If timezone-aware, remove timezone but keep local time
        if hasattr(s.dt, "tz"):
            s = s.dt.tz_localize(None)

        df[col] = s

    return df"""


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


"""
def load_csv_with_converters(file_path, euro_columns = list(), sep=';', dtypes= dict()):
   
    converters = {col: parse_euro_number for col in euro_columns}

    logger.info(f"Loading CSV file: {file_path}")
    
    return pd.read_csv(file_path, sep=sep, converters=converters, dtype=dtypes) """


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


"""def combine_json_files(files: list, file_type: str = "files",euro_columns=list(), dtypes=dict()) -> Optional[pd.DataFrame]:
    
    Combines multiple JSON files into a single pandas DataFrame.

    Same behavior as combine_csv_files, with:
      - error handling
      - single-file optimization
      - data type conversions
    
    if not files:
        logger.warning(f"No {file_type} found.")
        return None

    # --- Optimize single file case ---
    if len(files) == 1:
        file_path = files[0]
        try:
            df = load_json_with_converters(file_path, euro_columns, dtypes=dtypes)
            if df is None:
                return None
            logger.info(f"Successfully read single JSON file: {file_path}")
            return df
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}")
            return None

    # --- Multi-file case ---
    dataframes = []
    for f in files:
        try:
            df = load_json_with_converters(f, euro_columns, dtypes=dtypes)
            if df is not None:
                dataframes.append(df)
                logger.info(f"Successfully read JSON file: {f}")
        except FileNotFoundError:
            logger.error(f"File not found: {f}")
            continue
        except json.JSONDecodeError as e:
            logger.error(f"Error reading JSON file {f}: {e}")
            continue

    if not dataframes:
        logger.warning(f"No valid {file_type} JSON dataframes to process.")
        return None

    combined_df = pd.concat(dataframes, ignore_index=True)
    logger.info(f"Successfully concatenated {len(dataframes)} {file_type}.")

    return combined_df """


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

#Testi
"""def save_clean_json(endpoint: str, data: pd.DataFrame, base_path: Path):
    endpoint_name = endpoint.lstrip("/").replace("/", "-") or "root"

    base_path.mkdir(parents=True, exist_ok=True)

    date_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"{date_str}-{endpoint_name}.json"
    full_path = base_path / filename

    json_ready = data.to_dict(orient="records")
    json_ready = replace_nan(json_ready)

    with open(full_path, "w", encoding="utf-8") as f:
        json.dump(json_ready, f, indent=4, ensure_ascii=False,allow_nan=False)

    logger.info(f"Saved cleaned data: {full_path}")
    return json_ready"""

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

"""def load_json_safely(path: Path):
    df = pd.read_json(path, dtype={})
    df = pd.DataFrame(df)
    df = df.fillna("")
    return df.astype("string")"""


def load_parquet_safely(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    return df


"""def load_and_restrict(path: Path, required_cols: List[str]) -> pd.DataFrame:
    df = load_json_safely(path)
    keep = [col for col in required_cols if col in df.columns]
    return df[keep].copy()"""


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

    #patt = re.compile(r"(\d{4}-\d{2}-\d{2})-" + re.escape(prefix) + r"\.json$")
    patt = re.compile(r"(\d{4}-\d{2}-\d{2})-" + re.escape(prefix) + r"\.parquet$")
    candidates = []

    #for file in base.glob("*.json"):
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

#Testi
"""def save_combined_json(df: pd.DataFrame, date_str: str, outdir: Path, endpoint :str):
    outdir.mkdir(parents=True, exist_ok=True)
    file_name = f"Combined{endpoint.capitalize()}.parquet"
    output_file = outdir / f"{date_str}-{file_name}"
    df.to_json(output_file, orient="records", indent=2, force_ascii=False)
    logger.info("Saved combined:", output_file)
    return output_file"""

def save_combined_parquet(df: pd.DataFrame, date_str: str, outdir: Path, endpoint :str):
    outdir.mkdir(parents=True, exist_ok=True)
    file_name = f"Combined{endpoint.capitalize()}.parquet"
    output_file = outdir / f"{date_str}-{file_name}"
    df.to_parquet(output_file,index=False)
    logger.info("Saved combined:", output_file)
    return str(output_file)


#For error handling in debugging, might delete later
def check_duplicates(df: pd.DataFrame):
    dups = df.columns[df.columns.duplicated(keep=False)]
    logger.info("Duplikaattikolumnit:", list(dups))




def combine_sales_data(staged_path: Path, mapping: dict):
    date_rows, rows = load_latest(staged_path, "CustomerOrderRows",LoadMode.STAGED, mapping["CustomerOrderRows"])
    date_orders, orders = load_latest(staged_path, "CustomerOrders",LoadMode.STAGED, mapping["CustomerOrders"])
    date_customers, customers = load_latest(staged_path, "Customers",LoadMode.STAGED, mapping["Customers"])

    if len({date_rows, date_orders, date_customers}) > 1:
        logger.info("⚠ WARNING: Source dates differ!", date_rows, date_orders, date_customers)


    rows = rows.rename(columns={
        "Id": "OrderRowId",
        "OrderDate": "OrderRowDate"
    })

    orders = orders.rename(columns={
        "Id": "OrderId"
    })

    customers = customers.rename(columns={
        "Id": "CustomerRecordId"
    })

    merged = rows.merge(
        orders,
        left_on="ParentOrderId",
        right_on="OrderId"
    )

    merged = merged.merge(
        customers,
        left_on="CustomerId",
        right_on="CustomerRecordId"
    )
    date_str = datetime.now().strftime("%Y-%m-%d")
    endpoint = "sales"
    saved_parquet =  save_combined_parquet(merged,date_str,LOCAL_COMBINED_PATH / endpoint,endpoint)
    clean_parquet_history(endpoint, LOCAL_COMBINED_PATH / endpoint, days_to_keep=2)
    return saved_parquet



def combine_sales_margin_data(
    staged_sales_path: Path,
    staged_inventory_path: Path,
    staged_common_path: Path,
    mapping: dict
):

    #1. LOAD DATA
    _, invoice_logs = load_latest(
        staged_sales_path, "InvoiceLogs", LoadMode.STAGED, mapping["InvoiceLogs"]
    )
    
    _, delivery_rows = load_latest(
        staged_sales_path, "CustomerOrderDeliveryRows", LoadMode.STAGED, mapping["CustomerOrderDeliveryRows"]
    )

    _, invoice_rows = load_latest(
        staged_sales_path, "CustomerOrderInvoiceRows", LoadMode.STAGED, mapping["CustomerOrderInvoiceRows"]
    )

    _, order_rows = load_latest(
        staged_sales_path, "CustomerOrderRows", LoadMode.STAGED, mapping["CustomerOrderRows"]
    )

    _, parts = load_latest(
        staged_inventory_path, "Parts", LoadMode.STAGED, mapping["Parts"]
    )

    _, customers = load_latest(
        staged_sales_path, "Customers", LoadMode.STAGED, mapping["Customers"]
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

    #2. PART EXTRA FIELD MAPPING (CustomerCode tms)
    part_extrafields = resolve_part_extrafields_by_group_name(
        extraFieldGroups=extraFieldGroups,
        extraFieldTemplates=extraFieldTemplates,
        extraFieldOptionTemplates=extraFieldOptionTemplates,
        extraFields=extraFields,
        group_name="lisätiedot"
    )

    part_extrafields = part_extrafields.rename(columns={
        "Code": "ExtraCustomerCode"
    })

    
    data = invoice_logs.copy()
    data["InvoiceLogId"] = data["Id"]

    #3. MERGE PART DATA
    data = invoice_logs.merge(
        parts,
        left_on="PartId",
        right_on="Id",
        how="left",
        suffixes=("", "_Part")
    )

    #4. MERGE EXTRA FIELD MAPPING
    data = data.merge(
        part_extrafields[["PartId", "ExtraCustomerCode"]],
        on="PartId",
        how="left"
    )

    data = data.merge(
        delivery_rows[["Id", "InvoiceRowId", "CustomerOrderRowId"]],
        left_on="DeliveryRowId",
        right_on="Id",
        how="left",
        suffixes=("", "_Delivery")
    )

    
    data = data.merge(
        invoice_rows.rename(columns={
            "Id": "InvoiceRowId_join",
            "StandardPrice": "StandardPrice_InvoiceRow",
            "OverrideStandardPrice": "OverrideStandardPrice"
        })[
            ["InvoiceRowId_join", "StandardPrice_InvoiceRow", "OverrideStandardPrice"]
        ],
        left_on="InvoiceRowId",
        right_on="InvoiceRowId_join",
        how="left"
    )


    
    data = data.merge(
        order_rows.rename(columns={
            "Id": "CustomerOrderRowId_join",
            "Price": "Price_OrderRow",
            "StandardPrice": "StandardPrice_OrderRow" 
        })[
            ["CustomerOrderRowId_join", "Price_OrderRow","StandardPrice_OrderRow" ]
        ],
        left_on="CustomerOrderRowId",
        right_on="CustomerOrderRowId_join",
        how="left"
    )
    
    dup = data.groupby("Id").size()
    print(dup.value_counts().head())


    #5. CUSTOMERCODE RESOLVE (SQL logic)
    data["FinalCustomerCode"] = (
        data["ExtraCustomerCode"]
        .combine_first(data["CustomerCode"])
    )


    # Quantity adjust
    data["AdjQuantity"] = data.apply(
        lambda r: (
            r["InvoicedQuantity"] / r["ConversionFactor"]
            if r.get("OrderRowType") == 1 and r.get("ConversionFactor") not in (0, None)
            else r["InvoicedQuantity"]
        ),
        axis=1
    )

    # Revenue
    data["Revenue"] = (
        (data["SetupPrice"].fillna(0)
         + data["Price"].fillna(0) * data["AdjQuantity"])
        * (1 - data["Discount"].fillna(0) / 100)
        * data["ExchangeRate"].fillna(1)
    )

    # Unit cost (FIFO → Standard fallback)
    
    
    data["UnitCost"] = (
        data["OverrideStandardPrice"]
        .combine_first(data["StandardPrice_InvoiceRow"])
        .combine_first(data["StandardPrice_Part"])
        .combine_first(data["StandardPrice"])
        .fillna(0)
)

   
    # Cost
    data["Cost"] = data["UnitCost"] * data["InvoicedQuantity"]

    # Margin
    data["Margin"] = data["Revenue"] - data["Cost"]

    # Margin %
    data["MarginPct"] = data.apply(
        lambda r: (r["Margin"] / r["Revenue"] * 100) if r["Revenue"] != 0 else 0,
        axis=1
    )

    print("Revenue:", data["Revenue"].sum())
    print("Cost:", data["Cost"].sum())

    #7. FLAG fictitious parts
    data["IsFictitious"] = data["Type"] == 2


    #8. AGGREGATE (like SQL GROUP BY)
    raw_data = data.copy()
    result = data.groupby(["FinalCustomerCode","Type"]).agg({
        "Revenue": "sum",
        "Cost": "sum",
        "Margin": "sum",
        "InvoicedQuantity": "sum",
        "Id": "count",
        "IsFictitious": "sum"
    }).reset_index()

    result["MarginPct"] = result.apply(
        lambda r: (r["Margin"] / r["Revenue"] * 100) if r["Revenue"] != 0 else 0,
        axis=1
    )

    #9. SAVE
    date_str = datetime.now().strftime("%Y-%m-%d")
    endpoint = "materials"

    saved_parquet = save_combined_parquet(
        result,
        date_str,
        LOCAL_COMBINED_PATH / endpoint,
        endpoint
    )

    
    save_combined_parquet(
        raw_data,
        date_str,
        LOCAL_COMBINED_PATH / (endpoint + "_raw"),
        endpoint + "_raw"
)

    clean_parquet_history(
        endpoint,
        LOCAL_COMBINED_PATH / endpoint,
        days_to_keep=2
    )

    return saved_parquet



def combine_inventory_data(staged_path: Path, mapping: dict):

    date_codes, part_codes = load_latest(staged_path, "PartCodes",LoadMode.STAGED, mapping["PartCodes"])
    date_parts, parts = load_latest(staged_path, "Parts",LoadMode.STAGED, mapping["Parts"])

    if len({date_codes, date_parts}) > 1:
        logger.info("⚠ WARNING: Source dates differ!", date_codes, date_parts)

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


def combine_manufacturing_data(staged_manufacturing_path: Path, staged_common_path : Path,mapping: dict):

    date_mo, manufacturing_orders = load_latest(
        staged_manufacturing_path, "ManufacturingOrders",LoadMode.STAGED, mapping["ManufacturingOrders"]
    )
    date_ops, orderOperations = load_latest(
        staged_manufacturing_path, "ManufacturingOrderOperations",LoadMode.STAGED, mapping["ManufacturingOrderOperations"]
    )
    date_rows, operationRows = load_latest(
        staged_manufacturing_path, "OperationRows",LoadMode.STAGED, mapping["OperationRows"]
    )

    ef_rows,extraFields = load_latest(
        staged_common_path, "ExtraFields",LoadMode.STAGED, mapping["ExtraFields"]
    )

    efg_rows,extraFieldGroups = load_latest(
        staged_common_path, "ExtraFieldGroups",LoadMode.STAGED, mapping["ExtraFieldGroups"]
    )

    efot_rows,extraFieldOptionTemplates = load_latest(
        staged_common_path, "ExtraFieldOptionTemplates",LoadMode.STAGED, mapping["ExtraFieldOptionTemplates"]
    )

    eft_rows,extraFieldTemplates = load_latest(
        staged_common_path, "ExtraFieldTemplates",LoadMode.STAGED, mapping["ExtraFieldTemplates"]
    )

    manufacturing_orders = manufacturing_orders.rename(columns={
        "Id": "ManufacturingOrderId"
    })

    orderOperations = orderOperations.rename(columns={
        "Id": "ManufacturingOrderOperationId",
        "ReportedQuantity" : "OperationReportedQuantity",
        "PlannedQuantity" : "OperationPlannedQuantity",
        "PartId" : "OperationPartId"
    })

    operationRows = operationRows.rename(columns={
        "Id": "OperationRowId"
    })

    
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
        )


    date_str = datetime.now().strftime("%Y-%m-%d")

    endpoint = "manufacturing"
    saved_parquet =  save_combined_parquet(merged,date_str,LOCAL_COMBINED_PATH / endpoint,endpoint)
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

"""
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

    return saved"""


def combine_manufacturing_reportings(staged_path: Path,staged_common_path: Path, mapping: dict):

    _, manufacturing_orders = load_latest(
        staged_path,
        "ManufacturingOrders",
        LoadMode.STAGED,
        mapping["ManufacturingOrders"]
    )

    _, orderOperations = load_latest(
        staged_path,
        "ManufacturingOrderOperations",
        LoadMode.STAGED,
        mapping["ManufacturingOrderOperations"]
    )

    _, reportingRows = load_latest(
        staged_path,
        "ManufacturingOrderOperationReportings",
        LoadMode.STAGED,
        mapping["ManufacturingOrderOperationReportings"]
    )



    manufacturing_orders = manufacturing_orders.rename(columns={
        "Id": "ManufacturingOrderId"
    })

    orderOperations = orderOperations.rename(columns={
        "Id": "ManufacturingOrderOperationId",
    })

    reportingRows = reportingRows.rename(columns={
        "Id": "ReportingId",
        "OperationId": "ManufacturingOrderOperationId"
    })

    merged = reportingRows.merge(
        orderOperations,
        on="ManufacturingOrderOperationId",
        how="left"
    )

    merged = merged.merge(
        manufacturing_orders,
        on="ManufacturingOrderId",
        how="left"
    )

    date_str = datetime.now().strftime("%Y-%m-%d")
    endpoint = "reportings"

    saved = save_combined_parquet(
        merged,
        date_str,
        LOCAL_COMBINED_PATH / endpoint,
        endpoint
    )

    clean_parquet_history(endpoint, LOCAL_COMBINED_PATH / endpoint, days_to_keep=7)

    return saved


def process_and_stage_fact_order_rows(combined_sales_path: Path):

    combined_sales = pd.read_parquet(combined_sales_path)

    fact_order_rows = pd.DataFrame({
        "order_row_id": to_int64(combined_sales["OrderRowId"]),
        "order_number": combined_sales["OrderNumber"],
        "work_number": combined_sales["LinkedManufacturingOrderId"],
        "customer_id": to_int64(combined_sales["CustomerId"]),
        "seller_id": to_int64(combined_sales["SellerId"]),
        "article_id": to_int64(combined_sales["PartId"]),
        "order_created_date":     get_date_key(combined_sales["OrderDate"]),
        "start_date_planned":     pd.NA,
        "start_date_actual":      pd.NA,
        "end_date_planned":       pd.NA,
        "end_date_actual":        get_date_key(combined_sales["NewFinishDate"]),
        "delivery_date_planned":  get_date_key(combined_sales["DesiredDeliveryDate"]),
        "delivery_date_actual":   get_date_key(combined_sales["DeliveryDate"]),
        "order_state_id":         pd.to_numeric(combined_sales["Status"],errors="coerce"),
        "order_type_id":          pd.to_numeric(combined_sales["OrderRowType"],errors="coerce"),
        "ordered_qty":            pd.to_numeric(combined_sales["OrderedQuantity"],errors="coerce"),
        "total_price":            pd.NA,
        "unit_price":             pd.to_numeric(combined_sales["Price"],errors="coerce").astype("float"),
        "unit_prime_cost_planned": pd.NA,
        "prime_cost_planned_sum":  pd.NA,
        "unit_prime_cost_actual":  pd.NA,
        "prime_cost_actual_sum":   pd.NA,
        "unit":                   to_int64(combined_sales["UnitId"]),
        "currency":               "EUR",
        "sales_margin_planned":   pd.NA,
        "sales_margin_actual":    pd.NA,
        "new_article":            pd.NA,
        "order_complete":         get_date_key(combined_sales["NewFinishDate"]),
    })


    """excel_path = append_df_to_excel(
        fact_order_rows,"fact_order_rows"
    )"""


"""def append_df_to_excel(df: pd.DataFrame, sheet_name: str):
    logger.info("Saving excel: " + sheet_name)
    df = clean_dataframe_for_excel(df)
    df = convert_large_ints_to_str(df)
    if EXCEL_PATH.exists():
        # Append-mode: if_sheet_exists sallittu
        with pd.ExcelWriter(
            EXCEL_PATH,
            engine="openpyxl",
            mode="a",
            if_sheet_exists="replace"
        ) as writer:
            df.to_excel(
                writer,
                sheet_name=sheet_name[:31],
                index=False
            )
    else:
        # Write-mode: EI if_sheet_exists
        with pd.ExcelWriter(
            EXCEL_PATH,
            engine="openpyxl",
            mode="w"
        ) as writer:
            df.to_excel(
                writer,
                sheet_name=sheet_name[:31],
                index=False
            )"""

_ILLEGAL_EXCEL_CHARS_RE = re.compile(r"[\x00-\x08\x0B-\x0C\x0E-\x1F\x7F]")

def convert_large_ints_to_str(
    df: pd.DataFrame,
    max_digits: int = 15
) -> pd.DataFrame:
    df = df.copy()

    int_like_cols = df.select_dtypes(
        include=["int64", "uint64", "Int64"]
    ).columns

    for col in int_like_cols:
        s = df[col]

        if s.notna().any():
            max_len = s.dropna().astype(str).str.len().max()

            if max_len > max_digits:
                df[col] = s.astype("string")

    return df




def _clean_excel_string(s: str) -> str:
    if s is None:
        return s

    if not isinstance(s, str):
        try:

            if isinstance(s, (list, dict)):
                s = json.dumps(s, ensure_ascii=False)
            else:
                s = str(s)
        except Exception:
            s = str(s)

    return _ILLEGAL_EXCEL_CHARS_RE.sub("", s)

def clean_dataframe_for_excel(df: pd.DataFrame) -> pd.DataFrame:
   
    if df is None or df.empty:
        return df

    df_clean = df.copy()

    obj_cols = df_clean.select_dtypes(include=["object", "string"]).columns
    for col in obj_cols:
        df_clean[col] = df_clean[col].map(
            lambda v: _clean_excel_string(v) if pd.notna(v) else v
        )

    return df_clean


def process_and_stage_fact_workphases(combined_manufacturing_path: Path):

    combined_manufacturing = pd.read_parquet(combined_manufacturing_path)
    
    combined_manufacturing["PlannedStartDate"] = pd.to_datetime(
        combined_manufacturing["PlannedStartDate"],
        errors="coerce",
        utc=True
    )

    combined_manufacturing["PlannedFinishDate"] = pd.to_datetime(
        combined_manufacturing["PlannedFinishDate"],
        errors="coerce",
        utc=True
    )

    combined_manufacturing["ActualStartDate"] = pd.to_datetime(
        combined_manufacturing["ActualStartDate"],
        errors="coerce",
        utc=True
    )

    combined_manufacturing["ActualFinishDate"] = pd.to_datetime(
        combined_manufacturing["ActualFinishDate"],
        errors="coerce",
        utc=True
    )

    fact_workphases = pd.DataFrame({
        "workphase_id": to_int64(combined_manufacturing["ManufacturingOrderOperationId"]),
        "order_row_id": to_int64(combined_manufacturing["CustomerOrderId"]),
        "work_number": to_int64(combined_manufacturing["OperationNumber"]),
        "order_number" : combined_manufacturing["OrderNumber"],
        "machine_id": to_int64(combined_manufacturing["WorkCenterId"]),
        "article_id": to_int64(combined_manufacturing["PartId"]),
        "start_date_planned" : get_date_key(combined_manufacturing["PlannedStartDate"]),
        "start_date_actual": get_date_key(combined_manufacturing["ActualStartDate"]),
        "end_date_planned": get_date_key(combined_manufacturing["PlannedFinishDate"]),
        "end_date_actual": get_date_key(combined_manufacturing["ActualFinishDate"]),
        "start_time_planned": pd.NA,
        "start_time_actual": pd.NA,
        "end_time_planned": pd.NA,
        "end_time_actual": pd.NA,
        "phase_id": to_int64(combined_manufacturing["OperationRowId"]),
        "start_timestamp_planned": pd.NA,
        "end_timestamp_planned": pd.NA,
        "phase_duration_planned_min": (ticks_to_minutes(combined_manufacturing["PlannedUnitTime"]) + ticks_to_minutes(combined_manufacturing["PlannedSetupTime"])),
        "phase_duration_actual_min": (ticks_to_minutes(combined_manufacturing["ReportedUnitTime"]) + ticks_to_minutes(combined_manufacturing["ReportedSetupTime"])),
        "planned_qty": pd.to_numeric(combined_manufacturing["PlannedQuantity"],errors="coerce"),
        "produced_qty": pd.to_numeric(combined_manufacturing["ReportedQuantity"],errors="coerce"),
        "rejected_qty": pd.to_numeric(combined_manufacturing["RejectedQuantity"],errors="coerce"),
        "phase_price_planned": pd.NA,
        "phase_price_actual": pd.NA,
        "setup_time_planned_min": ticks_to_minutes(combined_manufacturing["PlannedSetupTime"]),
        "setup_time_actual_min": ticks_to_minutes(combined_manufacturing["ReportedSetupTime"]),
        "production_time_planned_min": ticks_to_minutes(combined_manufacturing["PlannedUnitTime"]),
        "production_time_actual_min": ticks_to_minutes(combined_manufacturing["ReportedUnitTime"]),
        "single_item_time_planned_min" : (combined_manufacturing["PlannedFinishDate"] - combined_manufacturing["PlannedStartDate"]).dt.total_seconds() / 60,
        "single_item_time_actual_min": (combined_manufacturing["ActualFinishDate"] - combined_manufacturing["ActualStartDate"]).dt.total_seconds() / 60,
        "has_related_events": pd.NA
    })
    
    """excel_path = append_df_to_excel(
        fact_workphases,"fact_workphases"
    )"""

    

def process_and_stage_fact_machine_events(combined_manufacturing_reporting_path: Path):

    combined_reportings = pd.read_parquet(combined_manufacturing_reporting_path)

    combined_reportings["SetupTime"] = (
        combined_reportings["SetupTime"]
        .fillna(0)
        .astype("int64")
    )
    
    unit_time_min = ticks_to_minutes(combined_reportings["UnitTime"])
    setup_time_min = ticks_to_minutes(combined_reportings["SetupTime"])

    fact_machine_events = pd.DataFrame({
        "machine_event_id": to_int64(combined_reportings["ReportingId"]),
        "event_type_id": to_int32(combined_reportings["Type"]),
        "workphase_id": to_int64(combined_reportings["ManufacturingOrderOperationId"]),
        "work_number" : pd.NA,
        "phase_number": combined_reportings["OperationNumber"],
        "machine_id": to_int64(combined_reportings["WorkCenterId"]),
        "article_id" : to_int64(combined_reportings["PartId"]),
        "user_id": to_int64(combined_reportings["EmployeeId"]),
        "program_id": pd.NA,
        "start_date": get_date_key(combined_reportings["ActualStartDate"]),
        "end_date": get_date_key(combined_reportings["ActualFinishDate"]),
        "start_time": pd.NA,
        "end_time": get_date_key(combined_reportings["ActualReportedDate"]),
        "start_at": pd.NA,
        "end_at": pd.NA,
        "duration_min": pd.NA, #Lopetus - aloitusaika
        "single_item_time_min": unit_time_min + setup_time_min, #Kuinka kauan kone on tehnyt, saadaan ehkä paremmin konedatasta
        "produced_qty": combined_reportings["Quantity"],
        "effective_production_min": pd.NA,
        "effective_production_pct": pd.NA,
        "ineffective_production_min": pd.NA,
        "machine_active_pct": pd.NA,
        "idle_min": pd.NA,
        "idle_pct": pd.NA,
        "power_off_min": pd.NA,
        "power_off_pct": pd.NA,
        "exception_id": pd.NA,
        "power_consumption_kwh": pd.NA,
        "notes": pd.NA,
    })
    
    """excel_path = append_df_to_excel(
        fact_machine_events,"fact_machine_events"
    )"""



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


def process_dim():
    date_rows, work_centers = load_latest(LOCAL_STAGED_PATH / "manufacturing", "WorkCenters",LoadMode.STAGED)
    date_rows, customers = load_latest(LOCAL_STAGED_PATH / "sales", "Customers",LoadMode.STAGED)
    date_rows, order_types = load_latest(LOCAL_STAGED_PATH / "sales", "CustomerOrderTypes",LoadMode.STAGED)
    date_rows, parts = load_latest(LOCAL_COMBINED_PATH / "inventory", "CombinedInventory",LoadMode.COMBINED)

    rename_work_centers = {
        "Id" : "machine_id",
        "Description" : "machine_name",
        "Type" : "machine_type",
        "DepartmentId" : "machine_group_id", 
    }

    work_center_int_cols = ["machine_id","machine_group_id"]
    work_center_keep_columns = [ "machine_id","machine_name","manufacturer_name", "machine_type","cost_per_hour","machine_group_id","valid_from","valid_to","is_current"]
    work_centers = shape_and_types(work_centers,rename_work_centers,work_center_keep_columns,work_center_int_cols)


    rename_parts = {
        "Id_part" : "article_id",
        "Code" : "article_code",
        "PartNumber" : "article_name",
        "Alias_part" : "article_name_other",
        "Description_part" : "article_description",
        "ActiveRevisionId" : "revision",
        "StandardPrice" : "prime_cost",
        "ExtraDescription" : "notes",
    }

    part_int_columns = ["article_id","revision"]
    part_float_columns = ["prime_cost"]
    part_keep_columns = [ "article_id","article_code","article_name","article_name_other","article_description","article_group","revision","prime_cost","notes","valid_from","valid_to","is_current"]
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
    """append_df_to_excel(
        work_centers,"work_centers"
    )
    append_df_to_excel(
        parts,"parts"
    )
    append_df_to_excel(
        customers,"dim_customer"
    )
    append_df_to_excel(
        order_types,"dim_order_type"
    )"""
    


#Test for debugging the airflow behaviour without aiflow
def erp_file_processor_test():
    manufacturing_files = get_latest_endpoint_files("manufacturing")
    for endpoint, files in manufacturing_files.items():
        for file_path in files:
            process_erp_files(Path(file_path))

    common_files = get_latest_endpoint_files("common")
    for endpoint, files in common_files.items():
        for file_path in files:
            process_erp_files(Path(file_path))

    inventory_files = get_latest_endpoint_files("inventory")
    for endpoint, files in inventory_files.items():
        for file_path in files:
            process_erp_files(Path(file_path))
            
    sales_files = get_latest_endpoint_files("sales")
    for endpoint, files in sales_files.items():
        for file_path in files:
            process_erp_files(Path(file_path))
    

    combined_orders = combine_manufacturing_data(LOCAL_STAGED_PATH / "manufacturing",LOCAL_STAGED_PATH / "common",combine_manufacturing_tables)
    combined_sales = combine_sales_data(LOCAL_STAGED_PATH / "sales", combine_sales_tables)
    combined_inventory = combine_inventory_data(LOCAL_STAGED_PATH / "inventory", combine_inventory_tables)
    combined_reportings = combine_manufacturing_reportings(LOCAL_STAGED_PATH / "manufacturing",LOCAL_STAGED_PATH / "common",combine_reporting_tables)
    combined_materials = combine_sales_margin_data(LOCAL_STAGED_PATH / "sales",LOCAL_STAGED_PATH / "inventory",LOCAL_STAGED_PATH / "common",combine_material_tables)
   
    process_and_stage_fact_order_rows(Path(combined_sales))
    process_and_stage_fact_workphases(Path(combined_orders))
    process_and_stage_fact_machine_events(Path(combined_reportings))
    processed_dim = process_dim()

