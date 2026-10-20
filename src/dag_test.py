from datetime import datetime
import logging
import json
import pandas as pd
import time
import re
import os
from pathlib import Path
from dotenv import set_key, dotenv_values, load_dotenv
from erp_pipeline.config.erp_config import (combine_manufacturing_tables,combine_sales_tables,combine_work_interval_tables,combine_inventory_tables)
from erp_pipeline.config.path_config import LOCAL_RAW_PATH_STR,LOCAL_RAW_PATH, LOCAL_COMBINED_PATH, LOCAL_STAGED_PATH
from erp_pipeline.process.process_erp import process_erp_files
from erp_pipeline.helpers.file_helpers import get_latest_endpoint_files
from erp_pipeline.combine_data.combine_data_files import combine_manufacturing_data,combine_sales_data,combine_work_interval_data,combine_inventory_data
from erp_pipeline.database.database_mapper import process_and_stage_fact_machine_events,process_and_stage_fact_order_rows,process_and_stage_fact_workphases,process_dim
from erp_pipeline.fetch_erp_data.common import fetch_common
from erp_pipeline.fetch_erp_data.inventory import fetch_inventory
from erp_pipeline.fetch_erp_data.manufacturing import fetch_manufacturing
from erp_pipeline.fetch_erp_data.sales import fetch_sales
from erp_pipeline.fetch_erp_data.time_recording import fetch_time_recording
from erp_pipeline.auth.auth import login_and_get_token


logger = logging.getLogger("custom_logger")

DEVENV_PATH = Path(__file__).parent / "dev.env"
load_dotenv(DEVENV_PATH)
OUTPUT_EXCEL_FOLDER = Path(
    os.getenv("OUTPUT_EXCEL_FOLDER", "./excel_datamodel")
)
OUTPUT_EXCEL_FOLDER.mkdir(parents=True, exist_ok=True)
DATE_STR = datetime.now().strftime("%Y-%m-%d_%H%M%S")
EXCEL_PATH = OUTPUT_EXCEL_FOLDER / f"datamodel_{DATE_STR}.xlsx"
CACHE_PATH = Path(os.getenv("TOKEN_CACHE_PATH", Path.home() / ".erp_pipeline_token.json"))

_sheets_to_write: "dict[str, pd.DataFrame]" = {}

def get_cached_token_or_none():
    try:
        cached = json.loads(CACHE_PATH.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return None
    if cached.get("expires_at", 0) > time.time() + 60:
        return cached["token"]
    return None


def save_token_if_new(token_or_dict):
    if isinstance(token_or_dict, dict):
        CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = CACHE_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(token_or_dict))
        os.chmod(tmp, 0o600)
        tmp.replace(CACHE_PATH)
        return token_or_dict["token"]
    return token_or_dict

def append_df_to_excel(df: pd.DataFrame, sheet_name: str):
    print("Queuing sheet for excel: " + sheet_name)
    df = clean_dataframe_for_excel(df)
    df = convert_large_ints_to_str(df)
    _sheets_to_write[sheet_name[:31]] = df


def flush_excel():
    """
    Kirjoittaa kaikki append_df_to_excel:lla kerätyt taulut EXCEL_PATH:iin
    yhdellä ExcelWriter-sessiolla. Kutsu tämä kerran, kun kaikki DAG:n
    taulut on käsitelty (esim. erp_file_processor_test()-funktion lopussa).
    """
    if not _sheets_to_write:
        print("Ei kirjoitettavia sheettejä, ohitetaan Excel-tallennus.")
        return

    print(f"Kirjoitetaan {len(_sheets_to_write)} sheettiä tiedostoon: {EXCEL_PATH}")
    with pd.ExcelWriter(
        EXCEL_PATH,
        engine="openpyxl",
        mode="w",
    ) as writer:
        for sheet_name, df in _sheets_to_write.items():
            df.to_excel(
                writer,
                sheet_name=sheet_name,
                index=False
            )
    print(f"Excel-tallennus valmis: {EXCEL_PATH}")

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
            max_len = s.dropna().astype("string").str.len().max()

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
    
    
#Test for debugging the airflow behaviour without aiflow
def erp_file_processor_test():
    values = dotenv_values(DEVENV_PATH)

    cached_token = get_cached_token_or_none()
    if(cached_token):
        authenticate = cached_token
    else:
        fresh_result = login_and_get_token(values.get("LOGIN_URL"))
        authenticate = save_token_if_new(fresh_result)
        
    fetch_common(authenticate,values.get("COMMON_URL"),LOCAL_RAW_PATH_STR + "/common")
    fetch_inventory(authenticate,values.get("INVENTORY_URL"),LOCAL_RAW_PATH_STR + "/inventory")
    fetch_manufacturing(authenticate,values.get("MANUFACTURING_URL"),LOCAL_RAW_PATH_STR + "/manufacturing")
    fetch_sales(authenticate,values.get("SALES_URL"),LOCAL_RAW_PATH_STR + "/sales")
    fetch_time_recording(authenticate,values.get("TIMERECORDING_URL"),LOCAL_RAW_PATH_STR + "/time_recording")

    categories = [
        "manufacturing",
        "common",
        "inventory",
        "sales",
        "time_recording",
    ]

    for category in categories:
        endpoint_files = get_latest_endpoint_files(
            category,
            LOCAL_RAW_PATH
        )

        for files in endpoint_files.values():
            for file_path in files:
                process_erp_files(
                    Path(file_path),
                    staged_path=LOCAL_STAGED_PATH
                )
    
    
        
    combined_manufacturing = combine_manufacturing_data(LOCAL_STAGED_PATH / "manufacturing",LOCAL_STAGED_PATH / "common",LOCAL_STAGED_PATH / "sales", combine_manufacturing_tables,combined_path=LOCAL_COMBINED_PATH)
    combined_sales = combine_sales_data(LOCAL_STAGED_PATH / "sales",LOCAL_STAGED_PATH /"manufacturing", combine_sales_tables,combined_path=LOCAL_COMBINED_PATH)
    combined_inventory = combine_inventory_data(LOCAL_STAGED_PATH / "inventory", combine_inventory_tables,combined_path= Path(LOCAL_COMBINED_PATH))
    #combined_reportings = combine_manufacturing_reportings(LOCAL_STAGED_PATH / "manufacturing",LOCAL_STAGED_PATH / "common",LOCAL_STAGED_PATH / "sales",combine_reporting_tables)
    #combined_materials = combine_sales_margin_data(LOCAL_STAGED_PATH / "sales",LOCAL_STAGED_PATH / "inventory",LOCAL_STAGED_PATH / "common",combine_material_tables)
    combined_work_intervals = combine_work_interval_data(LOCAL_STAGED_PATH / "time_recording",LOCAL_STAGED_PATH / "manufacturing",combine_work_interval_tables,combined_path=LOCAL_COMBINED_PATH)
   
    fact_order_rows = process_and_stage_fact_order_rows(Path(combined_sales))
    fact_workphases = process_and_stage_fact_workphases(Path(combined_manufacturing))
    fact_machine_events = process_and_stage_fact_machine_events(Path(combined_work_intervals))
    processed_dims = process_dim(staged_path= LOCAL_STAGED_PATH, combined_path= LOCAL_COMBINED_PATH)

    append_df_to_excel(
        fact_order_rows,"fact_order_rows"
    )

    append_df_to_excel(
        fact_workphases,"fact_workphases"
    )
    
    append_df_to_excel(
        fact_machine_events,"fact_machine_events"
    )
    
    for table_name, df in processed_dims.items():
        append_df_to_excel(df, table_name)

    flush_excel()



""" def combine_sales_margin_data(
    staged_sales_path: Path,
    staged_inventory_path: Path,
    staged_common_path: Path,
    mapping: dict
):

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

    data = invoice_logs.merge(
        parts,
        left_on="PartId",
        right_on="Id",
        how="left",
        suffixes=("", "_Part")
    )

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

    
    customer_id = (
        _clean_key_series(data["CustomerId"])
        if "CustomerId" in data.columns
        else pd.Series(np.nan, index=data.index, dtype="object")
    )

    extra_customer_code = (
        _clean_key_series(data["ExtraCustomerCode"])
        if "ExtraCustomerCode" in data.columns
        else pd.Series(np.nan, index=data.index, dtype="object")
    )

    customer_code = (
        _clean_key_series(data["CustomerCode"])
        if "CustomerCode" in data.columns
        else pd.Series(np.nan, index=data.index, dtype="object")
    )

    data["CustomerKey"] = (
        customer_id
        .fillna(extra_customer_code)
        .fillna(customer_code)
    )



    # Vektoroitu versio aiemmasta data.apply(..., axis=1) -toteutuksesta.
    # Sama logiikka: jaetaan ConversionFactor:lla vain jos OrderRowType == 1
    # ja ConversionFactor on olemassa eikä ole 0, muuten käytetään sellaisenaan.
    order_row_type = data["OrderRowType"] if "OrderRowType" in data.columns else pd.Series(np.nan, index=data.index)
    conversion_factor = data["ConversionFactor"] if "ConversionFactor" in data.columns else pd.Series(np.nan, index=data.index)

    adj_mask = (
        (order_row_type == 1)
        & conversion_factor.notna()
        & (conversion_factor != 0)
    )

    data["AdjQuantity"] = np.where(
        adj_mask,
        data["InvoicedQuantity"] / conversion_factor,
        data["InvoicedQuantity"]
    )

    data["Revenue"] = (
        (data["SetupPrice"].fillna(0)
         + data["Price"].fillna(0) * data["AdjQuantity"])
        * (1 - data["Discount"].fillna(0) / 100)
        * data["ExchangeRate"].fillna(1)
    )

    data["UnitCost"] = (
        data["OverrideStandardPrice"]
        .combine_first(data["StandardPrice_InvoiceRow"])
        .combine_first(data["StandardPrice_Part"])
        .combine_first(data["StandardPrice"])
        .fillna(0)
    )

   
    data["Cost"] = data["UnitCost"] * data["InvoicedQuantity"]

    data["Margin"] = data["Revenue"] - data["Cost"]

    # Vektoroitu versio aiemmasta data.apply(..., axis=1) -toteutuksesta.
    data["MarginPct"] = np.where(
        data["Revenue"] != 0,
        data["Margin"] / data["Revenue"] * 100,
        0
    )


    date_str = datetime.now().strftime("%Y-%m-%d")
    endpoint = "materials"

    
    saved_parquet = save_combined_parquet(
        data,
        date_str,
        LOCAL_COMBINED_PATH / endpoint,
        endpoint
)

    clean_parquet_history(
        endpoint,
        LOCAL_COMBINED_PATH / endpoint,
        days_to_keep=2
    )

    return saved_parquet
"""


"""
def combine_manufacturing_reportings(
    staged_path: Path,
    staged_common_path: Path,
    staged_sales_path : Path,
    mapping: dict
):
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

    _, extraFields = load_latest(
        staged_common_path,
        "ExtraFields",
        LoadMode.STAGED,
        mapping["ExtraFields"]
    )

    _, extraFieldGroups = load_latest(
        staged_common_path,
        "ExtraFieldGroups",
        LoadMode.STAGED,
        mapping["ExtraFieldGroups"]
    )

    _, extraFieldOptionTemplates = load_latest(
        staged_common_path,
        "ExtraFieldOptionTemplates",
        LoadMode.STAGED,
        mapping["ExtraFieldOptionTemplates"]
    )

    _, extraFieldTemplates = load_latest(
        staged_common_path,
        "ExtraFieldTemplates",
        LoadMode.STAGED,
        mapping["ExtraFieldTemplates"]
    )

    _, customers = load_latest(
        staged_sales_path,
        "Customers",
        LoadMode.STAGED, 
        mapping["Customers"]
    )

    # Renamet ennen mergejä
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

    
    customers = customers.rename(columns={
        "Id": "ResolvedCustomerId",
        "Code": "ResolvedCustomerCode"
    })

    
    # Partin extrafieldit
    part_extrafields = resolve_part_extrafields_by_group_name(
        extraFieldGroups=extraFieldGroups,
        extraFieldTemplates=extraFieldTemplates,
        extraFieldOptionTemplates=extraFieldOptionTemplates,
        extraFields=extraFields,
        group_name="lisätiedot"
    )

    # Liitä extrafieldit valmistusmääräimeen PartId:n kautta
    manufacturing_orders = manufacturing_orders.merge(
        part_extrafields,
        on="PartId",
        how="left"
    )

    
    if "Code" in manufacturing_orders.columns:
        manufacturing_orders["Code"] = manufacturing_orders["Code"].astype("string").str.strip()
        customers["ResolvedCustomerCode"] = customers["ResolvedCustomerCode"].astype("string").str.strip()

        customers = customers.drop_duplicates(subset=["ResolvedCustomerCode"])

        manufacturing_orders = manufacturing_orders.merge(
            customers[["ResolvedCustomerId", "ResolvedCustomerCode"]],
            left_on="Code",
            right_on="ResolvedCustomerCode",
            how="left"
        )
    
    fact = (
        reportingRows
        .merge(
            orderOperations,
            on="ManufacturingOrderOperationId",
            how="left"
        )
    )
    
    for dt_col in ["ReportedWhen", "CreatedAt", "UpdatedAt"]:
        fact = _clean_datetime_column(fact, dt_col)

    numeric_cols = [
        "Quantity", "RejectedQuantity",
        "SetupTime", "UnitTime",
        "PlannedSetupTime", "PlannedUnitTime",
        "SetupCost", "UnitCost",
        "SetupCostFactor1", "SetupCostFactor2", "SetupCostFactor3",
        "UnitCostFactor1", "UnitCostFactor2", "UnitCostFactor3",
        "IsSubcontractor",
    ]

    for col in numeric_cols:
        if col in fact.columns:
            fact[col] = pd.to_numeric(fact[col], errors="coerce")

    quantity = _series_or_zero(fact, "Quantity")
    rejected_quantity = _series_or_zero(fact, "RejectedQuantity")

    setup_time = _series_or_zero(fact, "SetupTime")
    unit_time = _series_or_zero(fact, "UnitTime")

    planned_setup_time = _series_or_zero(fact, "PlannedSetupTime")
    planned_unit_time = _series_or_zero(fact, "PlannedUnitTime")

    setup_cost = _series_or_zero(fact, "SetupCost")
    unit_cost = _series_or_zero(fact, "UnitCost")

    setup_factor_1 = _series_or_zero(fact, "SetupCostFactor1")
    setup_factor_2 = _series_or_zero(fact, "SetupCostFactor2")
    setup_factor_3 = _series_or_zero(fact, "SetupCostFactor3")

    unit_factor_1 = _series_or_zero(fact, "UnitCostFactor1")
    unit_factor_2 = _series_or_zero(fact, "UnitCostFactor2")
    unit_factor_3 = _series_or_zero(fact, "UnitCostFactor3")

    is_subcontractor = _series_or_zero(fact, "IsSubcontractor")


    fact["SetupHours"] = setup_time / TIME_DIVISOR
    fact["UnitHours"] = unit_time / TIME_DIVISOR
    fact["PlannedSetupHours"] = planned_setup_time / TIME_DIVISOR
    fact["PlannedUnitHours"] = planned_unit_time / TIME_DIVISOR

    fact["SetupFactorTotal"] = setup_factor_1 + setup_factor_2 + setup_factor_3
    fact["UnitFactorTotal"] = unit_factor_1 + unit_factor_2 + unit_factor_3

    fact["CalculatedSetupCost"] = np.where(
        is_subcontractor == 1,
        setup_cost,
        fact["SetupFactorTotal"] * fact["SetupHours"]
    )
    fact["PlannedSetupCost"] = np.where(
        is_subcontractor == 1,
        setup_cost,
        fact["SetupFactorTotal"] * fact["PlannedSetupHours"]
    )

    fact["CalculatedUnitCost"] = np.where(
        is_subcontractor == 1,
        unit_cost,
        fact["UnitFactorTotal"] * fact["UnitHours"]
    )

    fact["PlannedUnitCost"] = np.where(
        is_subcontractor == 1,
        setup_cost,
        fact["UnitFactorTotal"] * fact["PlannedUnitHours"]
    )

    fact["TotalHours"] = fact["SetupHours"] + fact["UnitHours"]
    fact["TotalPlannedHours"] = fact["PlannedSetupHours"] + fact["PlannedUnitHours"]
    fact["TotalCost"] = fact["CalculatedSetupCost"] + fact["CalculatedUnitCost"]
    fact["TotalPlannedCost"] = fact["PlannedSetupCost"] + fact["PlannedUnitCost"]

    fact["ActualHourlyRate"] = np.where(
        fact["TotalHours"] > 0,
        fact["TotalCost"] / fact["TotalHours"],
        np.nan
    )
    fact["PlannedHourlyRate"] = np.where(
            fact["TotalPlannedHours"] > 0,
            fact["TotalPlannedCost"] / fact["TotalPlannedHours"],
            np.nan
        )
    # Yhdistä raporttirivit -> operaatiot -> valmistusmääräimet
    merged = (
        fact
        .merge(
            manufacturing_orders,
            on="ManufacturingOrderId",
            how="left"
        )
    )

    # Päivämäärät
    for dt_col in ["ReportedWhen", "CreatedAt", "UpdatedAt"]:
        merged = _clean_datetime_column(merged, dt_col)

    # Numeeriset sarakkeet joita käytetään laskennassa
    numeric_cols = [
        "Quantity",
        "RejectedQuantity",
        "SetupTime",
        "PlannedSetupTime",
        "UnitTime",
        "PlannedUnitTime",
        "ReportedUnitTime",
        "ReportedSetupTime",
        "SetupCost",
        "UnitCost",
        "SetupCostFactor1",
        "SetupCostFactor2",
        "SetupCostFactor3",
        "UnitCostFactor1",
        "UnitCostFactor2",
        "UnitCostFactor3",
        "IsSubcontractor",
    ]

    for col in numeric_cols:
        if col in merged.columns:
            merged[col] = pd.to_numeric(merged[col], errors="coerce")


    customer_id = (
        _clean_key_series(merged["CustomerId"])
        if "CustomerId" in merged.columns
        else pd.Series(np.nan, index=merged.index, dtype="object")
    )

    resolved_customer_id = (
        _clean_key_series(merged["ResolvedCustomerId"])
        if "ResolvedCustomerId" in merged.columns
        else pd.Series(np.nan, index=merged.index, dtype="object")
    )

    code = (
        _clean_key_series(merged["Code"])
        if "Code" in merged.columns
        else pd.Series(np.nan, index=merged.index, dtype="object")
    )

    merged["CustomerKey"] = customer_id.fillna(resolved_customer_id).fillna(code)

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

    date_str = datetime.now().strftime("%Y-%m-%d")
    endpoint = "reportings"

    saved = save_combined_parquet(
        merged,
        date_str,
        LOCAL_COMBINED_PATH / endpoint,
        endpoint
    )

    clean_parquet_history(
        endpoint,
        LOCAL_COMBINED_PATH / endpoint,
        days_to_keep=7
    )

    return saved
"""
"""excel_path = append_df_to_excel(
        fact_order_rows,"fact_order_rows"
    )"""