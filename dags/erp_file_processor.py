from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import logging
import json
import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.parser import parse
import os
import pytz
import locale

from babel.numbers import parse_decimal

from decimal import Decimal
from typing import Optional

from airflow.utils.context import Context

########## Global Google Cloud Setup ##########

LOCAL_BASE_PATH = "/src/results"
TEMP_FILE_PATH = "data/temp-files"



# Define known source types
ERP_COMMON_SOURCE_TYPES = [
    "erp_common_currencies",
    "erp_common_extrafieldgroups",
    "erp_common_extrafieldoptiontemplates",
    "erp_common_extrafields",
    "erp_common_extrafieldtemplates",
    "erp_common_reasoncodes",
    "erp_common_units"
]

ERP_INVENTORY_SOURCE_TYPES = [
    "erp_inventory_partcodes",
    "erp_inventory_parts",
]

ERP_MANUFACTURING_SOURCE_TYPES = [
    "erp_manufacturing_drawings",
    "erp_manufacturing_manualOperationStatuses",
    "erp_manufacturing_orderMaterials",
    "erp_manufacturing_orderNodeDrawings",
    "erp_manufacturing_orderNodes",
    "erp_manufacturing_orderOperationControlDataRows",
    "erp_manufacturing_orderOperationReportings",
     "erp_manufacturing_orderOperations",
    "erp_manufacturing_orders",
    "erp_manufacturing_orderTypes",
    "erp_manufacturing_materialRows",
    "erp_manufacturing_operationRowControlDataRows",
    "erp_manufacturing_operationRows",
     "erp_manufacturing_orderQuantityManufacturingOrderOperations",
    "erp_manufacturing_orderQuantityManufacturingOrders",
    "erp_manufacturing_preparations",
    "erp_manufacturing_productionCalendarExceptions",
    "erp_manufacturing_reasonCodeTimeConsumptions",
    "erp_manufacturing_workCenterCostFactorGroups",
    "erp_manufacturing_workCenters",
]

ERP_SALES_SOURCE_TYPES = [
    "erp_sales_customerOrderRows",
    "erp_sales_customerOrders",
    "erp_sales_customerOrderTypes",
    "erp_sales_customers",
    "erp_sales_invoiceLogs",
    "erp_sales_orderQuantityCustomerOrders",
    "erp_sales_salesPrices",
]

euro_columns = {
    "erp_inventory_parts" : ["AveragePrice", "StandardPrice"],
    "erp_manufacturing_orderOperationReportings" : ["SetupCost","UnitCost"],
    "erp_manufacturing_orderRows" : ["SetupCost","UnitCost"],
    "erp_sales_customerOrderRows" : ["Price","PriceInCompanyCurrency","SetupPrice"],
    "erp_sales_invoiceLogs" : ["Price","SetupPrice","PriceInCompanyCurrency",
                               "SetupPriceInCompanyCurrency","StandardPrice"],
   
}

# Airflow related configs

SNOWFLAKE_CONN_ID = "tuova_conn"
DB = "TUOVA_DB"
SCHEMA = "ETL_STAGE"
CONTROL_TABLE = "FILE_LOAD_MAP"               # TUOVA_DB.ETL_STAGE.FILE_LOAD_MAP
EXT_STAGE = "EXT_GCS_TUOVA"                   # TUOVA_DB.ETL_STAGE.EXT_GCS_TUOVA
FILE_FORMAT = "PARQUET"                       # TUOVA_DB.ETL_STAGE.PARQUET

# ================

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
    Accepts scalar or pandas Series. Vectorized for Series input.
    """
    if isinstance(dt, pd.Series):
        s = dt
        # If not datetime already, parse once (cached)
        if not pd.api.types.is_datetime64_any_dtype(s):
            s = pd.to_datetime(s, errors='coerce', dayfirst=True, cache=True)
        out = pd.to_numeric(s.dt.strftime("%Y%m%d"), errors='coerce')
        return out.astype("Int64")  # NA-aware integer
    # --- scalar fallback (original behavior) ---
    if pd.isnull(dt):
        return pd.NA
    if isinstance(dt, str):
        dt = pd.to_datetime(dt, errors='coerce', dayfirst=True)
        if pd.isnull(dt):
            return pd.NA
    return int(dt.strftime("%Y%m%d"))


def get_time_key(dt):
    """
    Convert to seconds since midnight with <NA> for missing.
    Accepts scalar or pandas Series. Vectorized for Series input.
    """
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

def parse_erp_dates(df, date_columns):
    for col in date_columns:
        df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
    return df

def parse_euro_number(x):
    """
    Parses a European-style number string to float.
    Returns original value if parsing fails.
    """
    try:
        # Replace decimal comma with dot
        x_clean = x.replace(',', '.')
        return float(x_clean)
    except Exception:
        return x

def load_csv_with_converters(file_path, euro_columns = list(), sep=';', dtypes= dict()):
   
    converters = {col: parse_euro_number for col in euro_columns}

    logging.info(f"Loading CSV file: {file_path}")
    
    return pd.read_csv(file_path, sep=sep, converters=converters, dtype=dtypes)



# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_json_with_converters(file_path, euro_columns=list(), dtypes=dict()):
    """
    Loads a single JSON file and converts euro-formatted numbers and dtypes.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON in {file_path}: {e}")
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
                logging.warning(f"Failed to convert column {col} to {dtype} in {file_path}: {e}")

    return df


def combine_json_files(files: list, file_type: str = "files",euro_columns=list(), dtypes=dict()) -> Optional[pd.DataFrame]:
    """
    Combines multiple JSON files into a single pandas DataFrame.

    Same behavior as combine_csv_files, with:
      - error handling
      - single-file optimization
      - data type conversions
    """
    if not files:
        logging.warning(f"No {file_type} found.")
        return None

    # --- Optimize single file case ---
    if len(files) == 1:
        file_path = files[0]
        try:
            df = load_json_with_converters(file_path, euro_columns, dtypes=dtypes)
            if df is None:
                return None
            logging.info(f"Successfully read single JSON file: {file_path}")
            return df
        except FileNotFoundError:
            logging.error(f"File not found: {file_path}")
            return None

    # --- Multi-file case ---
    dataframes = []
    for f in files:
        try:
            df = load_json_with_converters(f, euro_columns, dtypes=dtypes)
            if df is not None:
                dataframes.append(df)
                logging.info(f"Successfully read JSON file: {f}")
        except FileNotFoundError:
            logging.error(f"File not found: {f}")
            continue
        except json.JSONDecodeError as e:
            logging.error(f"Error reading JSON file {f}: {e}")
            continue

    if not dataframes:
        logging.warning(f"No valid {file_type} JSON dataframes to process.")
        return None

    combined_df = pd.concat(dataframes, ignore_index=True)
    logging.info(f"Successfully concatenated {len(dataframes)} {file_type}.")

    return combined_df


###### Airflow things ##########

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
########## Tasks ##########


@task 
def list_erp_files_from_local_project():
    
    date_str = datetime.now().strftime("%Y-%m-%d")
    todays_files = []

    # loop through all endpoint directories
    for entry in os.scandir(LOCAL_BASE_PATH):
        if not entry.is_dir():
            continue

        todays_folder = os.path.join(entry.path, date_str)
        if not os.path.isdir(todays_folder):
            continue

        # collect files under this date folder
        for file_entry in os.scandir(todays_folder):
            if file_entry.is_file():
                todays_files.append(file_entry.path)

    return todays_files




def save_clean_json(endpoint: str, data: pd.DataFrame, base_path: str):
    endpoint_name = endpoint.lstrip("/").replace("/", "-")
    if not endpoint_name:
        endpoint_name = "root"

    folder_path = os.path.join(base_path, endpoint_name)
    os.makedirs(folder_path, exist_ok=True)

    date_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"{date_str}-{endpoint_name}.json"
    full_path = os.path.join(folder_path, filename)

    json_ready = data.to_dict(orient="records")

    with open(full_path, "w", encoding="utf-8") as f:
        json.dump(json_ready, f, indent=4, ensure_ascii=False)

    print(f"Saved cleaned data: {full_path}")
    return full_path




@task()
def process_erp_workcenters(files: list):
    """Process ERP workcenter files."""
    if not files:
        logging.warning("No ERP workcenter files found.")
        return
    
    erp_workcenters = combine_json_files(files, file_type="erp_workcenters", euro_columns=euro_columns["erp_workcenters"])

    if erp_workcenters is not None:
        logging.info(f"📊 Processed {len(erp_workcenters)} ERP workcenters records.")
        workcenters_dm = erp_workcenters[["Id", "PricePerHour","Description", "OperationDescription", "BasicTime"]]

        save_clean_json("Manufacturing",workcenters_dm,"results/staged")

        logging.info(f"Machines DataFrame shape: {workcenters_dm.head(5)}")


def erp_file_processor():
    erp_files = list_erp_files_from_local_project()
    
    

dag_instance = erp_file_processor()
