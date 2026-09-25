from __future__ import annotations
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from typing import Dict, List
from pathlib import Path
from airflow.models import Variable

import time
import logging
logger = logging.getLogger("airflow.task")

combine_sales_tables = {
    "CustomerOrderRows" : ["Id","PartId","DeliveredQuantity","DeliveryDate","DesiredDeliveryDate","LinkedManufacturingOrderId","InitialDeliveryDate","OrderDate","OrderedQuantity","OrderRowType",
                                     "ParentOrderId","ParentRowId","Price","PriceInCompanyCurrency","RevisionId","NewFinishDate","UnitId","PartStatus","Status"],
    "CustomerOrders" : ["Id","CustomerId","InvoiceCustomerId","OrderDate","OrderNumber","OrderTypeId","Status","SellerId"],
    "Customers" : ["Id","Name","Code","AlternativeName"],
    "InvoiceLogs" : ["Id","CustomerOrderId","InvoicedQuantity","Price","LoggingTimestamp","InvoiceDate"],
    "ManufacturingOrders" : ["Id","OrderNumber","StartDate","InitialFinishDate","FinishDate","CustomerOrderId","Status"],
    "ManufacturingOrderOperations" : ["Id","ManufacturingOrderId","ActualStartDate","ActualFinishDate"],
}

combine_manufacturing_tables = {
    "ManufacturingOrders" : ["Id","OrderNumber","OrderTypeId","CustomerOrderId","CustomerId","InitialFinishDate","Status","PartId","ReportedQuantity","PlannedQuantity",],
    "ManufacturingOrderOperations" : ["Id","WorkCenterId","PartId",
        "PlannedQuantity","ReportedQuantity","RejectedQuantity","RestQuantity",
        "ReportedUnitTime","PlannedUnitTime","ReportedSetupTime","PlannedSetupTime",
        "ManufacturingOrderId","OperationNumber","OperationRowId","PlannedStartDate","ActualStartDate",
        "PlannedFinishDate","ActualFinishDate","WorkshopOperationStatus","Description","FixedLeadTime"],
    "OperationRows" : ["Id","OrderOperation","UnitTime","IsSubcontractor","UnitCost","SetupCost","FixedLeadTime"],
    "ExtraFieldGroups" : ["Id","Name","RowNumber"],
    "ExtraFieldOptionTemplates" : ["Id","Code","Description","ExtraFieldTemplateId"],
    "ExtraFields" : ["Id","ParentId","ParentClass","SelectedOptionId","SelectedOptionIds"],
    "ExtraFieldTemplates" : ["Id","Name","ParentId","RowNumber"],
    "WorkCenters" : ["Id","Number"],
    "CustomerOrderRows" : ["Id","LinkedManufacturingOrderId"],
}

combine_reporting_tables = {
    "ManufacturingOrders" : ["Id","OrderNumber","OrderTypeId","CustomerOrderId","CustomerId","InitialFinishDate","Status","PartId",],
    "ManufacturingOrderOperationReportings" : ["Id","OperationId","Quantity","SetupTime","UnitTime","WarehouseId","EmployeeId","ActualReportedDate",
        "PreviousRestQuantity","RestQuantity","MainPartId","UnitCost","SetupCost","Type","ReportingTimestamp","CommentId","SetupCostFactor1",
        "SetupCostFactor2","SetupCostFactor3","UnitCostFactor1","UnitCostFactor2","UnitCostFactor3"],
    "ManufacturingOrderOperations" : ["Id","WorkCenterId",
    "PlannedQuantity","ReportedQuantity","RejectedQuantity",
    "ReportedUnitTime","PlannedUnitTime","ReportedSetupTime","PlannedSetupTime",
    "ManufacturingOrderId","OperationNumber","OperationRowId","PlannedStartDate","ActualStartDate",
    "PlannedFinishDate","ActualFinishDate","WorkshopOperationStatus","Description"],
    "ExtraFieldGroups" : [
        "Id","Name","RowNumber"
    ],
    "ExtraFieldOptionTemplates" : [
        "Id","Code","Description","ExtraFieldTemplateId"
    ],
    "ExtraFields" : [
        "Id","ParentId","ParentClass","SelectedOptionId","SelectedOptionIds"
    ],
    "ExtraFieldTemplates" : [
        "Id","Name","ParentId","RowNumber"
    ],
    "Customers" : ["Id","Code"],
}

combine_inventory_tables = {
    "PartCodes" : ["Id","Alias","Code","Description"],
    "Parts" : ["Id","PartNumber","Description","ActiveRevisionId","Alias","AveragePrice","CompanyId","PartCodeId","StandardPrice","ExtraDescription"]
}

combine_purchase_tables = {
     "PurchaseOrders" : [
            "Id","WarehouseId","ProjectId","OrderAmount","OrderAmountInCompanyCurrency","IsStockOrder","IsConfirmed",
            "CustomerOrderId","OrderNumber","OrderDate", "Rows","Status","OrderTypeId","CategoryString",
            ],
        "PurchaseOrderRows" : [
            "Id","OrderRowType","PartId","ParentRowId","DeliveryDate","OrderedQuantity","Price","Discount","WarehouseId","OrderDate",
            "StandardPrice","SetupPrice","RestQuantity","DeliveredQuantity",
            "IsConfirmed","LinkedSubcontractingManufacturingOrderOperation",
            "SubPartParentRowId","ParentOrderId","PriceOrigin","OrderQuantityId","OrderQuantity",
            ],
}

combine_material_tables = {
    "InvoiceLogs": [
        "Id","PartId","CustomerCode",
        "InvoicedQuantity","Price","SetupPrice","Discount","ExchangeRate",
        "OrderRowType","ConversionFactor","DeliveryRowId",
        "StandardPrice","FifoPrice","InvoiceDate"
    ],
    "Parts": [
        "Id","PartNumber","Description","StandardPrice","Type"
    ],
    "Customers": [
        "Id","Code","Name"
    ],
    "ExtraFieldGroups" : [
        "Id","Name","RowNumber"
    ],
    "ExtraFieldOptionTemplates" : [
        "Id","Code","Description","ExtraFieldTemplateId"
    ],
    "ExtraFields" : [
        "Id","ParentId","ParentClass","SelectedOptionId","SelectedOptionIds"
    ],
    "ExtraFieldTemplates" : [
        "Id","Name","ParentId","RowNumber"
    ],
    "CustomerOrderInvoiceRows" : [
        "Id","OverrideStandardPrice","StandardPrice"
    ],
    "CustomerOrderDeliveryRows" : [
        "Id","InvoiceRowId","CustomerOrderRowId"
    ],
    "CustomerOrderRows" : [
        "Id","Price","StandardPrice"
    ],
}

combine_work_interval_tables = {
    "ManufacturingOrders" : ["Id","OrderNumber","PartId"],
    "ManufacturingOrderOperations" : ["Id","OrderNumber","ManufacturingOrderId","WorkCenterId","PartId"],
    "WorkIntervals" : ["Id","OperationId","ReportingEmployeeId","IntervalStart","IntervalEnd","WorkedTime","ReportedQuantity","RejectedQuantity"],
}

RAW_PATH = Path("/opt/airflow/results/raw")
STAGED_PATH = Path("/opt/airflow/results/staged")
COMBINED_PATH = Path("/opt/airflow/results/combined")
VARIABLE_KEY = "erp_auth_token"
LOG_FILE = Path(__file__).resolve().parent / "app.log"

PYTHON_VENV_PATH = "/opt/airflow/config/python-venvs/erp_pipeline/.venv/bin/python"
ENV_PATH = "/opt/airflow/config/python-venvs/erp_pipeline/.env"
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@task()
def get_cached_token_or_none():
    cached = Variable.get(VARIABLE_KEY, default_var=None, deserialize_json=True)
    if cached and cached.get("expires_at", 0) > time.time():
        return cached["token"]
    return None

@task.external_python(python=PYTHON_VENV_PATH, task_id="login_task")
def login_task(cached_token, env_path=None):
    if cached_token:
        return cached_token
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=env_path)
    from erp_pipeline.auth.auth import login_and_get_token
    result = login_and_get_token()
    return result

@task()
def save_token_if_new(token_or_dict):
    if isinstance(token_or_dict, dict):
        Variable.set(VARIABLE_KEY, token_or_dict, serialize_json=True)
        return token_or_dict["token"]
    return token_or_dict

@task.external_python(python=PYTHON_VENV_PATH, task_id="fetch_common_task")
def fetch_common_task(token,env_path=None):
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=env_path)
    from erp_pipeline.fetch_erp_data.common import fetch_common
    fetch_common(token)

@task.external_python(python=PYTHON_VENV_PATH, task_id="fetch_inventory_task")
def fetch_inventory_task(token,env_path=None):
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=env_path)
    from erp_pipeline.fetch_erp_data.inventory import fetch_inventory
    fetch_inventory(token)

@task.external_python(python=PYTHON_VENV_PATH, task_id="fetch_manufacturing_task")
def fetch_manufacturing_task(token,env_path=None):
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=env_path)
    from erp_pipeline.fetch_erp_data.manufacturing import fetch_manufacturing
    fetch_manufacturing(token)

@task.external_python(python=PYTHON_VENV_PATH, task_id="fetch_sales_task")
def fetch_sales_task(token,env_path=None):
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=env_path)
    from erp_pipeline.fetch_erp_data.sales import fetch_sales
    fetch_sales(token)


@task.external_python(python=PYTHON_VENV_PATH, task_id="fetch_time_recording_task")
def fetch_time_recording_task(token,env_path=None):
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=env_path)
    from erp_pipeline.fetch_erp_data.time_recording import fetch_time_recording
    fetch_time_recording(token)

@task.external_python(python=PYTHON_VENV_PATH, task_id="get_latest_endpoint_files",multiple_outputs=True,trigger_rule=TriggerRule.ALL_SUCCESS)
def get_latest_endpoint_files_task(endpoint : str, env_path=None):
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=env_path)
    from erp_pipeline.helpers.file_helpers import get_latest_endpoint_files

    raw = get_latest_endpoint_files(endpoint)
    return {ep: [str(p) for p in paths] for ep, paths in raw.items()}

@task.external_python(python=PYTHON_VENV_PATH, task_id="process_erp_files", trigger_rule=TriggerRule.ALL_SUCCESS)
def process_erp_files_task(my_files,env_path=None):
    from pathlib import Path
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=env_path)
    from erp_pipeline.process.process_erp import process_erp_files
    print("Processing files")

    for _, files in my_files.items():
        for file_path in files:
            print(file_path)
            process_erp_files(Path(file_path))

@task.external_python(python=PYTHON_VENV_PATH, task_id="combine_sales_data",trigger_rule=TriggerRule.ALL_SUCCESS)
def combine_sales_data_task(staged_path: str, manufacturing_path: str, mapping: dict,env_path=None):
    from pathlib import Path
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=env_path)
    from erp_pipeline.combine_data.combine_data_files import combine_sales_data

    return combine_sales_data(Path(staged_path),Path(manufacturing_path),mapping)


@task.external_python(python=PYTHON_VENV_PATH, task_id="combine_material_data",trigger_rule=TriggerRule.ALL_SUCCESS)
def combine_material_data_task(combined_sales_path:str,staged_path: str, mapping: dict,env_path=None):
    from pathlib import Path
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=env_path)
    from erp_pipeline.combine_data.combine_data_files import combine_material_data

    return combine_material_data(Path(combined_sales_path),Path(staged_path),mapping)


@task.external_python(python=PYTHON_VENV_PATH, task_id="combine_inventory_data",trigger_rule=TriggerRule.ALL_SUCCESS)
def combine_inventory_data_task(staged_path: str, mapping: dict,env_path=None):
    from pathlib import Path
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=env_path)
    from erp_pipeline.combine_data.combine_data_files import combine_inventory_data

    return combine_inventory_data(Path(staged_path),mapping)


@task.external_python(python=PYTHON_VENV_PATH, task_id="combine_manufacturing_data",trigger_rule=TriggerRule.ALL_SUCCESS)
def combine_manufacturing_data_task(staged_manufacturing_path: str,staged_commmon_path : str,staged_sales_path:str, mapping: dict,env_path=None):
    from pathlib import Path
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=env_path)
    from erp_pipeline.combine_data.combine_data_files import combine_manufacturing_data

    return combine_manufacturing_data(Path(staged_manufacturing_path),Path(staged_commmon_path),Path(staged_sales_path), mapping)


@task.external_python(python=PYTHON_VENV_PATH, task_id="combine_purchase_data",trigger_rule=TriggerRule.ALL_SUCCESS)
def combine_purchase_data_task(staged_path: str, mapping: dict,env_path=None):
    from pathlib import Path
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=env_path)
    from erp_pipeline.combine_data.combine_data_files import combine_purchase_data

    return combine_purchase_data(Path(staged_path),mapping)


@task.external_python(python=PYTHON_VENV_PATH, task_id="combine_work_interval_data",trigger_rule=TriggerRule.ALL_SUCCESS)
def combine_work_interval_data_task(staged_path: str, staged_manufacturing_path : str, mapping: dict,env_path=None):
    from pathlib import Path
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=env_path)
    from erp_pipeline.combine_data.combine_data_files import combine_work_interval_data

    return combine_work_interval_data(Path(staged_path),Path(staged_manufacturing_path),mapping)


@task.external_python(python=PYTHON_VENV_PATH, task_id="process_and_stage_fact_order_rows",trigger_rule=TriggerRule.ALL_SUCCESS)
def process_and_stage_fact_order_rows_task(combined_sales_path,env_path=None):
    from pathlib import Path
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=env_path)
    from erp_pipeline.database.database_mapper import process_and_stage_fact_order_rows
    path = Path(combined_sales_path)
    return process_and_stage_fact_order_rows(path)


@task.external_python(python=PYTHON_VENV_PATH, task_id="process_and_stage_fact_workphases", trigger_rule=TriggerRule.ALL_SUCCESS)
def process_and_stage_fact_workphases_task(combined_manufacturing_path,env_path=None):
    from pathlib import Path
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=env_path)
    from erp_pipeline.database.database_mapper import process_and_stage_fact_workphases

    path = Path(combined_manufacturing_path)
    return process_and_stage_fact_workphases(path)
   

@task.external_python(python=PYTHON_VENV_PATH, task_id="process_and_stage_fact_machine_events", trigger_rule=TriggerRule.ALL_SUCCESS)
def process_and_stage_fact_machine_events_task(combined_work_interval_path,env_path=None):
    from pathlib import Path
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=env_path)
    from erp_pipeline.database.database_mapper import process_and_stage_fact_machine_events

    path = Path(combined_work_interval_path)
    return process_and_stage_fact_machine_events(path)



@task.external_python(python=PYTHON_VENV_PATH, task_id="process_dim", trigger_rule=TriggerRule.ALL_SUCCESS)
def process_dim_task(env_path=None):
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=env_path)
    
    from erp_pipeline.database.database_mapper import process_dim, save_or_raise, DIM_CONFLICT_COLUMNS

    for table, df in process_dim().items():
        save_or_raise(df, table, conflict_columns=DIM_CONFLICT_COLUMNS[table])

@task.external_python(python=PYTHON_VENV_PATH, task_id="save_or_raise", trigger_rule=TriggerRule.ALL_SUCCESS)
def save_or_raise_task(my_file, table_name, conflict_columns, env_path=None):
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=env_path)
    from erp_pipeline.database.database_mapper import save_or_raise
    save_or_raise(my_file, table_name, conflict_columns)

@dag(
    dag_id="server_erp_file_processor",
    schedule="@daily",
    default_args=default_args,
    start_date=datetime(2025, 8, 16),
    catchup=False,
    tags=["gcs", "etl", "demo"],
)


def server_erp_file_processor():

    cached_token = get_cached_token_or_none()
    fresh_result = login_task(cached_token, env_path=ENV_PATH)
    authenticate = save_token_if_new(fresh_result)
    
    t_common = fetch_common_task(authenticate,ENV_PATH)
    t_inventory = fetch_inventory_task(authenticate,ENV_PATH)
    t_manufacturing = fetch_manufacturing_task(authenticate,ENV_PATH)
    t_sales = fetch_sales_task(authenticate,ENV_PATH)
    t_time_recordings = fetch_time_recording_task(authenticate,ENV_PATH)
    
    manufacturing_files = get_latest_endpoint_files_task("manufacturing",ENV_PATH)
    common_files = get_latest_endpoint_files_task("common",ENV_PATH)
    inventory_files = get_latest_endpoint_files_task("inventory",ENV_PATH)
    sales_files = get_latest_endpoint_files_task("sales",ENV_PATH)
    time_recording_files = get_latest_endpoint_files_task("time_recording",ENV_PATH)

    process_manufacturing = process_erp_files_task(manufacturing_files,ENV_PATH)
    process_common = process_erp_files_task(common_files,ENV_PATH)
    process_inventory = process_erp_files_task(inventory_files,ENV_PATH)
    process_sales = process_erp_files_task(sales_files,ENV_PATH)
    process_time_recordings = process_erp_files_task(time_recording_files,ENV_PATH)
    

    combined_orders = combine_manufacturing_data_task(str(STAGED_PATH / "manufacturing"),str(STAGED_PATH / "common"),str(STAGED_PATH / "sales"), combine_manufacturing_tables,ENV_PATH)
    combined_sales = combine_sales_data_task(str(STAGED_PATH / "sales"),str(STAGED_PATH / "manufacturing"), combine_sales_tables,ENV_PATH)
    combined_inventory = combine_inventory_data_task(str(STAGED_PATH / "inventory"), combine_inventory_tables,ENV_PATH)
    combined_work_intervals = combine_work_interval_data_task(str(STAGED_PATH / "time_recording"),str(STAGED_PATH / "manufacturing"),combine_work_interval_tables,ENV_PATH)

    processed_orders = process_and_stage_fact_order_rows_task(combined_sales,ENV_PATH)
    save_or_raise_task(processed_orders,"fact_order_rows",["order_row_id"])
    processed_workphases = process_and_stage_fact_workphases_task(combined_orders,ENV_PATH)
    save_or_raise_task(processed_workphases,"fact_workphases",["workphase_id"])
    processed_dim = process_dim_task(ENV_PATH)
    processed_machine_events = process_and_stage_fact_machine_events_task(combined_work_intervals,ENV_PATH)
    save_or_raise_task(processed_machine_events,"fact_machine_events",["erp_id"])

    authenticate

    authenticate >> t_common >> common_files
    common_files >> t_inventory >> inventory_files
    inventory_files >> t_manufacturing >> manufacturing_files
    manufacturing_files >> t_sales >> sales_files
    sales_files >> t_time_recordings >> time_recording_files

    manufacturing_files >> process_manufacturing >> combined_orders
    common_files >> process_common
    inventory_files >> process_inventory >> combined_inventory
    sales_files >> process_sales >> combined_sales
    time_recording_files >> process_time_recordings >> combined_work_intervals

    combined_sales >> processed_dim
    combined_orders >> processed_dim

    processed_dim >> processed_orders

    processed_orders >> processed_workphases
    combined_orders >> processed_workphases

    processed_workphases >> processed_machine_events
    combined_work_intervals >> processed_machine_events

    
dag_instance = server_erp_file_processor()
