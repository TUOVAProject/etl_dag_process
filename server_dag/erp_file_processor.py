from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import logging
from typing import Dict, List
from pathlib import Path

from fetchers.common import fetch_common
from fetchers.inventory import fetch_inventory
from fetchers.manufacturing import fetch_manufacturing
from fetchers.sales import fetch_sales
from fetchers.purchases import fetch_purchases
from fetchers.time_recording import fetch_time_recording
from auth.auth import get_token

from server_dag.combine_data.combine_data_files import combine_manufacturing_data,combine_inventory_data,combine_material_data,combine_purchase_data,combine_sales_data,combine_work_interval_data
from server_dag.helpers.file_helpers import get_latest_endpoint_files
from server_dag.database.database_mapper import process_and_stage_fact_machine_events,process_and_stage_fact_order_rows,process_and_stage_fact_workphases,process_dim
from server_dag.process.process_erp import process_erp_files

from config.erp_config import (
    combine_inventory_tables,
    combine_manufacturing_tables,
    combine_sales_tables,combine_purchase_tables,
    combine_work_interval_tables
)


logger = logging.getLogger("custom_logger")

RAW_PATH = Path("/opt/airflow/data/results/raw")
STAGED_PATH = Path("/opt/airflow/data/results/staged")
COMBINED_PATH = Path("/opt/airflow/data/results/combined")

LOG_FILE = Path(__file__).resolve().parent / "app.log"


###### Airflow things ##########

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

########## Tasks ##########

@task(multiple_outputs=True,trigger_rule=TriggerRule.ALL_SUCCESS)
def get_latest_endpoint_files_task(endpoint : str) -> Dict[str,List[str]]:
    raw = get_latest_endpoint_files(endpoint)
    return {ep: [str(p) for p in paths] for ep, paths in raw.items()}


@task(trigger_rule=TriggerRule.ALL_SUCCESS)
def combine_sales_data_task(staged_path: str, manufacturing_path: str, mapping: dict):
    return combine_sales_data(Path(staged_path),Path(manufacturing_path),mapping)


@task(trigger_rule=TriggerRule.ALL_SUCCESS)
def combine_material_data_task(combined_sales_path:str,staged_path: str, mapping: dict):
    return combine_material_data(Path(combined_sales_path),Path(staged_path),mapping)


@task(trigger_rule=TriggerRule.ALL_SUCCESS)
def combine_inventory_data_task(staged_path: str, mapping: dict):
    return combine_inventory_data(Path(staged_path),mapping)


@task(trigger_rule=TriggerRule.ALL_SUCCESS)
def combine_manufacturing_data_task(staged_manufacturing_path: str,staged_commmon_path : str,staged_sales_path:str, mapping: dict):
    return combine_manufacturing_data(Path(staged_manufacturing_path),Path(staged_commmon_path),Path(staged_sales_path), mapping)


@task(trigger_rule=TriggerRule.ALL_SUCCESS)
def combine_purchase_data_task(staged_path: str, mapping: dict):
    return combine_purchase_data(Path(staged_path),mapping)


@task(trigger_rule=TriggerRule.ALL_SUCCESS)
def combine_work_interval_data_task(staged_path: str, staged_manufacturing_path : str, mapping: dict):
    return combine_work_interval_data(Path(staged_path),Path(staged_manufacturing_path),mapping)


@task(trigger_rule=TriggerRule.ALL_SUCCESS)
def process_and_stage_fact_order_rows_task(combined_sales_path):
    path = Path(combined_sales_path)
    return process_and_stage_fact_order_rows(path)


@task(trigger_rule=TriggerRule.ALL_SUCCESS)
def process_and_stage_fact_workphases_task(combined_manufacturing_path):
    path = Path(combined_manufacturing_path)
    return process_and_stage_fact_workphases(path)
   

@task(trigger_rule=TriggerRule.ALL_SUCCESS)
def process_and_stage_fact_machine_events_task(combined_work_interval_path):
    path = Path(combined_work_interval_path)
    return process_and_stage_fact_machine_events(path)



@task(trigger_rule=TriggerRule.ALL_SUCCESS)
def process_dim_task():
    process_dim()



@task(trigger_rule=TriggerRule.ALL_SUCCESS)
def process_erp_files_task(my_files):
    logger.info("Processing files")
    for _, files in my_files.items():
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
    

    combined_orders = combine_manufacturing_data_task(str(STAGED_PATH / "manufacturing"),str(STAGED_PATH / "common"),str(STAGED_PATH / "sales"), combine_manufacturing_tables)
    combined_sales = combine_sales_data_task(str(STAGED_PATH / "sales"),str(STAGED_PATH / "manufacturing"), combine_sales_tables)
    combined_inventory = combine_inventory_data_task(str(STAGED_PATH / "inventory"), combine_inventory_tables)
    combined_purchases = combine_purchase_data_task(str(STAGED_PATH / "purchase"), combine_purchase_tables)
    combined_work_intervals = combine_work_interval_data_task(str(STAGED_PATH / "time_recording"),str(STAGED_PATH / "manufacturing"),combine_work_interval_tables)

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

    #truncated_tables >> processed_dim
    combined_sales >> processed_dim
    combined_orders >> processed_dim

    processed_dim >> processed_orders

    processed_orders >> processed_workphases
    combined_orders >> processed_workphases

    processed_workphases >> processed_machine_events
    combined_work_intervals >> processed_machine_events

    
dag_instance = erp_file_processor()
