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
    from erp_pipeline.config.path_config import RAW_PATH
    raw = get_latest_endpoint_files(endpoint,RAW_PATH)
    return {ep: [str(p) for p in paths] for ep, paths in raw.items()}

@task.external_python(python=PYTHON_VENV_PATH, task_id="process_erp_files", trigger_rule=TriggerRule.ALL_SUCCESS)
def process_erp_files_task(my_files,env_path=None):
    from pathlib import Path
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=env_path)
    from erp_pipeline.process.process_erp import process_erp_files
    from erp_pipeline.config.path_config import STAGED_PATH
    print("Processing files")

    for _, files in my_files.items():
        for file_path in files:
            print(file_path)
            process_erp_files(Path(file_path), STAGED_PATH)

@task.external_python(python=PYTHON_VENV_PATH, task_id="combine_sales_data",trigger_rule=TriggerRule.ALL_SUCCESS)
def combine_sales_data_task(env_path=None):
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=env_path)
    from erp_pipeline.config.erp_config import combine_sales_tables
    from erp_pipeline.combine_data.combine_data_files import combine_sales_data
    from erp_pipeline.config.path_config import COMBINED_PATH,STAGED_PATH

    return combine_sales_data(STAGED_PATH / "sales",STAGED_PATH / "manufacturing",combine_sales_tables,COMBINED_PATH)


@task.external_python(python=PYTHON_VENV_PATH, task_id="combine_material_data",trigger_rule=TriggerRule.ALL_SUCCESS)
def combine_material_data_task(combined_sales_path:str,staged_path: str, env_path=None):
    from pathlib import Path
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=env_path)
    from erp_pipeline.config.erp_config import combine_material_tables
    from erp_pipeline.combine_data.combine_data_files import combine_material_data
    from erp_pipeline.config.path_config import COMBINED_PATH

    return combine_material_data(Path(combined_sales_path),Path(staged_path),combine_material_tables,COMBINED_PATH)


@task.external_python(python=PYTHON_VENV_PATH, task_id="combine_inventory_data",trigger_rule=TriggerRule.ALL_SUCCESS)
def combine_inventory_data_task(staged_path: str, env_path=None):
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=env_path)
    from erp_pipeline.config.erp_config import combine_inventory_tables
    from erp_pipeline.combine_data.combine_data_files import combine_inventory_data
    from erp_pipeline.config.path_config import COMBINED_PATH, STAGED_PATH

    return combine_inventory_data(STAGED_PATH / "inventory",combine_inventory_tables,COMBINED_PATH)


@task.external_python(python=PYTHON_VENV_PATH, task_id="combine_manufacturing_data",trigger_rule=TriggerRule.ALL_SUCCESS)
def combine_manufacturing_data_task(env_path=None):
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=env_path)
    from erp_pipeline.config.erp_config import combine_manufacturing_tables
    from erp_pipeline.combine_data.combine_data_files import combine_manufacturing_data
    from erp_pipeline.config.path_config import COMBINED_PATH,STAGED_PATH

    return combine_manufacturing_data(STAGED_PATH / "manufacturing",STAGED_PATH / "common",STAGED_PATH / "sales", combine_manufacturing_tables,COMBINED_PATH)


@task.external_python(python=PYTHON_VENV_PATH, task_id="combine_purchase_data",trigger_rule=TriggerRule.ALL_SUCCESS)
def combine_purchase_data_task(staged_path: str, env_path=None):
    from pathlib import Path
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=env_path)
    from erp_pipeline.config.erp_config import combine_purchase_tables
    from erp_pipeline.combine_data.combine_data_files import combine_purchase_data
    from erp_pipeline.config.path_config import COMBINED_PATH

    return combine_purchase_data(Path(staged_path),combine_purchase_tables,COMBINED_PATH)


@task.external_python(python=PYTHON_VENV_PATH, task_id="combine_work_interval_data",trigger_rule=TriggerRule.ALL_SUCCESS)
def combine_work_interval_data_task(env_path=None):
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=env_path)
    from erp_pipeline.config.erp_config import combine_work_interval_tables
    from erp_pipeline.combine_data.combine_data_files import combine_work_interval_data
    from erp_pipeline.config.path_config import COMBINED_PATH,STAGED_PATH

    return combine_work_interval_data(STAGED_PATH / "time_recording",STAGED_PATH / "manufacturing",combine_work_interval_tables,COMBINED_PATH)


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
    

    combined_orders = combine_manufacturing_data_task(ENV_PATH)
    combined_sales = combine_sales_data_task(ENV_PATH)
    combined_inventory = combine_inventory_data_task(ENV_PATH)
    combined_work_intervals = combine_work_interval_data_task(ENV_PATH)

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
