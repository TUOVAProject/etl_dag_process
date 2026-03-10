"""from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from auth.auth import _login_and_get_token
from fetchers.common import fetch_common
from fetchers.inventory import fetch_inventory
from fetchers.manufacturing import fetch_manufacturing
from fetchers.sales import fetch_sales
from erp_file_processor import erp_file_processor

def monitor_erp_airflow_trigger():
    print("Fetching from /Commons endpoint")
    #fetch_common()
    print("Fetching from /Inventory endpoint")
    #fetch_inventory()
    print("Fetching from /Manufacturing endpoint")
    #fetch_manufacturing()
    print("Fetching from /Sales endpoint")
    #fetch_sales()
    erp_file_processor()


with DAG(
    dag_id="monitor_erp_airflow_trigger",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["erp", "monitor"],
) as dag:

    fetch_task = PythonOperator(
        task_id="monitor_erp_airflow_trigger",
        python_callable=monitor_erp_airflow_trigger,
    )"""

