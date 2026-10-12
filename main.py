#from src.fetchers.common import fetch_common
#from src.fetchers.inventory import fetch_inventory
#from src.fetchers.manufacturing import fetch_manufacturing
#from src.fetchers.sales import fetch_sales
##from src.auth.auth import _login_and_get_token
from src.dag_test import erp_file_processor_test
from src.dataset_to_excel import dataset_to_excel,export_endpoint_to_csv,combined_dataset_to_excel,combined_export_endpoint_to_csv
#from dags.erp_file_processor import erp_file_processor_test
from src.config.logger_config import get_custom_logger
from time import perf_counter


def measure_call(name, function):
    start = perf_counter()
    result = function()
    duration = perf_counter() - start
    print(f"{name}: {duration:.2f} s")
    return result

def main():
    get_custom_logger()
    measure_call(
    "ERP file processor",
    erp_file_processor_test,
    )
    measure_call(
        "Dataset to Excel",
        dataset_to_excel,
    )
    measure_call(
        "Combined dataset to Excel",
        combined_dataset_to_excel,
    )


if __name__ == "__main__":
    main()