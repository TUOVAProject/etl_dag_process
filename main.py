#from src.fetchers.common import fetch_common
#from src.fetchers.inventory import fetch_inventory
#from src.fetchers.manufacturing import fetch_manufacturing
#from src.fetchers.sales import fetch_sales
##from src.auth.auth import _login_and_get_token
from src.dag_test import erp_file_processor_test
from src.dataset_to_excel import dataset_to_excel,export_endpoint_to_csv,combined_dataset_to_excel,combined_export_endpoint_to_csv
#from dags.erp_file_processor import erp_file_processor_test
from src.config.logger_config import get_custom_logger


def main():
    #print("Fetching from /Commons endpoint")
    #fetch_common()
    #print("Fetching from /Inventory endpoint")
    #fetch_inventory()
    #print("Fetching from /Manufacturing endpoint")
    #fetch_manufacturing()
    #print("Fetching from /Sales endpoint")
    #fetch_sales()
    #erp_file_processor()
    get_custom_logger()
    erp_file_processor_test()
    #dataset_to_excel()
    #export_endpoint_to_csv()
    #combined_export_endpoint_to_csv()
    combined_dataset_to_excel()

if __name__ == "__main__":
    main()