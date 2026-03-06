from src.fetchers.common import fetch_common
from src.fetchers.inventory import fetch_inventory
from src.fetchers.manufacturing import fetch_manufacturing
from src.fetchers.sales import fetch_sales
from src.auth.auth import _login_and_get_token
from src.dag_test import erp_file_processor
from src.dataset_to_excel import dataset_to_excel,export_endpoint_to_csv

import pandas as pd
import sqlalchemy as sa


def main():
    #print("Fetching from /Commons endpoint")
    #fetch_common()
    #print("Fetching from /Inventory endpoint")
    #fetch_inventory()
    #print("Fetching from /Manufacturing endpoint")
    #fetch_manufacturing()
    #print("Fetching from /Sales endpoint")
    #fetch_sales()
    erp_file_processor()
    #export_endpoint_to_csv()
    

if __name__ == "__main__":
    main()