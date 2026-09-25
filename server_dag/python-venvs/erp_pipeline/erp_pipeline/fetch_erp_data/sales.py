from __future__ import annotations
import requests
from typing import Optional
from erp_pipeline.helpers.file_helpers import save_parquet
from erp_pipeline.file_cleaner import clean_folder
from erp_pipeline.config.config import SALES_URL, AUTHVALUE,CA_BUNDLE_PATH
from erp_pipeline.config.path_config import SALES_DIR

def fetch_sales(token, sales_url: Optional[str] = None, sales_dir: Optional[str] = None):
    sales_url = sales_url or SALES_URL
    sales_dir = sales_dir or SALES_DIR
    authHeader = {
        AUTHVALUE : token,
        "Accept" : "application/json"
        }
    endpoints = [
        "/Customers",
        "/CustomerOrderRows",
        "/CustomerOrders",
        "/OrderQuantityCustomerOrders",
        "/CustomerOrderTypes",    
        "/InvoiceLogs",
        "/CustomerOrderInvoiceRows",
    ]
    query = "?$orderby=Id desc"

    def get_sales_data(endpoint):
        url = f"{sales_url}{endpoint}{query}"
        response = requests.get(url, headers=authHeader, verify=False)
        if response.status_code != 200:
            print(f"[{endpoint}] Request failed with {response.status_code}")
            response.raise_for_status()
        return response.json() 
    results = {}
    for ep in endpoints:
        data = get_sales_data(ep)
        save_parquet(ep,data,base_path=sales_dir)
        clean_folder(ep, days_to_keep = 2, base_path=sales_dir)
        results[ep] = data
    return results



    
    