from __future__ import annotations
import requests
from typing import Optional
from erp_pipeline.helpers.file_helpers import save_parquet
from erp_pipeline.file_cleaner import clean_folder
from erp_pipeline.config.config import MANUFACTURING_URL, AUTHVALUE,CA_BUNDLE_PATH
from erp_pipeline.config.path_config import MANUFACTURING_DIR


def fetch_manufacturing(token, manufacturing_url: Optional[str] = None, manufacturing_dir: Optional[str] = None):
    manufacturing_url = manufacturing_url or MANUFACTURING_URL
    manufacturing_dir = manufacturing_dir or MANUFACTURING_DIR
    authHeader = {
        AUTHVALUE : token,
        "Accept" : "application/json"
        }
    endpoints = [
        "/ManufacturingOrderMaterials",
        "/ManufacturingOrderOperationReportings",
        "/ManufacturingOrderOperations",
        "/ManufacturingOrders",
        "/ManufacturingOrderTypes",
        "/OperationRows",
        "/WorkCenters",
        
    ]
    query = "?orderby=Id desc"

    
    def get_manufacturing_data(endpoint):
            url = f"{manufacturing_url}{endpoint}{query}"
            response = requests.get(url, headers=authHeader,verify=False)
            if response.status_code != 200:
                print(f"[{endpoint}] Request failed with {response.status_code}")
                response.raise_for_status()
            return response.json() 
        
    results = {}
    for ep in endpoints:
        data = get_manufacturing_data(ep)
        save_parquet(ep,data,base_path=manufacturing_dir)
        clean_folder(ep, days_to_keep = 2, base_path=manufacturing_dir)
        results[ep] = data
    return results



    
    