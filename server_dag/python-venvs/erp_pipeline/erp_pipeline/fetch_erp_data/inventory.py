from __future__ import annotations
import requests
from typing import Optional
from erp_pipeline.helpers.file_helpers import save_parquet
from erp_pipeline.file_cleaner import clean_folder
from erp_pipeline.config.config import INVENTORY_URL, AUTHVALUE,CA_BUNDLE_PATH
from erp_pipeline.config.path_config import INVENTORY_DIR


def fetch_inventory(token, inventory_url: Optional[str] = None, inventory_dir: Optional[str] = None):
    inventory_url = inventory_url or INVENTORY_URL
    inventory_dir = inventory_dir or INVENTORY_DIR
    authHeader = {
        AUTHVALUE : token,
        "Accept" : "application/json"
        }
    endpoints = [
        "/Parts",
        "/PartCodes",
    ]
    query = "?$orderby=Id desc"

    def get_inventory_data(endpoint):
        url = f"{inventory_url}{endpoint}{query}"
        response = requests.get(url, headers=authHeader, verify=False)
        if response.status_code != 200:
            print(f"[{endpoint}] Request failed with {response.status_code}")
            response.raise_for_status()
        return response.json()  
    
    results = {}
    for ep in endpoints:
        data = get_inventory_data(ep)
        save_parquet(ep,data,base_path=inventory_dir)
        clean_folder(ep, days_to_keep = 2, base_path=inventory_dir)
        results[ep] = data
    return results



    
    