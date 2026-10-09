from __future__ import annotations
import requests
from pathlib import Path
from typing import Optional
from erp_pipeline.helpers.file_helpers import save_parquet
from erp_pipeline.file_cleaner import clean_folder
from erp_pipeline.config.config import COMMON_URL, AUTHVALUE
from erp_pipeline.config.path_config import COMMON_DIR


def fetch_common(token, common_url: Optional[str] = None, common_dir: Optional[str] = None):
    common_url = common_url or COMMON_URL
    common_dir = common_dir or COMMON_DIR
    authHeader = {
        AUTHVALUE : token,
        "Accept" : "application/json"
        }
    endpoints = [
        "/Currencies",
        "/Units",
        "/ExtraFields",
        "/ExtraFieldTemplates",
        "/ExtraFieldOptionTemplates",
        "/ExtraFieldGroups",
    ]
    query = "?$orderby=Id desc"

    def get_common_data(endpoint):
        url = f"{common_url}{endpoint}{query}"
        response = requests.get(url, headers=authHeader, verify=False,timeout=30)
        if response.status_code != 200:
            print(f"[{endpoint}] Request failed with {response.status_code}")
            response.raise_for_status()
        return response.json()
        
    results = {}
    for ep in endpoints:
        data = get_common_data(ep)
        save_parquet(ep,data,base_path=common_dir)
        clean_folder(ep, days_to_keep = 2, base_path=common_dir)
        results[ep] = data
    return results



    
    