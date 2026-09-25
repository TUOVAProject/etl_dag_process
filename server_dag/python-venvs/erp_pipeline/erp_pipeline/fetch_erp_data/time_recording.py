from __future__ import annotations
import requests
from typing import Optional
from erp_pipeline.helpers.file_helpers import save_parquet
from erp_pipeline.file_cleaner import clean_folder
from erp_pipeline.config.config import TIMERECORDING_URL, AUTHVALUE,CA_BUNDLE_PATH
from erp_pipeline.config.path_config import TIME_RECORDING_DIR

def fetch_time_recording(token, time_recording_url: Optional[str] = None, time_recording_dir: Optional[str] = None):
    time_recording_url = time_recording_url or TIMERECORDING_URL
    time_recording_dir = time_recording_dir or TIME_RECORDING_DIR
    authHeader = {
        AUTHVALUE : token,
        "Accept" : "application/json"
        }
    endpoints = [
        "/WorkIntervals",
        "/IndirectWorkCodes"
    ]
    query = "?$orderby=Id desc"

    def get_time_recording_data(endpoint):
        url = f"{time_recording_url}{endpoint}{query}"
        response = requests.get(url, headers=authHeader, verify=False)
        if response.status_code != 200:
            print(f"[{endpoint}] Request failed with {response.status_code}")
            response.raise_for_status()
        return response.json() 
    
        
    results = {}
    for ep in endpoints:
        data = get_time_recording_data(ep)
        save_parquet(ep,data,base_path=time_recording_dir)
        clean_folder(ep, days_to_keep = 2, base_path=time_recording_dir)
        results[ep] = data
    return results



    
    