import requests
from helpers import save_json,save_parquet,clean_folder
from config.config import MANUFACTURING_URL, AUTHVALUE
from auth.auth import get_token, force_refresh_token
import logging
logger = logging.getLogger("custom_logger")

def fetch_manufacturing():
    token = get_token()
    authHeader = {
        AUTHVALUE : token,
        "Accept" : "application/json"
        }
    endpoints = [
        "/Drawings",
        #"/MaintenanceReportingRows",
        #"/MaintenanceReportings",
        #"/MaintenanceReportingSelectionRows",
        "/ManualOperationStatuses",
        "/ManufacturingOrderMaterials",
        "/ManufacturingOrderNodeDrawings",
        "/ManufacturingOrderNodes",
        "/ManufacturingOrderOperationControlDataRows",
        "/ManufacturingOrderOperationReportings",
        "/ManufacturingOrderOperations",
        "/ManufacturingOrders",
        "/ManufacturingOrderTypes",
        "/MaterialRows",
        "/OperationRowControlDataRows",
        "/OperationRows",
        "/OrderQuantityManufacturingOrderOperations",
        "/OrderQuantityManufacturingOrders",
        "/Preparations",
        "/ProductionCalendarExceptions",
        "/ReasonCodeTimeConsumptions",
        "/WorkCenterCostFactorGroups",
        "/WorkCenters",
        
    ]
    query = "?orderby=Id desc"

    def get_manufacturing_data(endpoint):
        url = f"{MANUFACTURING_URL}/{endpoint}{query}"
        max_retries = 2
        for attempt in range(max_retries + 1):
            response = requests.get(url, headers=authHeader,verify=False)

            if response.status_code != 200:
                logger.info(f"[{endpoint}] Token expired → refreshing ({attempt+1}/{max_retries})")
                token = force_refresh_token()
                authHeader[AUTHVALUE] = token
                continue

            try:
                response.raise_for_status()
                return response.json()
            except Exception as err:
                print(f"[{endpoint}] Request failed: {err} (attempt {attempt+1}/{max_retries})")
                if attempt == max_retries:
                    raise

        raise RuntimeError("Unexpected exit from retry loop")
        
    results = {}
    for ep in endpoints:
        data = get_manufacturing_data(ep)
        save_parquet(ep,data,base_path="results/raw/manufacturing")
        results[ep] = data
    clean_folder(ep, days_to_keep = 2, base_path="results/raw/manufacturing")
    return results



    
    