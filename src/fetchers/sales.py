import requests
from src.helpers import save_json
from src.config.config import SALES_URL, AUTHVALUE
from src.auth.auth import get_token, force_refresh_token

def fetch_sales():
    token = get_token()
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
        "/SalesPrices", 
        "/InvoiceLogs"
    ]
    query = "?$top=3000&$orderby=Id desc"

    def get_sales_data(endpoint):
        url = f"{SALES_URL}/{endpoint}{query}"
        max_retries = 2
        for attempt in range(max_retries + 1):
            response = requests.get(url, headers=authHeader,verify=False)

            if response.status_code != 200:
                print(f"[{endpoint}] Token expired → refreshing ({attempt+1}/{max_retries})")
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
        data = get_sales_data(ep)
        save_json(ep,data,base_path="results/raw/sales")
        results[ep] = data
    return results



    
    