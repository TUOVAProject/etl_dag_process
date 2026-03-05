import json
import os
from datetime import datetime

def endpoint_to_filename(endpoint: str) -> str:
    name = endpoint.lstrip("/")

    name = name.replace("/", "-")

    if not name:
        name = "root"
    
    return f"{name}.json"


def save_json(endpoint: str, data: dict, base_path="results"):
    endpoint_name = endpoint.lstrip("/").replace("/", "-")
    if not endpoint_name:
        endpoint_name = "root"

    date_str = datetime.now().strftime("%Y-%m-%d")

    folder_path = os.path.join(base_path, endpoint_name, date_str)
    os.makedirs(folder_path, exist_ok=True)

    filename = f"{endpoint_name}.json"
    full_path = os.path.join(folder_path, filename)

    with open(full_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

    print(f"Saved: {full_path}")
