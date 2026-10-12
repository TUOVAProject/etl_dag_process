from __future__ import annotations
import os
from datetime import datetime
import pandas as pd
from pathlib import Path
from collections import defaultdict
from typing import Dict, List

DATE_FMT = "%Y-%m-%d"

def get_latest_endpoint_files(endpoint: str,rawpath: Path) -> Dict[str, List[Path]]:
    print(f"Loading latest endpoint files {rawpath}")
    dir = Path(rawpath) / endpoint
    if not dir.is_dir():
        return {}

    files_by_endpoint: Dict[str, List[Path]] = defaultdict(list)

    for endpoint_dir in dir.iterdir():
        if not endpoint_dir.is_dir():
            continue

        endpoint_name = endpoint_dir.name

        date_dirs = []
        for child in endpoint_dir.iterdir():
            if child.is_dir():
                d = _parse_date_dir(child.name)
                if d is not None:
                    date_dirs.append((d, child))

        if not date_dirs:
            continue

        _, latest_dir = max(date_dirs, key=lambda t: t[0])

        for file_path in latest_dir.iterdir():
            if file_path.is_file():
                files_by_endpoint[endpoint_name].append(file_path)

    return dict(files_by_endpoint)

def _parse_date_dir(name: str):
    """Palauttaa datetime-olion jos nimi on YYYY-MM-DD, muuten None."""
    try:
        return datetime.strptime(name, DATE_FMT).date()
    except ValueError:
        return None

def endpoint_to_filename(endpoint: str) -> str:
    name = endpoint.lstrip("/")

    name = name.replace("/", "-")

    if not name:
        name = "root"
    
    return f"{name}.json"

def endpoint_to_parquet_filename(endpoint: str) -> str:
    name = endpoint.lstrip("/")

    name = name.replace("/", "-")

    if not name:
        name = "root"
    
    return f"{name}.parquet"


#Tallennetaan parquet tuotannossa
def save_parquet(endpoint: str, data: dict, base_path="results"):
    endpoint_name = endpoint.lstrip("/").replace("/", "-")
    if not endpoint_name:
        endpoint_name = "root"

    date_str = datetime.now().strftime("%Y-%m-%d")

    folder_path = os.path.join(base_path, endpoint_name, date_str)
    os.makedirs(folder_path, exist_ok=True)

    filename = f"{endpoint_name}.parquet"
    full_path = os.path.join(folder_path, filename)

    df = pd.DataFrame(data)
    df.to_parquet(full_path,index=False)

    print(f"Saved: {full_path}")
