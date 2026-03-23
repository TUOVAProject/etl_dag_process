import json
import os
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path
import shutil
from typing import Union
import logging

logger = logging.getLogger("custom_logger")

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

#Jsonin tallennus testiympäristössä
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

    logger.info(f"Saved: {full_path}")

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

    logger.info(f"Saved: {full_path}")




def clean_folder(endpoint: str, days_to_keep: int, base_path: Union[str, Path] = "results", dry_run: bool = False) -> None:
    """
    Keep only the latest `days_to_keep` date folders under results/<endpoint_name>/.

    - A "date folder" is one named exactly YYYY-MM-DD.
    - Non-date-named subfolders are ignored (not deleted).
    - If there are <= days_to_keep date folders, nothing is deleted.
    - Set dry_run=True to logger.info what would be deleted without removing anything.
    """
    # Normalize names/paths
    endpoint_name = endpoint.lstrip("/").replace("/", "-") or "root"
    endpoint_folder = Path(base_path) / endpoint_name

    if days_to_keep < 0:
        raise ValueError("days_to_keep must be >= 0")

    if not endpoint_folder.exists():
        logger.info(f"[clean_folder] Skipping: {endpoint_folder} does not exist.")
        return

    # Collect date-named folders
    date_dirs: list[tuple[datetime, Path]] = []
    for child in endpoint_folder.iterdir():
        if not child.is_dir():
            continue
        try:
            dt = datetime.strptime(child.name, "%Y-%m-%d")
        except ValueError:
            # Not a date folder → ignore
            continue
        date_dirs.append((dt, child))

    if not date_dirs:
        logger.info(f"[clean_folder] No date folders under {endpoint_folder}. Nothing to do.")
        return

    date_dirs.sort(key=lambda x: x[0], reverse=True)

    keep = date_dirs[:days_to_keep] if days_to_keep > 0 else []
    delete = date_dirs[days_to_keep:] if days_to_keep >= 0 else []

    keep_names = [p.name for _, p in keep]
    del_names = [p.name for _, p in delete]

    logger.info(f"[clean_folder] Folder: {endpoint_folder}")
    logger.info(f"[clean_folder] Keeping ({len(keep_names)}): {keep_names}")
    logger.info(f"[clean_folder] Deleting ({len(del_names)}): {del_names}")

    for _, folder in delete:
        if dry_run:
            logger.info(f"[clean_folder] (dry-run) Would delete: {folder}")
        else:
            try:
                shutil.rmtree(folder)
                logger.info(f"[clean_folder] Deleted: {folder}")
            except Exception as e:
                logger.info(f"[clean_folder] Failed to delete {folder}: {e}")



