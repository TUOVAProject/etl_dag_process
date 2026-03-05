import os
import re
import json
from datetime import datetime
from pathlib import Path
import pandas as pd
from typing import List, Tuple, Optional

INPUT_DIRS = [
    "results/raw/inventory",
    "results/raw/common",
    "results/raw/manufacturing",
    "results/raw/sales",
    "results/raw/accounting",
]

OUTPUT_FILE = "/home/sasu/TUOVA/data_set_05_02_2026.xlsx"

OUTPUT_DIR = "/home/sasu/TUOVA/exports/csv_per_endpoint"
RECURSIVE_WITHIN_DATE_FOLDER = False

CSV_ENCODING = "utf-8-sig"
CSV_INDEX = False
CSV_SEPARATOR = "," 

DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
DATE_FORMAT = "%Y-%m-%d"

def parse_date_folder_name(name: str):
    """Palauttaa datetime-olion jos nimi on dd-mm-yyyy, muuten None."""
    if not DATE_PATTERN.match(name):
        return None
    try:
        return datetime.strptime(name, DATE_FORMAT)
    except ValueError:
        return None


def find_latest_date_subfolder_under_categories(root: Path):
    """
    Etsii jokaisen endpointCategory-kansion alta uusimman yyyy-mm-dd päiväkansion.
    Palauttaa listan poluista.
    """
    results = []

    for category in root.iterdir():
        if not category.is_dir():
            continue

        dated_subdirs = []
        for d in category.iterdir():
            if d.is_dir():
                dt = parse_date_folder_name(d.name)
                if dt:
                    dated_subdirs.append((dt, d))

        if not dated_subdirs:
            continue
        
        dated_subdirs.sort(key=lambda x: x[0])
        latest = dated_subdirs[-1][1]

        results.append(latest)

    return results


def safe_sheet_name(name: str) -> str:
    cleaned = re.sub(r'[:\\/?*\[\]]', "_", name)
    return cleaned[:31] if len(cleaned) > 31 else cleaned

def json_to_dataframe(json_obj):
    
    if isinstance(json_obj, list):
        if len(json_obj) == 0:
            return pd.DataFrame()
        if all(isinstance(x, dict) for x in json_obj):
            return pd.DataFrame(json_obj)
        return pd.DataFrame({"value": json_obj})
    elif isinstance(json_obj, dict):
        return pd.DataFrame([json_obj])
    else:
        return pd.DataFrame([{"value": json_obj}])

def dataset_to_excel():
    output_path = Path(OUTPUT_FILE)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        sheets_written = 0

        
        for dir_str in INPUT_DIRS:
            root = Path(dir_str)
            if not root.is_dir():
                print(f"Skipping missing folder: {root}")
                continue

            latest_date_folders = find_latest_date_subfolder_under_categories(root)

            for latest in latest_date_folders:
                print(f"Using latest folder: {latest}")

                json_files = sorted(latest.glob("*.json"))

                for json_path in json_files:

                    try:
                        with json_path.open("r", encoding="utf-8") as f:
                            data = json.load(f)
                    except json.JSONDecodeError as e:
                        print(f"Skipping invalid JSON ({json_path}): {e}")
                        continue
                    except Exception as e:
                        print(f"Skipping file due to error ({json_path}): {e}")
                        continue

                    df = json_to_dataframe(data)

                    base_prefix = root.name
                    base_file = json_path.stem
                    sheet_name = safe_sheet_name(f"{base_prefix}_{base_file}")

                    if df.empty:
                        print(f"Empty dataframe from {json_path}, writing headers only.")
                        df = pd.DataFrame()

                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    sheets_written += 1

            print(f"Saved Excel file: {output_path} (sheets: {sheets_written})")


def safe_filename(name: str, max_len: int = 128) -> str:

    name = re.sub(r'[<>:"/\\|?*]', "_", name)
    name = name.replace(" ", "_")
    name = re.sub(r"_+", "_", name).strip("_")
    return name[:max_len]

def latest_date_folder_per_category(domain_root: Path) -> List[Tuple[Path, Path]]:
    
    results: List[Tuple[Path, Path]] = []
    if not domain_root.is_dir():
        return results

    for category in sorted(p for p in domain_root.iterdir() if p.is_dir()):
        dated: List[Tuple[datetime, Path]] = []
        for maybe_date in category.iterdir():
            if maybe_date.is_dir():
                dt = parse_date_folder_name(maybe_date.name)
                if dt:
                    dated.append((dt, maybe_date))
        if not dated:
            continue
        dated.sort(key=lambda t: t[0])
        results.append((category, dated[-1][1]))  # uusin
    return results

def enumerate_json_files(folder: Path, recursive: bool = False) -> List[Path]:
    if recursive:
        return sorted(folder.rglob("*.json"))
    return sorted(p for p in folder.iterdir() if p.is_file() and p.suffix.lower() == ".json")



def export_endpoint_to_csv():
    out_dir = Path(OUTPUT_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)

    files_written = 0

    for domain_str in INPUT_DIRS:
        domain_root = Path(domain_str)
        domain = domain_root.name
        if not domain_root.is_dir():
            print(f"Skipping missing domain: {domain_root}")
            continue

        pairs = latest_date_folder_per_category(domain_root)
        if not pairs:
            print(f"No categories with date folders under: {domain_root}")
            continue

        for category_path, latest_date_path in pairs:
            category = category_path.name  
            date_str = latest_date_path.name 
            print(f"[{domain}/{category}] using latest date: {date_str}")

            json_files = enumerate_json_files(latest_date_path, recursive=RECURSIVE_WITHIN_DATE_FOLDER)
            if not json_files:
                print(f"  No JSON files found in: {latest_date_path}")
                continue

            for json_path in json_files:
                endpoint = json_path.stem 
                csv_name = f"{safe_filename(domain)}_{safe_filename(category)}_{safe_filename(endpoint)}.csv"
                csv_path = out_dir / csv_name

                try:
                    with json_path.open("r", encoding="utf-8") as f:
                        data = json.load(f)
                except json.JSONDecodeError as e:
                    print(f"  Skipping invalid JSON ({json_path}): {e}")
                    continue
                except Exception as e:
                    print(f"  Skipping due to read error ({json_path}): {e}")
                    continue

                df = json_to_dataframe(data)

                if df is None or (isinstance(df, pd.DataFrame) and df.empty):
                    print(f"  Empty/No rows in {json_path}, writing empty CSV with header.")
                    df = df if isinstance(df, pd.DataFrame) else pd.DataFrame()
                    if df.empty:
                        df = pd.DataFrame({"info": []})

                try:
                    df.to_csv(csv_path, sep=CSV_SEPARATOR, index=CSV_INDEX, encoding=CSV_ENCODING)
                    files_written += 1
                    print(f"  Wrote: {csv_path}")
                except Exception as e:
                    print(f"  Failed to write CSV ({csv_path}): {e}")

    print(f"\nDone. CSV files written: {files_written}. Output dir: {Path(OUTPUT_DIR).resolve()}")



