import os
import re
import json
from datetime import datetime
from pathlib import Path
import pandas as pd
from typing import List, Tuple, Optional


LOCAL_BASE_PATH = Path("results")
LOCAL_RAW_PATH = LOCAL_BASE_PATH / "raw"
LOCAL_COMBINED_PATH = LOCAL_BASE_PATH / "combined"

INPUT_DIRS = [
    LOCAL_RAW_PATH / "inventory",
    LOCAL_RAW_PATH / "common",
    LOCAL_RAW_PATH / "manufacturing",
    LOCAL_RAW_PATH / "sales",
    LOCAL_RAW_PATH / "accounting",
    LOCAL_RAW_PATH / "time_recording",
]

COMBINED_DIRS = [
    LOCAL_COMBINED_PATH / "inventory",
    LOCAL_COMBINED_PATH / "reportings",
    LOCAL_COMBINED_PATH / "manufacturing",
    LOCAL_COMBINED_PATH / "sales",
    LOCAL_COMBINED_PATH / "materials",
    LOCAL_COMBINED_PATH / "materials_raw",
    LOCAL_COMBINED_PATH / "time_recording",
]

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



def find_latest_parquet_file_in_category(category_dir: Path):
    """
    category_dir = results/combined/<endpoint>
    Palauttaa uusimman päivän yyyy-mm-dd-*.parquet -tiedostot
    """
    dated_files = []

    for f in category_dir.glob("*.parquet"):
        dt = parse_date_from_filename(f.name)
        if dt:
            dated_files.append((dt, f))

    if not dated_files:
        print(f"No dated parquet files in: {category_dir}")
        return []

    dated_files.sort(key=lambda x: x[0])
    latest_date = dated_files[-1][0]

    return [f for dt, f in dated_files if dt == latest_date]


def safe_sheet_name(name: str) -> str:
    cleaned = re.sub(r'[:\\/?*\[\]]', "_", name)
    return cleaned[:31] if len(cleaned) > 31 else cleaned


_ILLEGAL_EXCEL_CHARS_RE = re.compile(r"[\x00-\x08\x0B-\x0C\x0E-\x1F\x7F]")

def _clean_excel_string(s: str) -> str:
    if s is None:
        return s
    # Ensure string type
    if not isinstance(s, str):
        try:
            # Convert lists/dicts to JSON so they are readable in Excel
            if isinstance(s, (list, dict)):
                s = json.dumps(s, ensure_ascii=False)
            else:
                s = str(s)
        except Exception:
            s = str(s)
    # Remove illegal characters
    return _ILLEGAL_EXCEL_CHARS_RE.sub("", s)

def clean_dataframe_for_excel(df: pd.DataFrame) -> pd.DataFrame:
   
    if df is None or df.empty:
        return df

    df_clean = df.copy()

    obj_cols = df_clean.select_dtypes(include=["object", "string"]).columns
    for col in obj_cols:
        df_clean[col] = df_clean[col].map(
            lambda v: _clean_excel_string(v) if pd.notna(v) else v
        )

    return df_clean


def parquet_to_dataframe(obj):  
    path = Path(obj)
    if not path.exists():
        raise FileNotFoundError(f"Parquet file not found: {path}")

    return pd.read_parquet(path)


def parse_date_from_filename(name: str):
    """
    Parsii päivämäärän tiedoston nimestä (yyyy-mm-dd-*)
    Palauttaa datetime.date tai None
    """
    try:
        date_part = name.split("-", 3)[0:3]
        return datetime.strptime("-".join(date_part), "%Y-%m-%d").date()
    except Exception:
        return None


def dataset_to_excel():
    date_str = datetime.now().strftime("%Y-%m-%d")
    OUTPUT_FILE = f"/home/sasu/TUOVA/data_set_{date_str}.xlsx"
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

                parquet_files = sorted(latest.glob("*.parquet"))

                for parquet_path in parquet_files:

                    try:
                        df = parquet_to_dataframe(parquet_path)
                    except Exception as e:
                        print(f"Skipping invalid parquet ({parquet_path}): {e}")
                        continue

                    base_file = parquet_path.stem
                    sheet_name = safe_sheet_name(f"{base_file}")

                    if df.empty:
                        print(f"Empty dataframe from {parquet_path}, writing headers only.")
                        df = pd.DataFrame()
                    save_df = clean_dataframe_for_excel(df)
                    save_df.to_excel(writer, sheet_name=sheet_name, index=False)
                    sheets_written += 1

            print(f"Saving Excel file: {output_path} (sheets: {sheets_written})")


def combined_dataset_to_excel():
    date_str = datetime.now().strftime("%Y-%m-%d")
    OUTPUT_FILE = f"/home/sasu/TUOVA/combined_data_set_{date_str}.xlsx"
    output_path = Path(OUTPUT_FILE)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        sheets_written = 0

        for dir_str in COMBINED_DIRS:
            root = Path(dir_str)
            if not root.is_dir():
                print(f"Skipping missing folder: {root}")
                continue

            parquet_files = find_latest_parquet_file_in_category(root)

            for parquet_path in parquet_files:
                print(f"Using parquet: {parquet_path}")

                try:
                    df = parquet_to_dataframe(parquet_path)
                except Exception as e:
                    print(f"Skipping invalid parquet ({parquet_path}): {e}")
                    continue

                base_file = parquet_path.stem
                sheet_name = safe_sheet_name(base_file)

                if df.empty:
                    print(f"Empty dataframe from {parquet_path}, writing headers only.")
                    df = pd.DataFrame()

                save_df = clean_dataframe_for_excel(df)
                print(sheet_name)
                save_df.to_excel(writer, sheet_name=sheet_name, index=False)
                sheets_written += 1

        print(f"Saving Excel file: {output_path} (sheets: {sheets_written})")


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

def enumerate_parquet_files(folder: Path, recursive: bool = False) -> List[Path]:
    if recursive:
        return sorted(folder.rglob("*.parquet"))
    return sorted(p for p in folder.iterdir() if p.is_file() and p.suffix.lower() == ".parquet")


def combined_export_endpoint_to_csv():
    date_str = datetime.now().strftime("%Y-%m-%d")
    OUTPUT_DIR = f"/home/sasu/TUOVA/exports/csv_combined_dataset_{date_str}"
    out_dir = Path(OUTPUT_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)

    files_written = 0

    for domain_str in COMBINED_DIRS:
        domain_root = Path(domain_str)
        domain = domain_root.name

        if not domain_root.is_dir():
            print(f"Skipping missing domain: {domain_root}")
            continue

        triples = find_latest_parquet_file_in_category(domain_root)
        if not triples:
            print(f"No parquet files found under: {domain_root}")
            continue

        for category, parquet_path, latest_date in triples:
            print(f"[{domain}/{category}] using latest date: {latest_date}")

            endpoint = parquet_path.stem
            csv_name = f"{safe_filename(endpoint)}.csv"
            csv_path = out_dir / csv_name

            try:
                df = parquet_to_dataframe(parquet_path)
            except Exception as e:
                print(f"  Skipping invalid Parquet ({parquet_path}): {e}")
                continue

            if df is None or df.empty:
                print(f"  Empty dataframe in {parquet_path}, creating empty CSV with header.")
                df = pd.DataFrame({"info": []})

            try:
                df.to_csv(
                    csv_path,
                    sep=CSV_SEPARATOR,
                    index=CSV_INDEX,
                    encoding=CSV_ENCODING
                )
                files_written += 1
                print(f"  Wrote: {csv_path}")
            except Exception as e:
                print(f"  Failed to write CSV ({csv_path}): {e}")

    print(f"CSV export finished, files written: {files_written}")


def export_endpoint_to_csv():
    date_str = datetime.now().strftime("%Y-%m-%d")
    OUTPUT_DIR = f"/home/sasu/TUOVA/exports/csv_dataset_{date_str}"
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

            parquet_files = enumerate_parquet_files(latest_date_path, recursive=RECURSIVE_WITHIN_DATE_FOLDER)
            if not parquet_files:
                print(f"  No parquet files found in: {latest_date_path}")
                continue

            
            for parquet_path in parquet_files:
                endpoint = parquet_path.stem
                #csv_name = f"{safe_filename(domain)}_{safe_filename(category)}_{safe_filename(endpoint)}.csv"
                csv_name = f"{safe_filename(endpoint)}.csv"
                csv_path = out_dir / csv_name

                try:
                    df = parquet_to_dataframe(parquet_path)
                except Exception as e:
                    print(f"  Skipping invalid Parquet ({parquet_path}): {e}")
                    continue

                if df is None or df.empty:
                    print(f"  Empty dataframe in {parquet_path}, creating an empty CSV with header.")
                    df = pd.DataFrame({"info": []})

                try:
                    df.to_csv(csv_path, sep=CSV_SEPARATOR, index=CSV_INDEX, encoding=CSV_ENCODING)
                    files_written += 1
                    print(f"  Wrote: {csv_path}")
                except Exception as e:
                    print(f"  Failed to write CSV ({csv_path}): {e}")


    print(f"\nDone. CSV files written: {files_written}. Output dir: {Path(OUTPUT_DIR).resolve()}")



