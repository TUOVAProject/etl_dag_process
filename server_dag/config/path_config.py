from pathlib import Path

DATA_DIR = Path("/opt/airflow/data/results")
RAW_DIR = DATA_DIR / "raw"

INVENTORY_DIR = str(RAW_DIR /"inventory")
COMMON_DIR = str(RAW_DIR /"common")
MANUFACTURING_DIR = str(RAW_DIR /"manufacturing")
SALES_DIR = str(RAW_DIR /"sales")
TIME_RECORDING_DIR = str(RAW_DIR /"time_recording")
