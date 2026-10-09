from pathlib import Path

DATA_DIR = Path("/opt/airflow/data/results")
RAW_DIR = DATA_DIR / "raw"

INVENTORY_DIR = str(RAW_DIR /"inventory")
COMMON_DIR = str(RAW_DIR /"common")
MANUFACTURING_DIR = str(RAW_DIR /"manufacturing")
SALES_DIR = str(RAW_DIR /"sales")
TIME_RECORDING_DIR = str(RAW_DIR /"time_recording")


##LOCAL TESTING

LOCAL_BASE_PATH = "results/raw"
LOCAL_STAGED_PATH = Path("results/staged")
LOCAL_COMBINED_PATH = Path("results/combined")

RAW_PATH = Path("/opt/airflow/data/results/raw")
STAGED_PATH = Path("/opt/airflow/data/results/staged")
COMBINED_PATH = Path("/opt/airflow/data/results/combined")