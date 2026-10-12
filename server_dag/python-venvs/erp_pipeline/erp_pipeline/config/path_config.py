from pathlib import Path

BASE_PATH = Path("/opt/airflow/data/results")

RAW_PATH = BASE_PATH / "raw"
STAGED_PATH = BASE_PATH / "staged"
COMBINED_PATH = BASE_PATH / "combined"

INVENTORY_DIR = str(RAW_PATH / "inventory")
COMMON_DIR = str(RAW_PATH / "common")
MANUFACTURING_DIR = str(RAW_PATH / "manufacturing")
SALES_DIR = str(RAW_PATH / "sales")
TIME_RECORDING_DIR = str(RAW_PATH / "time_recording")


##LOCAL TESTING

LOCAL_RAW_PATH_STR = "results/raw"
LOCAL_RAW_PATH = Path("results/raw")
LOCAL_STAGED_PATH = Path("results/staged")
LOCAL_COMBINED_PATH = Path("results/combined")
