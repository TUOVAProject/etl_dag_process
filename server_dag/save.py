import pandas as pd
from pathlib import Path
import logging
from datetime import datetime

STAGED_PATH = Path("/opt/airflow/data/results/staged")
logger = logging.getLogger("custom_logger")

def save_combined_parquet(df: pd.DataFrame, date_str: str, outdir: Path, endpoint :str):
    outdir.mkdir(parents=True, exist_ok=True)
    file_name = f"Combined{endpoint.capitalize()}.parquet"
    output_file = outdir / f"{date_str}-{file_name}"
    df.to_parquet(output_file,index=False)
    logger.info("Saved combined:", output_file)
    return str(output_file)


def save_clean_parquet(endpoint: str, df: pd.DataFrame, base_path: Path):
    endpoint_name = endpoint.lstrip("/").replace("/", "-") or "root"

    base_path.mkdir(parents=True, exist_ok=True)

    date_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"{date_str}-{endpoint_name}.parquet"
    full_path = base_path / filename

    df.to_parquet(full_path,index=False)

    logger.info(f"Saved cleaned data: {full_path}")
    return full_path