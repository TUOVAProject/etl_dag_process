from __future__ import annotations
from typing import Tuple,Optional
from pathlib import Path
import pandas as pd
from erp_pipeline.config.erp_config import (
    ERP_SOURCE_CONFIG,
)
from erp_pipeline.file_cleaner import clean_parquet_history
from erp_pipeline.save import save_clean_parquet

STAGED_PATH = Path("/opt/airflow/data/results/staged")

def extract_source_and_endpoint(file_path: Path) -> Tuple[str, str]:
    endpoint = file_path.parent.parent.name
    source_type = file_path.parent.parent.parent.name
    return source_type, endpoint

def process_erp_files(file_path: Path,staged_path: Optional[Path] = None):

    staged_path = staged_path or STAGED_PATH

    """Process ERP workcenter files."""
    source_type, endpoint = extract_source_and_endpoint(file_path)

    df = pd.read_parquet(file_path)

    try:
        columns = ERP_SOURCE_CONFIG[source_type][endpoint]
    except KeyError:
        raise ValueError(
            f"No schema for source_type={source_type}, endpoint={endpoint}"
        )

    df = df[[c for c in columns if c in df.columns]]
    print("Saveing parquet : ", staged_path/ source_type)
    saved_parquet = save_clean_parquet(endpoint, df, staged_path / source_type)
    clean_parquet_history(endpoint, staged_path / source_type, days_to_keep=2)
    return saved_parquet