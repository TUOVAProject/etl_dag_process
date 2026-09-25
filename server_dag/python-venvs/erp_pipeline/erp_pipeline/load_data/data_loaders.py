from __future__ import annotations
from pathlib import Path
from typing import List
import pandas as pd
import re
from typing import Optional, List
from pathlib import Path
from erp_pipeline.model.load_mode import LoadMode
import pandas as pd

def load_and_restrict(path: Path, required_cols: List[str]) -> pd.DataFrame:
    df = load_parquet_safely(path)
    keep = [col for col in required_cols if col in df.columns]
    return df[keep].copy()


def load_latest(
    base: Path,
    prefix: str,
    mode: LoadMode = LoadMode.STAGED,
    required_cols: Optional[List[str]] = None
):
    patt = re.compile(r"(\d{4}-\d{2}-\d{2})-" + re.escape(prefix) + r"\.parquet$")
    candidates = []

    for file in base.glob("*.parquet"):
        m = patt.match(file.name)
        if m:
            candidates.append((m.group(1), file))

    if not candidates:
        raise FileNotFoundError(f"No staged files for {prefix}")

    candidates.sort(key=lambda x: x[0])
    latest_date, filepath = candidates[-1]

    if mode == LoadMode.STAGED:
        if required_cols is not None:
            df = load_and_restrict(filepath, required_cols)
        else:
            df = load_parquet_safely(filepath)
    elif mode == LoadMode.COMBINED:
        df = load_parquet_safely(filepath)
    else:
        raise ValueError(f"Unknown mode '{mode}'. Use 'staged' or 'combined'.")

    return latest_date, df

def load_parquet_safely(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    return df