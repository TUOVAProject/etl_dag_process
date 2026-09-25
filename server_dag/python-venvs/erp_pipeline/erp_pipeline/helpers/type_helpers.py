from __future__ import annotations
import pandas as pd
from typing import Mapping, Sequence

UNKNOWN_DATE_KEY = 0
MAX_DATE_KEY = 20351231

def replace_nan(obj):
    if isinstance(obj, float) and pd.isna(obj):
        return None
    if isinstance(obj, dict):
        return {k: replace_nan(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [replace_nan(v) for v in obj]
    return obj

def shape_and_types(
    df: pd.DataFrame,
    rename_map: Mapping[str, str],
    keep_cols: Sequence[str],
    int_cols: Sequence[str] = (),
    float_cols: Sequence[str] = (),
) -> pd.DataFrame:
   
    out = df.rename(columns=rename_map).copy()

    for c in keep_cols:
        if c not in out.columns:
            out[c] = pd.NA

    out = out[[c for c in keep_cols if c in out.columns]]

    int_set = set(int_cols)
    float_set = set(float_cols)
    str_cols_auto = [c for c in keep_cols if c not in int_set | float_set]

    for c in int_cols:
        if c in out.columns:
            out[c] = to_int64(out[c])

    for c in float_cols:
        if c in out.columns:
            out[c] = to_float64(out[c])

    for c in str_cols_auto:
        if c in out.columns:
            out[c] = out[c].astype("string").str.strip()

    return out

def to_hhmmss(series):
    ts = pd.to_datetime(series, errors="coerce", utc=True).dt.tz_convert("Europe/Helsinki")

    return (
        ts.dt.strftime("%H%M%S")
        .pipe(pd.to_numeric, errors="coerce")
        .astype("Int64")
    )


def to_yyyymmdd(series):
    ts = pd.to_datetime(series, errors="coerce", utc=True).dt.tz_convert("Europe/Helsinki")

    return (
        ts.dt.strftime("%Y%m%d")
        .pipe(pd.to_numeric, errors="coerce")
        .astype("Int64")
    )

def ticks_to_minutes(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    return (numeric / 600_000_000).round()


def to_int64(series: pd.Series) -> pd.Series:
    
    series = series.astype("string").str.strip()

    series = series.replace("", pd.NA)

    return pd.to_numeric(series, errors="coerce").astype("Int64")

def to_int32(series: pd.Series) -> pd.Series:
    
    series = series.astype("string").str.strip()

    series = series.replace("", pd.NA)

    return pd.to_numeric(series, errors="coerce").astype("Int32")

def to_float64(series: pd.Series) -> pd.Series:
    if pd.api.types.is_string_dtype(series) or pd.api.types.is_object_dtype(series):
        series = series.astype("string").str.strip().replace("", pd.NA)
    return pd.to_numeric(series, errors="coerce").astype("Float64")

def get_date_key(dt):

    if isinstance(dt, pd.Series):
        s = dt.astype("string")
        s = pd.to_datetime(s, errors="coerce", utc=True)
        s = s.dt.tz_convert("Europe/Helsinki").dt.tz_localize(None)

        out = pd.to_numeric(
            s.dt.strftime("%Y%m%d"),
            errors="coerce"
        ).astype("Int64")

        out = out.where(
            (out.isna()) | (out <= MAX_DATE_KEY),
            UNKNOWN_DATE_KEY,
        )

        return out

    if pd.isnull(dt):
        return pd.NA

    if isinstance(dt, str):
        ts = pd.to_datetime(dt, errors="coerce", utc=True)

        if pd.isnull(ts):
            return pd.NA

        ts = ts.tz_convert("Europe/Helsinki").tz_localize(None)

        key = int(ts.strftime("%Y%m%d"))

        return (
            UNKNOWN_DATE_KEY
            if key > MAX_DATE_KEY
            else key
        )

    if getattr(dt, "tzinfo", None) is not None:
        dt = dt.tz_convert("Europe/Helsinki").tz_localize(None)

    key = int(pd.Timestamp(dt).strftime("%Y%m%d"))

    return (
        UNKNOWN_DATE_KEY
        if key > MAX_DATE_KEY
        else key
    )