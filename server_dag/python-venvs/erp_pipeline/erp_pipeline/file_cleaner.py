from __future__ import annotations
import re
from pathlib import Path
from datetime import datetime
import shutil
from typing import Union

from typing import Callable

def clean_folder(endpoint: str, days_to_keep: int, base_path: Union[str, Path] = "results", dry_run: bool = False) -> None:
    endpoint_name = endpoint.lstrip("/").replace("/", "-") or "root"
    endpoint_folder = Path(base_path) / endpoint_name

    if not endpoint_folder.exists():
        print(f"[clean_folder] Skipping: {endpoint_folder} does not exist.")
        return

    date_dirs: list[tuple[datetime, Path]] = []
    for child in endpoint_folder.iterdir():
        if not child.is_dir():
            continue
        try:
            dt = datetime.strptime(child.name, "%Y-%m-%d")
        except ValueError:
            continue
        date_dirs.append((dt, child))

    _cleanup_by_date("clean_folder", date_dirs, days_to_keep, shutil.rmtree, dry_run)


def clean_parquet_history(endpoint: str, base_path: Path, days_to_keep: int) -> None:
    endpoint_name = endpoint.lstrip("/").replace("/", "-") or "root"
    folder = Path(base_path)
    filename_pattern = re.compile(
        rf"(?P<date>\d{{4}}-\d{{2}}-\d{{2}})-{re.escape(endpoint_name)}\.parquet$"
    )

    if not folder.exists():
        return

    files: list[tuple[datetime, Path]] = []
    for f in folder.glob(f"*-{endpoint_name}.parquet"):
        match = filename_pattern.match(f.name)
        if match is None:
            continue
        try:
            dt = datetime.strptime(match.group("date"), "%Y-%m-%d")
        except ValueError:
            continue
        files.append((dt, f))

    _cleanup_by_date(
        "clean_parquet_history",
        files,
        days_to_keep,
        lambda p: p.unlink(missing_ok=True),
    )

def _cleanup_by_date(
    label: str,
    dated_items: list[tuple[datetime, Path]],
    days_to_keep: int,
    delete_fn: Callable[[Path], None],
    dry_run: bool = False,
) -> None:
    if days_to_keep < 0:
        raise ValueError("days_to_keep must be >= 0")

    if not dated_items:
        print(f"[{label}] Nothing to do.")
        return

    dated_items.sort(key=lambda x: x[0], reverse=True)
    keep = [p for _, p in dated_items[:days_to_keep]]
    delete = [p for _, p in dated_items[days_to_keep:]]

    print(f"[{label}] Keeping ({len(keep)}): {[p.name for p in keep]}")
    print(f"[{label}] Deleting ({len(delete)}): {[p.name for p in delete]}")

    for path in delete:
        if dry_run:
            print(f"[{label}] (dry-run) Would delete: {path}")
            continue
        try:
            delete_fn(path)
            print(f"[{label}] Deleted: {path}")
        except Exception as e:
            print(f"[{label}] Failed to delete {path}: {e}")