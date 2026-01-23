import json
from pathlib import Path
from typing import Any

def write_bronze_batch(
    entity: str,
    payload: Any,
    downloaded_at: str,
    base_path: str = "data/bronze",
    subdir: str | None = None,
):
    path = Path(base_path) / entity

    if subdir:
        path = path / subdir

    path.mkdir(parents=True, exist_ok=True)

    file_path = path / f"{entity}_{downloaded_at}.json"

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=4)

    return file_path

def fetch_and_write(entity: str, getter_func, access_token: str, downloaded_at: str, limit: int = None, time_ranges: list[str] = None):
    if time_ranges is None:
        time_ranges = ["short_term", "medium_term", "long_term"]
    
    for tr in time_ranges:
        items = getter_func(access_token, limit=limit, time_range=tr)
        write_bronze_batch(
            entity=entity,
            payload=items,
            downloaded_at=downloaded_at,
            subdir=f"{tr}"
        )