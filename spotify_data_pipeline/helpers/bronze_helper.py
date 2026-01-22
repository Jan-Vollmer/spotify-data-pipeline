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