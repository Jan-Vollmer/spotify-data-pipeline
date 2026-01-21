import json
from pathlib import Path
import pytest
from spotify_data_pipeline.bronze_helper import write_bronze_batch

def test_write_bronze_batch_creates_file_and_writes_json(tmp_path: Path):
    entity = "customer"
    payload = {"id": 1, "name": "Max"}
    downloaded_at = "2024-01-01"
    base_path = tmp_path

    file_path = write_bronze_batch(
        entity=entity,
        payload=payload,
        downloaded_at=downloaded_at,
        base_path=str(base_path),
    )

    assert file_path.exists()
    assert file_path.is_file()

    with open(file_path, encoding="utf-8") as f:
        data = json.load(f)

    assert data == payload

def test_write_bronze_batch_with_subdir(tmp_path: Path):
    entity = "order"
    payload = {"order_id": 123}
    downloaded_at = "2024-01-02"
    subdir = "daily"
    base_path = tmp_path

    file_path = write_bronze_batch(
        entity=entity,
        payload=payload,
        downloaded_at=downloaded_at,
        base_path=str(base_path),
        subdir=subdir,
    )

    expected_path = (
        tmp_path / entity / subdir / f"{entity}_{downloaded_at}.json"
    )

    assert file_path == expected_path
    assert file_path.exists()    

def test_write_bronze_batch_creates_directories(tmp_path: Path):
    entity = "product"
    payload = {"sku": "ABC"}
    downloaded_at = "2024-01-03"
    subdir = "full"

    write_bronze_batch(
        entity=entity,
        payload=payload,
        downloaded_at=downloaded_at,
        base_path=str(tmp_path),
        subdir=subdir,
    )

    dir_path = tmp_path / entity / subdir
    assert dir_path.exists()
    assert dir_path.is_dir()