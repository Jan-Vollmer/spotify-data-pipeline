import json
from pathlib import Path
import pytest
from spotify_data_pipeline.helpers.bronze_helper import write_bronze_batch, fetch_and_write
from unittest.mock import MagicMock

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

def test_fetch_and_write_calls_write_bronze_batch(monkeypatch):
    mock_write = MagicMock()
    monkeypatch.setattr("spotify_data_pipeline.helpers.bronze_helper.write_bronze_batch", mock_write)

    mock_getter = MagicMock(return_value=[{"id": 1}])
    fetch_and_write("top_tracks", mock_getter, "token", "2026-01-23", limit=5)

    assert mock_getter.call_count == 3  # short, medium, long
    assert mock_write.call_count == 3
    assert mock_write.call_args_list[0][1]["subdir"] == "short_term"    