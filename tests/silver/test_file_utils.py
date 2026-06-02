from datetime import datetime
from pathlib import Path
import pytest
from spotify_data_pipeline.helpers.file_utils import move_to_archive, list_json_files, extract_date_from_filename

def test_move_to_archive(tmp_path):
    file = tmp_path / "test.json"
    file.write_text('{"artist":"a"}')

    archive_dir = tmp_path / "archive"

    dest = move_to_archive(file, archive_dir)

    assert dest.exists()
    assert not file.exists()
    assert dest.read_text() == '{"artist":"a"}'

def test_list_json_files(tmp_path):
    (tmp_path / "b.json").write_text("b")
    (tmp_path / "a.json").write_text("a")
    (tmp_path / "ignore.txt").write_text("x")

    files = list_json_files(tmp_path)

    assert [f.name for f in files] == ["a.json", "b.json"]    

def test_extract_date_from_filename():
    file = Path("top_artists_2024-01-01T12-30-00.json")
    dt = extract_date_from_filename(file)
    expected = datetime.strptime("2024-01-01T12-30-00", "%Y-%m-%dT%H-%M-%S")
    assert dt == expected

def test_extract_date_from_filename_no_match():
    file = Path("no_date.json")
    dt = extract_date_from_filename(file)
    assert dt is None    