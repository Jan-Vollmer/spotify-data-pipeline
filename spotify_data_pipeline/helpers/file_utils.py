from pathlib import Path
import shutil
import re
from datetime import datetime

def move_to_archive(file_path: Path, archive_dir: Path):
    archive_dir.mkdir(parents=True, exist_ok=True)
    dest = archive_dir / file_path.name
    shutil.move(str(file_path), str(dest))
    return dest

def list_json_files(dir_path: Path):
    return sorted(dir_path.glob("*.json"))

def extract_date_from_filename(path: str):
    # top_artists_2024-01-01T12-30-00.json
    filename = path.name
    match = re.search(r"_(\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2})", filename)
    if not match:
        return None
    raw = match.group(1)
    return datetime.strptime(raw, "%Y-%m-%dT%H-%M-%S")