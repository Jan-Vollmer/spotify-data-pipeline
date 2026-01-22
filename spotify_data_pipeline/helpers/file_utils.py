from pathlib import Path
import shutil

def move_to_archive(file_path: Path, archive_dir: Path):
    archive_dir.mkdir(parents=True, exist_ok=True)
    dest = archive_dir / file_path.name
    shutil.move(str(file_path), str(dest))
    return dest

def list_json_files(dir_path: Path):
    return sorted(dir_path.glob("*.json"))