from pathlib import Path

def prepare_dirs(bronze_subdir: str, silver_subdir: str):
    bronze_dir = Path("data/bronze") / bronze_subdir
    archive_dir = bronze_dir / "archive"
    silver_dir = Path("data/silver") / silver_subdir

    bronze_dir.mkdir(parents=True, exist_ok=True)
    archive_dir.mkdir(parents=True, exist_ok=True)
    silver_dir.mkdir(parents=True, exist_ok=True)

    return bronze_dir, archive_dir, silver_dir