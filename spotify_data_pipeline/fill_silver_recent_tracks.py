import json
from pathlib import Path
import pandas as pd
import shutil

def fill_silver_recent_tracks():
    bronze_dir = Path("data/bronze/recent_tracks")
    archive_dir = bronze_dir / "archive"
    silver_dir = Path("data/silver/recent_tracks")

    archive_dir.mkdir(parents=True, exist_ok=True)
    silver_dir.mkdir(parents=True, exist_ok=True)

    all_items = []
    json_files = sorted(bronze_dir.glob("*.json"))

    for file_path in json_files:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            all_items.extend(data)

    if not all_items:
        print("Keine Daten gefunden.")
        return
    
    df = pd.json_normalize(all_items)
    parquet_path = silver_dir / "recent_tracks.parquet"

    df["played_at"] = pd.to_datetime(df["played_at"])
    df["year"] = df["played_at"].dt.year
    df["month"] = df["played_at"].dt.month

    for (year, month), group in df.groupby(["year", "month"]):
        month_dir = silver_dir / str(year)
        month_dir.mkdir(parents=True, exist_ok=True)
        parquet_path = month_dir / f"{month:02d}.parquet"
        if parquet_path.exists():
            existing_df = pd.read_parquet(parquet_path)
            combined = pd.concat([existing_df, group], ignore_index=True)
            combined.drop_duplicates(subset="played_at", keep="first", inplace=True)
        else:
            combined = group
        combined.to_parquet(parquet_path, index=False)
        print(f"{len(combined)} Zeilen gespeichert in {parquet_path}")

    for file_path in json_files:
        dest = archive_dir / file_path.name
        shutil.move(str(file_path), str(dest))

    print(f"{len(json_files)} Dateien nach archive verschoben.")    