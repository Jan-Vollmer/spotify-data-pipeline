from pathlib import Path
import pandas as pd

from spotify_data_pipeline.helpers.dir_helper import prepare_dirs
from spotify_data_pipeline.helpers.file_utils import move_to_archive, list_json_files
from spotify_data_pipeline.helpers.pandas_utils import load_jsons_to_df, append_to_parquet, transform_silver_recent_tracks

def fill_silver_recent_tracks():
    bronze_dir, archive_dir, silver_dir = prepare_dirs("recent_tracks", "recent_tracks")

    json_files = list_json_files(bronze_dir)
    if not json_files:
        print("Keine neuen JSON-Dateien gefunden.")
        return

    df = load_jsons_to_df(json_files)
    if df.empty:
        print("Keine Items in den JSON-Dateien.")
        return

    df = transform_silver_recent_tracks(df)
    
    df["played_at"] = pd.to_datetime(df["played_at"])
    df["year"] = df["played_at"].dt.year
    df["month"] = df["played_at"].dt.month

    for (year, month), group in df.groupby(["year", "month"]):
        month_dir = silver_dir / str(year)
        month_dir.mkdir(parents=True, exist_ok=True)
        parquet_path = month_dir / f"{month:02d}.parquet"

        combined = append_to_parquet(group, parquet_path, subset="played_at")
        print(f"{len(combined)} Zeilen gespeichert in {parquet_path}")

    for file_path in json_files:
        move_to_archive(file_path, archive_dir)

    print(f"{len(json_files)} Dateien nach archive verschoben.")