from datetime import datetime
import pandas as pd
from pathlib import Path
from spotify_data_pipeline.helpers.file_utils import move_to_archive, list_json_files, extract_date_from_filename
from spotify_data_pipeline.helpers.pandas_utils import load_jsons_to_df, transform_silver

def extract_timestamp(file_path):
    ts_str = file_path.stem.split("_")[-1]
    return datetime.strptime(ts_str, "%Y%m%d")

def enrich_json(df: pd.DataFrame, json_files: list[Path]) -> pd.DataFrame:
    enriched_list = []

    for file_path in json_files:
        ts = extract_timestamp(file_path)
        df_file = load_jsons_to_df([file_path])
        df_file = transform_silver(df_file)
        df_file = df_file.copy()
        df_file["position"] = range(1, len(df_file) + 1)
        df_file["timestamp"] = ts
        enriched_list.append(df_file)

    return pd.concat(enriched_list, ignore_index=True)

def process_silver_artists(time_range : str):
    bronze_dir = Path("data/bronze/top_artists") / time_range
    archive_dir = bronze_dir / "archive"
    json_files = list_json_files(bronze_dir)

    if not json_files:
        return

    for file in json_files:
        df = load_jsons_to_df([file])
        snapshot_date = extract_date_from_filename(file)
        df["snapshot_date"] = snapshot_date
        df = transform_silver(df)

        df["position"] = range(1, len(df) + 1)
        snapshot_str = snapshot_date.strftime("%Y-%m-%dT%H-%M-%S")
        silver_file = Path("data/silver/top_artists") / time_range / f"top_artists_{snapshot_str}.parquet"
        silver_file.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(silver_file, index=False)
        move_to_archive(file, archive_dir)