from datetime import datetime
import pandas as pd
from pathlib import Path
from spotify_data_pipeline.helpers.file_utils import move_to_archive, list_json_files, extract_date_from_filename
from spotify_data_pipeline.helpers.pandas_utils import load_jsons_to_df, transform_silver_track
import logging

def process_silver_tracks(time_range : str):
    logging.info(f"Processing silver tracks for time range '{time_range}'")
    bronze_dir = Path("data/bronze/top_tracks") / time_range
    archive_dir = bronze_dir / "archive"
    json_files = list_json_files(bronze_dir)
    logging.info(f"Found {len(json_files)} bronze files in {bronze_dir}")
    if not json_files:
        return

    for file in json_files:
        df = load_jsons_to_df([file])
        if df.empty:
            logging.warning(f"No tracks in bronze file {file.name}")

        snapshot_date = extract_date_from_filename(file)
        df["snapshot_date"] = snapshot_date
        df = transform_silver_track(df)
        logging.info(f"Transforming {len(df)} rows from {file.name}")

        if df.empty:
            logging.warning(f"No tracks after transform for snapshot {snapshot_date}")

        df["position"] = range(1, len(df) + 1)
        snapshot_str = snapshot_date.strftime("%Y-%m-%dT%H-%M-%S")
        silver_file = Path("data/silver/top_tracks") / time_range / f"top_tracks_{snapshot_str}.parquet"
        silver_file.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(silver_file, index=False)
        logging.info(f"Wrote {len(df)} rows to {silver_file}")

        move_to_archive(file, archive_dir)
        logging.info(f"Archived bronze file {file.name}")