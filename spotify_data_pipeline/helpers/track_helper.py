import json
import logging
import pandas as pd
from spotify_data_pipeline.helpers.blob_utils import (
    list_blobs, download_json_blob, move_blob_to_archive, upload_parquet_to_blob
)
from spotify_data_pipeline.helpers.pandas_utils import transform_silver_track
from spotify_data_pipeline.helpers.file_utils import extract_date_from_filename
from pathlib import Path

BRONZE = "bronze"
SILVER = "silver"

def process_silver_tracks(time_range: str):
    prefix = f"top_tracks_{time_range}/"
    logging.info(f"Prefix: {prefix}")
    blob_paths = [p for p in list_blobs(BRONZE, prefix)
                  if p.endswith(".json") and "/archive/" not in p]

    logging.info(f"Found {len(blob_paths)} bronze blobs for top_tracks/{time_range}")
    if not blob_paths:
        return

    for blob_path in blob_paths:
        data = json.loads(download_json_blob(BRONZE, blob_path))
        if not data:
            logging.warning(f"Empty blob: {blob_path}")
            continue

        df = pd.json_normalize(data)
        snapshot_date = extract_date_from_filename(Path(blob_path))
        df["snapshot_date"] = snapshot_date
        df = transform_silver_track(df)
        df["position"] = range(1, len(df) + 1)

        snapshot_str = snapshot_date.strftime("%Y-%m-%dT%H-%M-%S")
        silver_path = f"top_tracks_{time_range}/top_tracks_{snapshot_str}.parquet"
        upload_parquet_to_blob(df, SILVER, silver_path)
        logging.info(f"Wrote {len(df)} rows to {silver_path}")

        move_blob_to_archive(BRONZE, blob_path)