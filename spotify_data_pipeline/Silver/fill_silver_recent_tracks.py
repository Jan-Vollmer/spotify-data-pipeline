import json
import logging
import pandas as pd
from spotify_data_pipeline.helpers.blob_utils import (
    list_blobs, download_json_blob, move_blob_to_archive,
    download_parquet_from_blob, upload_parquet_to_blob
)
from spotify_data_pipeline.helpers.pandas_utils import transform_silver_recent_track

BRONZE = "bronze"
SILVER = "silver"
PREFIX = "/bronze/recent_tracks/"

def fill_silver_recent_tracks():
    blob_paths = [p for p in list_blobs(BRONZE, PREFIX)
                  if p.endswith(".json") and "/archive/" not in p]

    if not blob_paths:
        logging.info("No new JSON blobs found.")
        return

    all_items = []
    for path in blob_paths:
        data = json.loads(download_json_blob(BRONZE, path))
        all_items.extend(data)

    if not all_items:
        logging.info("No items in JSON blobs.")
        return

    df = pd.json_normalize(all_items)
    df = transform_silver_recent_track(df)

    tmp = pd.to_datetime(df["played_at"], format="ISO8601", utc=True)
    df["year"] = tmp.dt.year
    df["month"] = tmp.dt.month

    for (year, month), group in df.groupby(["year", "month"]):
        blob_path = f"recent_tracks/{year}/{month:02d}.parquet"
        existing = download_parquet_from_blob(SILVER, blob_path)
        if not existing.empty:
            combined = pd.concat([existing, group], ignore_index=True)
            combined.drop_duplicates(subset="played_at", keep="first", inplace=True)
        else:
            combined = group.drop(columns=["year", "month"], errors="ignore")
        upload_parquet_to_blob(combined, SILVER, blob_path)
        logging.info(f"{len(combined)} rows saved to {blob_path}")

    for path in blob_paths:
        move_blob_to_archive(BRONZE, path)

    logging.info(f"{len(blob_paths)} blobs moved to archive.")