import pandas as pd
import logging
from datetime import date
from spotify_data_pipeline.helpers.blob_utils import (
    list_blobs, download_parquet_from_blob, upload_parquet_to_blob
)

TERMS = ["long_term", "medium_term", "short_term"]
TERM_KEYS = {"long_term": "l", "medium_term": "m", "short_term": "s"}
SILVER = "silver"
GOLD = "gold"
PREFIX = "recent_tracks/"

def load_silver(scope: str, time_range: str):
    prefix = f"{scope}_{time_range}"
    blob_paths = [p for p in list_blobs(SILVER, prefix)
                  if p.endswith(".parquet") and "/archive/" not in p]
    logging.info(f"{len(blob_paths)} files in {prefix}")

    if not blob_paths:
        logging.info("No matching Parquet blobs found.")
        return pd.DataFrame()
    
    if len(blob_paths) > 100:
        logging.warning(f"Loading {len(blob_paths)} parquet files into memory")

    dfs = [download_parquet_from_blob(SILVER, path) for path in blob_paths]
    dfs = [df for df in dfs if not df.empty]    

    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)

def write_gold(df: pd.DataFrame, scope: str, year: str | None = None):
    snapshot_date = date.today()
    if year is not None:
        blob_path = f"{scope}/{year}/{scope}_{snapshot_date}.parquet"
    else:
        blob_path = f"{scope}/{scope}_{snapshot_date}.parquet"
    upload_parquet_to_blob(df, GOLD, blob_path)
    logging.info(f"{len(df)} rows written to gold for scope {scope}")

def build_gold_artist(time_range: str):
    df = load_silver("top_artists", time_range)
    if df.empty:
        logging.info(f"No Silver data for top_artists/{time_range}")
        return df
    df["term"] = time_range
    df["scope"] = "top_artists"
    df = df.drop_duplicates(subset=["id", "snapshot_date", "position", "term"], keep = "first")
    write_gold(df, "top_artists", time_range)
    logging.info(f"{len(df)} artists files written to gold")    

def clean_silver_tracks(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = clean_track_names(df)
    df = clean_track_sequence(df)
    df["artists_combined"] = df.apply(
        lambda row: [
            {"id": i, "name": n, "type": t}
            for i, n, t in zip(
                row["artist_ids"], row["artist_names"], row["artist_types"]
            )
        ],
        axis=1
    )   
    df = df.explode("artists_combined")
    df["artist_id"] = df["artists_combined"].apply(lambda x: x["id"] if x else None)
    df["artist_name"] = df["artists_combined"].apply(lambda x: x["name"] if x else None)
    df["artist_type"] = df["artists_combined"].apply(lambda x: x["type"] if x else None)
    df["album_artist_id"] = df["artists_combined"].apply(lambda x: x["id"] if x else None)
    df["album_artist_name"] = df["artists_combined"].apply(lambda x: x["name"] if x else None)
    df = df.drop(columns=["artists_combined"])
    df = df.drop_duplicates(subset=["track_id", "artist_id", "snapshot_date"], keep="first")
    return df 

def clean_silver_recent_tracks(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_track_names(df)
    df = clean_track_sequence(df)

    df["artists_combined"] = df.apply(
        lambda row: [
            {"id": i, "name": n, "type": t}
            for i, n, t in zip(
                   row["artist_ids"], row["artist_names"], row["artist_types"]
            )
        ],
        axis=1
    )

    df = df.explode("artists_combined")

    df["artist_id"] = df["artists_combined"].apply(lambda x: x["id"] if x else None)
    df["artist_name"] = df["artists_combined"].apply(lambda x: x["name"] if x else None)
    df["artist_type"] = df["artists_combined"].apply(lambda x: x["type"] if x else None)
    df["album_artist_id"] = df["artists_combined"].apply(lambda x: x["id"] if x else None)
    df["album_artist_name"] = df["artists_combined"].apply(lambda x: x["name"] if x else None)

    df = df.drop(columns=["artists_combined"])


    df = df.drop_duplicates(
        subset=["track_id", "artist_id", "played_at"],
        keep="first"
    )
    
    return df    

def build_gold_top_tracks(time_range: str):
    df = load_silver("top_tracks", time_range)
    if df.empty:
        logging.info(f"No silver data for top_tracks/{time_range}")
        return
    df = clean_silver_tracks(df)
    df["term"] = time_range
    df["scope"] = "top_tracks"
    write_gold(df, "top_tracks", time_range)
    logging.info(f"{len(df)} top_tracks rows written to gold")   

def build_gold_recent_tracks(year: str = "full"):
    blob_paths = [p for p in list_blobs(SILVER, PREFIX)
                  if p.endswith(".parquet") and "/archive/" not in p]

    if year != "full":
        blob_paths = [p for p in blob_paths if f"/{year}/" in p]

    if not blob_paths:
        logging.info("No matching Parquet blobs found.")
        return
    
    dfs = [download_parquet_from_blob(SILVER, path) for path in blob_paths]
    dfs = [df for df in dfs if not df.empty]

    if not dfs:
        logging.info("No items in Parquet blobs")
        return

    df_all = pd.concat(dfs, ignore_index=True)
    df_all = clean_silver_recent_tracks(df_all)
    write_gold(df_all, "recent_tracks", year if year != "full" else None)
    logging.info(f"{len(df_all)} recent_tracks rows written to gold")


def clean_track_sequence(df: pd.DataFrame) -> pd.DataFrame:
    desired_order = [
        "played_at"
    ]
    front = [c for c in desired_order if c in df.columns]
    rest = [c for c in df.columns if c not in front]
    return df[front + rest]

def clean_track_names(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={
        "album.id" : "album_id",
        "album.name" : "album_name",
        "album.release_date" : "release_date",
        "album.total_tracks" : "total_tracks",
        "track.album.type" : "album_type",
        "track.disc_number" : "disc_number",
        "track.duration_ms" : "duration_ms",
        "track.explicit" : "explicit",
        "track.popularity" : "popularity",
        "track.track_number" : "track_number",
        "track.type" : "track_type",
        "context.type" : "context_type",
        "album.album_type" :  "album_type"
    })
    return df     