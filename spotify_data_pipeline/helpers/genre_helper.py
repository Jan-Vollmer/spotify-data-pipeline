import logging
import pandas as pd
from datetime import date
from spotify_data_pipeline.Bronze.error_handler import request_with_retry
from spotify_data_pipeline.helpers.gold_helper import load_silver, PREFIX
from spotify_data_pipeline.helpers.blob_utils import list_blobs, upload_parquet_to_blob, download_parquet_from_blob

SILVER = "silver"
TERMS = ["short", "medium", "long"]
GENRE_DIM_PATH = "artist_genres/artist_genres.parquet"

def build_initial_artist_genre_dim():
    dfs = []
    for term in TERMS:
        df = load_silver("top_artists", term)
        if df.empty:
            logging.info(f"No silver data for top_artists_{term}")
            continue
        dfs.append(df[["id", "name", "genres", "snapshot_date"]])

    if not dfs:
        logging.info("No top_artists silver data found for initial genre load")
        return pd.DataFrame()

    df_all = pd.concat(dfs, ignore_index=True)
    df_all = df_all.sort_values("snapshot_date").drop_duplicates(subset=["id"], keep="last")
    df_all = df_all.rename(columns={"id": "artist_id", "name": "artist_name"})
    df_all = df_all.drop(columns=["snapshot_date"])
    df_all["updated_last"] = date.today()

    upload_parquet_to_blob(df_all, SILVER, GENRE_DIM_PATH)
    logging.info(f"Initial artist_genre dim written with {len(df_all)} artists")
    return df_all

def load_all_recent_tracks_silver() -> pd.DataFrame:
    blob_paths = [p for p in list_blobs("silver", PREFIX) if p.endswith(".parquet")]
    if not blob_paths:
        return pd.DataFrame()
    dfs = [download_parquet_from_blob("silver", p) for p in blob_paths]
    dfs = [df for df in dfs if not df.empty]
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def get_missing_artist_ids(existing_dim: pd.DataFrame) -> set[str]:
    all_ids = set()

    df_recent = load_all_recent_tracks_silver()
    if not df_recent.empty and "artist_ids" in df_recent.columns:
        all_ids.update(df_recent["artist_ids"].explode().dropna())

    for term in TERMS:
        df = load_silver("top_tracks", term)
        if df.empty or "artist_ids" not in df.columns:
            continue
        all_ids.update(df["artist_ids"].explode().dropna())

    known_ids = set(existing_dim["artist_id"]) if not existing_dim.empty else set()
    return all_ids - known_ids

def get_missing_artist_ids_since(existing_dim: pd.DataFrame, days: int = 7) -> set[str]:
    cutoff = pd.Timestamp.now() - pd.Timedelta(days=days)
    all_ids = set()

    df_recent = load_all_recent_tracks_silver()
    if not df_recent.empty and {"artist_ids", "played_at"} <= set(df_recent.columns):
        df_recent = df_recent.copy()
        df_recent["played_at"] = pd.to_datetime(df_recent["played_at"])
        df_recent = df_recent[df_recent["played_at"] >= cutoff]
        all_ids.update(df_recent["artist_ids"].explode().dropna())

    for term in TERMS:
        df = load_silver("top_tracks", term)
        if df.empty or not {"artist_ids", "snapshot_date"} <= set(df.columns):
            continue
        df = df.copy()
        df["snapshot_date"] = pd.to_datetime(df["snapshot_date"])
        df = df[df["snapshot_date"] >= cutoff]
        all_ids.update(df["artist_ids"].explode().dropna())

    known_ids = set(existing_dim["artist_id"]) if not existing_dim.empty else set()
    return all_ids - known_ids

def get_artist_genres(access_token: str, ids: list[str]) -> list:
    if len(ids) > 50:
        raise ValueError("Spotify /artists endpoint accepts max 50 IDs per call")

    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"ids": ",".join(ids)}

    url = "https://api.spotify.com/v1/artists"
    resp = request_with_retry(url, headers=headers, params=params)

    return resp.json()["artists"]

def chunk(items: list, size: int = 50):
    for i in range(0, len(items), size):
        yield items[i:i + size]

def fetch_and_append_missing(access_token: str, missing_ids: set[str], existing_dim: pd.DataFrame) -> pd.DataFrame:
    if not missing_ids:
        logging.info("No missing artist_ids to fetch")
        return existing_dim

    new_rows = []
    for batch in chunk(list(missing_ids)):
        artists = get_artist_genres(access_token, batch)
        for a in artists:
            new_rows.append({
                "artist_id": a["id"],
                "artist_name": a["name"],
                "genres": a["genres"],
                "updated_last": date.today(),
            })

    if not new_rows:
        logging.warning("No artist data returned from API for missing IDs")
        return existing_dim

    df_new = pd.DataFrame(new_rows)
    df_combined = pd.concat([existing_dim, df_new], ignore_index=True)
    df_combined = df_combined.drop_duplicates(subset=["artist_id"], keep="last")

    upload_parquet_to_blob(df_combined, SILVER, GENRE_DIM_PATH)
    logging.info(f"artist_genres dim updated: {len(df_new)} new artists added, {len(df_combined)} total")
    return df_combined

def update_recent_artist_genres(token: str):
    existing_dim = download_parquet_from_blob(SILVER, GENRE_DIM_PATH)
    missing = get_missing_artist_ids_since(existing_dim, days=7)
    fetch_and_append_missing(token, missing, existing_dim)