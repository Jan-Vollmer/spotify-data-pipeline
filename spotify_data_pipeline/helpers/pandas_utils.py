import json
from pathlib import Path
import pandas as pd

DROP_PATTERNS = [
    "available_markets",
    ".images",
    "images",
    "external_urls",
    "href",
    "preview_url",
    "uri",
    "type",
    ".followers"
]

def load_jsons_to_df(json_files):
    all_items = []
    for file_path in json_files:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            all_items.extend(data) 
    if not all_items:
        return pd.DataFrame()
    df = pd.json_normalize(all_items)
    return df

def dedupe_df(df, subset="played_at"):
    df.drop_duplicates(subset=subset, keep="first", inplace=True)
    return df

def append_to_parquet(df, parquet_path, subset="played_at"):
    if parquet_path.exists():
        existing_df = pd.read_parquet(parquet_path)
        combined = pd.concat([existing_df, df], ignore_index=True)
        combined.drop_duplicates(subset=subset, keep="first", inplace=True)
    else:
        combined = df
    combined.to_parquet(parquet_path, index=False)
    return combined

def transform_silver_artist(df: pd.DataFrame) -> pd.DataFrame:
    cols_to_drop = [
        col for col in df.columns
        if any(pattern in col for pattern in DROP_PATTERNS)
    ]

    return df.drop(columns=cols_to_drop, errors="ignore")

def transform_silver_track(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={
    "artists": "artists",
    "album.artists": "album_artists",
    "id": "track_id",
    "name": "track_name",
    "album.id": "album_id",
    "album.name": "album_name",
    "album.release_date": "album_release_date",
    "album.total_tracks": "album_total_tracks"
    }) 

    df["artist_ids"] = df["artists"].apply(
        lambda xs: tuple(a["id"] for a in xs) if isinstance(xs, list) else None
    )
    df["artist_names"] = df["artists"].apply(
    lambda xs: tuple(a["name"] for a in xs) if isinstance(xs, list) else None
    )
    df["artist_types"] = df["artists"].apply(
    lambda xs: tuple(a["type"] for a in xs) if isinstance(xs, list) else None
    )
    df["album_artist_ids"] = df["album_artists"].apply(
    lambda xs: tuple(a["id"] for a in xs) if isinstance(xs, list) else None
    )
    df["album_artist_names"] = df["album_artists"].apply(
    lambda xs: tuple(a["name"] for a in xs) if isinstance(xs, list) else None
    )

    DROP_COLS = [
        "artists",
        "external_urls",
        "href",
        "uri",
        "type",
        "images",
        "available_markets",
        "preview_url",
        "album.images",
        "album.uri",
        "album.artists",
        "album.available_markets",
        "album.external_urls.spotify",
        "album.href",
        "external_urls.spotify",
        "album.is_playable", 
        "album.release_date_precision", 
        "external_ids.isrc", 
        "track.album.release_date_precision", 
        "track.is_local", 
        "is_local"
    ]

    df = df.drop(columns=[c for c in DROP_COLS if c in df.columns],
                 errors="ignore")

    return df

def transform_silver_recent_track(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={
    "track.artists": "artists",
    "track.album.artists": "album_artists",
    "track.id": "track_id",
    "track.name": "track_name",
    "track.album.id": "album_id",
    "track.album.name": "album_name",
    "track.album.release_date": "album_release_date",
    "track.album.total_tracks": "album_total_tracks"
    })  

    df["artist_ids"] = df["artists"].apply(
        lambda xs: tuple(a["id"] for a in xs) if isinstance(xs, list) else None
    )
    df["artist_names"] = df["artists"].apply(
    lambda xs: tuple(a["name"] for a in xs) if isinstance(xs, list) else None
    )
    df["artist_types"] = df["artists"].apply(
    lambda xs: tuple(a["type"] for a in xs) if isinstance(xs, list) else None
    )
    df["album_artist_ids"] = df["album_artists"].apply(
    lambda xs: tuple(a["id"] for a in xs) if isinstance(xs, list) else None
    )
    df["album_artist_names"] = df["album_artists"].apply(
    lambda xs: tuple(a["name"] for a in xs) if isinstance(xs, list) else None
    )
    
    DROP_COLS = [
        "external_urls",
        "href",
        "uri",
        "type",
        "images",
        "available_markets",
        "preview_url",
        "track.album.images",
        "track.album.artists",
        "track.album.available_markets",
        "track.album.external_urls.spotify",
        "track.album.href",
        "track.album.uri",
        "track.external_urls.spotify",
        "track.available_markets",
        "track.external_urls.spotify",
        "track.href",
        "track.preview_url",
        "track.uri",
        "context.external_urls.spotify",
        "context.href",
        "artists",
        "album.artists",
        "track.album.album_type",
        "track.external_ids.isrc",
        "context.uri",
        "album.is_playable", 
        "album.release_date_precision", 
        "external_ids.isrc", 
        "track.album.release_date_precision",
        "track.is_local", 
        "is_local"
    ]

    df = df.drop(columns=[c for c in DROP_COLS if c in df.columns],
                 errors="ignore")

    if "played_at" in df.columns:
        df = df.drop_duplicates(subset=["artist_ids", "played_at"])

    return df