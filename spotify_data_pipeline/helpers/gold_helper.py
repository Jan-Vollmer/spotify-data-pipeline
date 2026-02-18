import pandas as pd
import logging
from pathlib import Path
from datetime import date

TERMS = ["long_term", "medium_term", "short_term"]
TERM_KEYS = {"long_term": "l", "medium_term": "m", "short_term": "s"}

def load_silver(scope: str, time_range: str):
    silver_dir = Path("data/silver") / scope / time_range

    if not silver_dir.exists():
        raise FileNotFoundError(silver_dir)

    files = list(silver_dir.glob("*.parquet"))
    logging.info(f"{len(files)} files in {silver_dir}")
    if not files:
        return pd.DataFrame()
    
    if len(files) > 100:
        logging.warning(f"Loading {len(files)} parquet files into memory")

    dfs = [pd.read_parquet(f) for f in files]
    return pd.concat(dfs, ignore_index=True)

def unify_silver(dfs: dict[str, pd.DataFrame], scope: str) -> pd.DataFrame:
    enriched = []
    for term, df in dfs.items():
        if df.empty:
            continue
        df = df.copy()
        df["term"] = term
        df["scope"] = scope
        enriched.append(df)

    if not enriched:
        return pd.DataFrame()

    return pd.concat(enriched, ignore_index=True)

def write_gold(df: pd.DataFrame, scope: str):
    gold_dir = Path("data/gold") / scope
    gold_dir.mkdir(parents=True, exist_ok=True)
    snapshot_date = date.today()
    file_path = gold_dir / f"{scope}_{snapshot_date}.parquet"
    df.to_parquet(file_path, index=False)
    logging.info(f"{len(df)} rows written to gold for scope {scope}")

def build_gold_artist():
    silvers = {
        TERM_KEYS[t]: load_silver("top_artists", t)
        for t in TERMS
    }
    df_all = unify_silver(silvers, scope="top_artists")
    write_gold(df_all, "top_artists")
    logging.info(f"{len(df_all)} artists files written to gold")    

def clean_silver_tracks(dfs: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    cleaned = {}

    for key, df in dfs.items():
        if df.empty:
            cleaned[key] = df
            continue
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

        df["artist_ids"] = df["artists_combined"].apply(lambda x: x["id"] if x else None)
        df["artist_names"] = df["artists_combined"].apply(lambda x: x["name"] if x else None)
        df["artist_types"] = df["artists_combined"].apply(lambda x: x["type"] if x else None)

        df = df.drop(columns=["artists_combined"])

        df = df.drop_duplicates(
            subset=["id", "artist_ids", "snapshot_date"],
            keep="first"
        )
        cleaned[key] = df

    return cleaned    

def clean_silver_recent_tracks(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_track_names(df)
    df = clean_track_sequence(df)

    df = df.drop_duplicates(
        subset=["id", "played_at"],
        keep="first"
    )
    
    return df    

def build_gold_top_tracks():
    silvers = {
        TERM_KEYS[t]: load_silver("top_tracks", t)
        for t in TERMS
    }
    silvers = clean_silver_tracks(silvers)
    df_all = unify_silver(silvers, scope="top_tracks")
    write_gold(df_all, "top_tracks")
    logging.info(f"{len(df_all)} top_tracks rows written to gold")   

def build_gold_recent_tracks(year: str = "full"):
    silver_dir = Path("data/silver/recent_tracks/")
    if not silver_dir.exists():
        raise FileNotFoundError(silver_dir)
    
    if year == "full":
        files = list(silver_dir.rglob("*.parquet"))
    else:
        dir = silver_dir / year
        if not dir.exists():
            raise FileNotFoundError(dir)
        files = list(dir.glob("*.parquet"))

    if not files:
        raise FileNotFoundError(silver_dir)
    
    dfs = [pd.read_parquet(f) for f in files]
    df_all = pd.concat(dfs, ignore_index=True)
    df_all = clean_silver_recent_tracks(df_all)
    write_gold(df_all, "recent_tracks")
    logging.info(f"{len(df_all)} recent_tracks rows written to gold")   


def clean_track_sequence(df: pd.DataFrame) -> pd.DataFrame:
    desired_order = [
        "player_at"
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