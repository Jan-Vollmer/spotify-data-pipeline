import pandas as pd
from pathlib import Path
from datetime import date

TERMS = ["long_term", "medium_term", "short_term"]
TERM_KEYS = {"long_term": "l", "medium_term": "m", "short_term": "s"}

def load_silver(scope: str, time_range: str):
    silver_dir = Path("data/silver") / scope / time_range

    if not silver_dir.exists():
        raise FileNotFoundError(silver_dir)

    files = list(silver_dir.glob("*.parquet"))
    if not files:
        return pd.DataFrame()

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

def build_gold_artist():
    silvers = {
        TERM_KEYS[t]: load_silver("top_artists", t)
        for t in TERMS
    }
    df_all = unify_silver(silvers, scope="top_artists")
    write_gold(df_all, "top_artists")    

def clean_silver_tracks(dfs: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    cleaned = {}

    for key, df in dfs.items():
        if df.empty:
            cleaned[key] = df
            continue
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

def build_gold_tracks(scope: str):
    silvers = {
        TERM_KEYS[t]: load_silver(scope, t)
        for t in TERMS
    }
    silvers = clean_silver_tracks(silvers)
    df_all = unify_silver(silvers, scope=scope)
    write_gold(df_all, scope)