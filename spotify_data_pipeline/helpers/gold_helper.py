import pandas as pd
from pathlib import Path
from datetime import date
from spotify_data_pipeline.helpers.file_utils import list_json_files

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
