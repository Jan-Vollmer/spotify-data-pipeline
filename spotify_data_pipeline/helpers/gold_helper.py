import pandas as pd
from pathlib import Path
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

def unify_silver():
