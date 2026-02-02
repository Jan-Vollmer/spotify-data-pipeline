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

def transform_silver(df: pd.DataFrame) -> pd.DataFrame:
    cols_to_drop = [
        col for col in df.columns
        if any(pattern in col for pattern in DROP_PATTERNS)
    ]

    return df.drop(columns=cols_to_drop, errors="ignore")