import pandas as pd
import logging

def check_completeness(df: pd.DataFrame, required_cols: list[str]) -> dict[str, int]:
    issues = {}
    for col in required_cols:
        if col not in df.columns:
            issues[col] = len(df)
            continue
        n_null = df[col].isna().sum()
        if n_null > 0:
            issues[col] = int(n_null)
    return issues 

def check_uniqueness(df: pd.DataFrame, subset: list[str]) -> int:
    missing_cols = [c for c in subset if c not in df.columns]
    if missing_cols:
        logging.warning(f"check_uniqueness: columns missing, skipping: {missing_cols}")
        return 0
    return int(df.duplicated(subset=subset, keep="first").sum())

def check_referential_consistency(df: pd.DataFrame, ref_df: pd.DataFrame, key: str) -> list:
    if df.empty or ref_df.empty:
        return []
    missing_keys = set(df[key].dropna()) - set(ref_df[key].dropna())
    return sorted(missing_keys)