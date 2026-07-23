import pandas as pd
from spotify_data_pipeline.helpers.data_quality import (
    check_completeness, check_uniqueness, check_referential_consistency
)

def test_check_completeness_detects_nulls():
    df = pd.DataFrame({"track_id": ["1", None], "name": ["a", "b"]})
    issues = check_completeness(df, ["track_id", "name"])
    assert issues == {"track_id": 1}

def test_check_completeness_detects_missing_column():
    df = pd.DataFrame({"name": ["a"]})
    issues = check_completeness(df, ["track_id", "name"])
    assert issues["track_id"] == 1

def test_check_completeness_no_issues():
    df = pd.DataFrame({"track_id": ["1", "2"]})
    assert check_completeness(df, ["track_id"]) == {}

def test_check_uniqueness_counts_duplicates():
    df = pd.DataFrame({"track_id": ["1", "1", "2"]})
    assert check_uniqueness(df, subset=["track_id"]) == 1

def test_check_uniqueness_no_duplicates():
    df = pd.DataFrame({"track_id": ["1", "2"]})
    assert check_uniqueness(df, subset=["track_id"]) == 0

def test_check_referential_consistency_finds_missing_keys():
    df = pd.DataFrame({"artist_id": ["a", "b", "c"]})
    ref_df = pd.DataFrame({"artist_id": ["a", "b"]})
    result = check_referential_consistency(df, ref_df, key="artist_id")
    assert result == ["c"]

def test_check_referential_consistency_empty_ref_returns_empty():
    df = pd.DataFrame({"artist_id": ["a"]})
    ref_df = pd.DataFrame()
    assert check_referential_consistency(df, ref_df, key="artist_id") == []