import pandas as pd
from unittest.mock import patch
import pytest
from pathlib import Path
from datetime import date
from spotify_data_pipeline.helpers.gold_helper import load_silver, unify_silver, write_gold

def test_load_silver_reads_and_concats():
    df1 = pd.DataFrame({"a": [1]})
    df2 = pd.DataFrame({"a": [2]})

    fake_files = ["f1.parquet", "f2.parquet"]

    with patch("spotify_data_pipeline.helpers.gold_helper.Path.exists", return_value=True), \
         patch("spotify_data_pipeline.helpers.gold_helper.Path.glob", return_value=fake_files), \
         patch("pandas.read_parquet", side_effect=[df1, df2]):

        result = load_silver("top_artists", "short_term")

        assert len(result) == 2
        assert result["a"].tolist() == [1, 2]

def test_load_silver_no_files_returns_empty_df():
    with patch("spotify_data_pipeline.helpers.gold_helper.Path.exists", return_value=True), \
         patch("spotify_data_pipeline.helpers.gold_helper.Path.glob", return_value=[]):

        df = load_silver("top_artists", "short_term")
        assert df.empty

def test_load_silver_dir_missing_raises():
    with patch("spotify_data_pipeline.helpers.gold_helper.Path.exists", return_value=False):
        with pytest.raises(FileNotFoundError):
            load_silver("top_artists", "short_term")

def test_unify_silver_merges_and_adds_columns():
    df_l = pd.DataFrame({"artist": ["A1"]})
    df_m = pd.DataFrame({"artist": ["A2"]})
    df_s = pd.DataFrame({"artist": ["A3"]})

    dfs = {"l": df_l, "m": df_m, "s": df_s}

    result = unify_silver(dfs, scope="top_artists")

    assert len(result) == 3
    assert set(result["term"]) == {"l", "m", "s"}
    assert all(result["scope"] == "top_artists")
    assert result["artist"].tolist() == ["A1", "A2", "A3"]

def test_unify_silver_skips_empty_dfs():
    df_l = pd.DataFrame({"artist": ["A1"]})
    df_m = pd.DataFrame()
    dfs = {"l": df_l, "m": df_m}

    result = unify_silver(dfs, scope="top_artists")

    assert len(result) == 1
    assert result["term"].iloc[0] == "l"

def test_write_gold_calls_to_parquet_with_correct_path():
    df = pd.DataFrame({"artist": ["A1", "A2"]})
    fixed_date = date(2026, 2, 12)

    with patch("spotify_data_pipeline.helpers.gold_helper.Path.mkdir") as mock_mkdir, \
         patch("pandas.DataFrame.to_parquet") as mock_parquet, \
         patch("spotify_data_pipeline.helpers.gold_helper.date") as mock_date:

        mock_date.today.return_value = fixed_date

        write_gold(df, "top_artists")

        assert mock_mkdir.called

        assert mock_parquet.called

        parquet_path = mock_parquet.call_args[0][0]
        assert str(parquet_path).endswith("top_artists_2026-02-12.parquet")