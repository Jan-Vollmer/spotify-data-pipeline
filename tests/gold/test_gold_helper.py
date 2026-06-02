import pandas as pd
from unittest.mock import patch
import pytest
from datetime import date
from spotify_data_pipeline.helpers.gold_helper import load_silver, unify_silver, write_gold, build_gold_artist, clean_silver_tracks, clean_silver_recent_tracks, build_gold_top_tracks, build_gold_recent_tracks, clean_track_sequence, clean_track_names

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

def test_clean_track_names_renames_columns():
    df = pd.DataFrame({
        "album.id": [1],
        "track.duration_ms": [100],
        "other": ["x"]
    })

    result = clean_track_names(df)

    assert "album_id" in result.columns
    assert "duration_ms" in result.columns
    assert "other" in result.columns
    assert "album.id" not in result.columns

def test_clean_track_sequence_orders_columns():
    df = pd.DataFrame({
        "b": [1],
        "player_at": [2],
        "a": [3]
    })

    result = clean_track_sequence(df)

    assert result.columns[0] == "player_at"

def test_clean_silver_recent_tracks_dedup():
    df = pd.DataFrame({
        "id": ["1", "1"],
        "played_at": ["2023", "2023"],
        "track.duration_ms": [100, 100]
    })

    result = clean_silver_recent_tracks(df)

    assert len(result) == 1
    assert "duration_ms" in result.columns        

def test_clean_silver_tracks_explode_and_dedup():
    df = pd.DataFrame({
        "id": ["1"],
        "snapshot_date": ["2023"],
        "artist_ids": [["a", "b"]],
        "artist_names": [["A", "B"]],
        "artist_types": [["artist", "artist"]],
    })

    result = clean_silver_tracks({"short_term": df})
    cleaned_df = result["short_term"]

    assert len(cleaned_df) == 2

    assert "artist_ids" in cleaned_df.columns
    assert cleaned_df["artist_ids"].isin(["a", "b"]).all()

def test_build_gold_recent_tracks(tmp_path, monkeypatch):

    silver_dir = tmp_path / "data/silver/recent_tracks/2023"
    silver_dir.mkdir(parents=True)

    df = pd.DataFrame({
        "id": ["1"],
        "played_at": ["2023"],
    })

    file_path = silver_dir / "test.parquet"
    df.to_parquet(file_path)

    monkeypatch.chdir(tmp_path)

    build_gold_recent_tracks(year="2023")

    gold_dir = tmp_path / "data/gold/recent_tracks"
    assert gold_dir.exists()

def test_build_gold_top_tracks_calls_pipeline():

    dummy_df = pd.DataFrame({"id": ["1"]})

    with patch("spotify_data_pipeline.helpers.gold_helper.load_silver", return_value=dummy_df), \
         patch("spotify_data_pipeline.helpers.gold_helper.clean_silver_tracks", return_value={"short": dummy_df}), \
         patch("spotify_data_pipeline.helpers.gold_helper.unify_silver", return_value=dummy_df) as mock_unify, \
         patch("spotify_data_pipeline.helpers.gold_helper.write_gold") as mock_write:

        build_gold_top_tracks()

        mock_unify.assert_called_once()
        mock_write.assert_called_once_with(dummy_df, "top_tracks")