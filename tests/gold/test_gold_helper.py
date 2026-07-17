import pandas as pd
from unittest.mock import patch
from datetime import date

from spotify_data_pipeline.helpers.gold_helper import (
    load_silver, write_gold, build_gold_artist, build_gold_top_tracks,
    build_gold_recent_tracks, clean_silver_tracks, clean_silver_recent_tracks,
    clean_track_sequence, clean_track_names,
)

GOLD_HELPER = "spotify_data_pipeline.helpers.gold_helper"


# --- load_silver -------------------------------------------------------

def test_load_silver_reads_and_concats():
    df1 = pd.DataFrame({"a": [1]})
    df2 = pd.DataFrame({"a": [2]})

    with patch(f"{GOLD_HELPER}.list_blobs", return_value=[
            "top_artists_short/f1.parquet", "top_artists_short/f2.parquet"]), \
         patch(f"{GOLD_HELPER}.download_parquet_from_blob", side_effect=[df1, df2]):

        result = load_silver("top_artists", "short")

        assert len(result) == 2
        assert result["a"].tolist() == [1, 2]

def test_load_silver_filters_non_parquet_and_archive():
    with patch(f"{GOLD_HELPER}.list_blobs", return_value=[
            "top_artists_short/f1.parquet",
            "top_artists_short/archive/old.parquet",
            "top_artists_short/readme.txt",
        ]), \
         patch(f"{GOLD_HELPER}.download_parquet_from_blob", return_value=pd.DataFrame({"a": [1]})) as mock_dl:

        load_silver("top_artists", "short")

        mock_dl.assert_called_once_with("silver", "top_artists_short/f1.parquet")

def test_load_silver_no_blobs_returns_empty_df():
    with patch(f"{GOLD_HELPER}.list_blobs", return_value=[]):
        df = load_silver("top_artists", "short")
        assert df.empty

def test_load_silver_skips_empty_downloads():
    with patch(f"{GOLD_HELPER}.list_blobs", return_value=["a.parquet", "b.parquet"]), \
         patch(f"{GOLD_HELPER}.download_parquet_from_blob",
               side_effect=[pd.DataFrame({"a": [1]}), pd.DataFrame()]):

        result = load_silver("top_artists", "short")
        assert len(result) == 1


# --- write_gold ----------------------------------------------------------

def test_write_gold_path_with_year():
    df = pd.DataFrame({"artist": ["A1"]})
    fixed_date = date(2026, 2, 12)

    with patch(f"{GOLD_HELPER}.upload_parquet_to_blob") as mock_upload, \
         patch(f"{GOLD_HELPER}.date") as mock_date:
        mock_date.today.return_value = fixed_date

        write_gold(df, "top_artists", "short")

        args, _ = mock_upload.call_args
        assert args[1] == "gold"
        assert args[2] == "top_artists/short/top_artists_2026-02-12.parquet"

def test_write_gold_path_without_year():
    df = pd.DataFrame({"artist": ["A1"]})
    fixed_date = date(2026, 2, 12)

    with patch(f"{GOLD_HELPER}.upload_parquet_to_blob") as mock_upload, \
         patch(f"{GOLD_HELPER}.date") as mock_date:
        mock_date.today.return_value = fixed_date

        write_gold(df, "recent_tracks")

        args, _ = mock_upload.call_args
        assert args[2] == "recent_tracks/recent_tracks_2026-02-12.parquet"


# --- build_gold_artist / build_gold_top_tracks ----------------------------

def test_build_gold_artist_dedups_and_writes():
    dummy_df = pd.DataFrame({
        "id": ["1", "1"], "snapshot_date": ["2023", "2023"], "position": [1, 1],
    })

    with patch(f"{GOLD_HELPER}.load_silver", return_value=dummy_df), \
         patch(f"{GOLD_HELPER}.write_gold") as mock_write:

        build_gold_artist("short")

        written_df = mock_write.call_args[0][0]
        assert len(written_df) == 1
        assert (written_df["term"] == "short").all()
        mock_write.assert_called_once_with(written_df, "top_artists", "short")

def test_build_gold_artist_empty_silver_skips_write():
    with patch(f"{GOLD_HELPER}.load_silver", return_value=pd.DataFrame()), \
         patch(f"{GOLD_HELPER}.write_gold") as mock_write:

        build_gold_artist("short")
        mock_write.assert_not_called()

def test_build_gold_top_tracks_calls_pipeline():
    dummy_df = pd.DataFrame({"id": ["1"]})

    with patch(f"{GOLD_HELPER}.load_silver", return_value=dummy_df), \
         patch(f"{GOLD_HELPER}.clean_silver_tracks", return_value=dummy_df) as mock_clean, \
         patch(f"{GOLD_HELPER}.write_gold") as mock_write:

        build_gold_top_tracks("short")

        mock_clean.assert_called_once_with(dummy_df)
        mock_write.assert_called_once_with(dummy_df, "top_tracks", "short")

def test_build_gold_top_tracks_empty_silver_skips_write():
    with patch(f"{GOLD_HELPER}.load_silver", return_value=pd.DataFrame()), \
         patch(f"{GOLD_HELPER}.write_gold") as mock_write:

        build_gold_top_tracks("short")
        mock_write.assert_not_called()


# --- build_gold_recent_tracks ----------------------------------------------

def test_build_gold_recent_tracks_filters_by_year():
    dummy_df = pd.DataFrame({"id": ["1"], "played_at": ["2023"]})

    with patch(f"{GOLD_HELPER}.list_blobs", return_value=[
            "recent_tracks/2023/a.parquet", "recent_tracks/2024/b.parquet"]), \
         patch(f"{GOLD_HELPER}.download_parquet_from_blob", return_value=dummy_df) as mock_dl, \
         patch(f"{GOLD_HELPER}.clean_silver_recent_tracks", return_value=dummy_df), \
         patch(f"{GOLD_HELPER}.write_gold") as mock_write:

        build_gold_recent_tracks(year="2023")

        mock_dl.assert_called_once_with("silver", "recent_tracks/2023/a.parquet")
        mock_write.assert_called_once_with(dummy_df, "recent_tracks", "2023")

def test_build_gold_recent_tracks_full_passes_none_as_year():
    dummy_df = pd.DataFrame({"id": ["1"], "played_at": ["2023"]})

    with patch(f"{GOLD_HELPER}.list_blobs", return_value=["recent_tracks/2023/a.parquet"]), \
         patch(f"{GOLD_HELPER}.download_parquet_from_blob", return_value=dummy_df), \
         patch(f"{GOLD_HELPER}.clean_silver_recent_tracks", return_value=dummy_df), \
         patch(f"{GOLD_HELPER}.write_gold") as mock_write:

        build_gold_recent_tracks(year="full")

        mock_write.assert_called_once_with(dummy_df, "recent_tracks", None)

def test_build_gold_recent_tracks_no_matches_skips_write():
    with patch(f"{GOLD_HELPER}.list_blobs", return_value=["recent_tracks/2024/b.parquet"]), \
         patch(f"{GOLD_HELPER}.write_gold") as mock_write:

        build_gold_recent_tracks(year="2023")
        mock_write.assert_not_called()


# --- Datenqualität: clean_* -------------------------------------------------

def test_clean_track_names_renames_columns():
    df = pd.DataFrame({"album.id": [1], "track.duration_ms": [100], "other": ["x"]})
    result = clean_track_names(df)
    assert "album_id" in result.columns
    assert "duration_ms" in result.columns
    assert "album.id" not in result.columns

def test_clean_track_sequence_orders_columns():
    df = pd.DataFrame({"b": [1], "played_at": [2], "a": [3]})
    result = clean_track_sequence(df)
    assert result.columns[0] == "played_at"

def test_clean_silver_recent_tracks_explodes_and_dedups():
    df = pd.DataFrame({
        "track_id": ["1"],
        "played_at": ["2023-01-01"],
        "artist_ids": [["a", "b"]],
        "artist_names": [["A", "B"]],
        "artist_types": [["artist", "artist"]],
    })

    result = clean_silver_recent_tracks(df)

    assert len(result) == 2
    assert set(result["artist_id"]) == {"a", "b"}
    assert set(result["artist_name"]) == {"A", "B"}

def test_clean_silver_recent_tracks_dedup_on_track_artist_played_at():
    df = pd.DataFrame({
        "track_id": ["1", "1"],
        "played_at": ["2023", "2023"],
        "artist_ids": [["a"], ["a"]],
        "artist_names": [["A"], ["A"]],
        "artist_types": [["artist"], ["artist"]],
    })

    result = clean_silver_recent_tracks(df)
    assert len(result) == 1

def test_clean_silver_tracks_explodes_and_dedups():
    df = pd.DataFrame({
        "track_id": ["1"],
        "snapshot_date": ["2023"],
        "artist_ids": [["a", "b"]],
        "artist_names": [["A", "B"]],
        "artist_types": [["artist", "artist"]],
    })

    result = clean_silver_tracks(df)

    assert len(result) == 2
    assert set(result["artist_id"]) == {"a", "b"}

def test_clean_silver_tracks_empty_input_returns_empty():
    result = clean_silver_tracks(pd.DataFrame())
    assert result.empty