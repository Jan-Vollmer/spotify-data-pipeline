from pathlib import Path
import json
import pandas as pd
import pytest
from unittest.mock import patch, call
from spotify_data_pipeline.helpers.artist_helper import process_silver_artists

DUMMY_DATA = [{"id": "a1", "name": "Artist A"}, {"id": "a2", "name": "Artist B"}]

def test_process_silver_artists_unit():
    blob_paths = [
        "top_artists_long/top_artists_2026-02-23T10-00-00.json",
        "top_artists_long/top_artists_2026-02-24T10-00-00.json",
    ]

    with patch("spotify_data_pipeline.helpers.artist_helper.list_blobs") as mock_list, \
         patch("spotify_data_pipeline.helpers.artist_helper.download_json_blob") as mock_download, \
         patch("spotify_data_pipeline.helpers.artist_helper.upload_parquet_to_blob") as mock_upload, \
         patch("spotify_data_pipeline.helpers.artist_helper.move_blob_to_archive") as mock_move, \
         patch("spotify_data_pipeline.helpers.artist_helper.transform_silver_artist", side_effect=lambda df: df):

        mock_list.return_value = blob_paths
        mock_download.return_value = json.dumps(DUMMY_DATA).encode()

        process_silver_artists("long")

        assert mock_download.call_count == 2
        assert mock_upload.call_count == 2
        assert mock_move.call_count == 2

def test_process_silver_artists_no_files():
    with patch("spotify_data_pipeline.helpers.artist_helper.list_blobs") as mock_list, \
         patch("spotify_data_pipeline.helpers.artist_helper.download_json_blob") as mock_download, \
         patch("spotify_data_pipeline.helpers.artist_helper.upload_parquet_to_blob") as mock_upload, \
         patch("spotify_data_pipeline.helpers.artist_helper.move_blob_to_archive") as mock_move:

        mock_list.return_value = []

        process_silver_artists("long")

        mock_download.assert_not_called()
        mock_upload.assert_not_called()
        mock_move.assert_not_called()

def test_process_silver_artists_empty_blob():
    blob_paths = ["top_artists_long/top_artists_2026-02-23T10-00-00.json"]

    with patch("spotify_data_pipeline.helpers.artist_helper.list_blobs") as mock_list, \
         patch("spotify_data_pipeline.helpers.artist_helper.download_json_blob") as mock_download, \
         patch("spotify_data_pipeline.helpers.artist_helper.upload_parquet_to_blob") as mock_upload, \
         patch("spotify_data_pipeline.helpers.artist_helper.move_blob_to_archive") as mock_move:

        mock_list.return_value = blob_paths
        mock_download.return_value = json.dumps([]).encode()

        process_silver_artists("long")

        mock_upload.assert_not_called()
        mock_move.assert_not_called()