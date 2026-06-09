import json
import pandas as pd
from unittest.mock import patch
from spotify_data_pipeline.helpers.track_helper import process_silver_tracks

DUMMY_DATA = [{"id": "t1", "name": "Track A"}, {"id": "t2", "name": "Track B"}]

def test_process_silver_tracks_unit():
    blob_paths = [
        "top_tracks_long/top_tracks_2026-02-23T10-00-00.json",
        "top_tracks_long/top_tracks_2026-02-24T10-00-00.json",
    ]

    with patch("spotify_data_pipeline.helpers.track_helper.list_blobs") as mock_list, \
         patch("spotify_data_pipeline.helpers.track_helper.download_json_blob") as mock_download, \
         patch("spotify_data_pipeline.helpers.track_helper.upload_parquet_to_blob") as mock_upload, \
         patch("spotify_data_pipeline.helpers.track_helper.move_blob_to_archive") as mock_move, \
         patch("spotify_data_pipeline.helpers.track_helper.transform_silver_track", side_effect=lambda df: df):

        mock_list.return_value = blob_paths
        mock_download.return_value = json.dumps(DUMMY_DATA).encode()

        process_silver_tracks("long")

        assert mock_download.call_count == 2
        assert mock_upload.call_count == 2
        assert mock_move.call_count == 2

def test_process_silver_tracks_no_files():
    with patch("spotify_data_pipeline.helpers.track_helper.list_blobs") as mock_list, \
         patch("spotify_data_pipeline.helpers.track_helper.download_json_blob") as mock_download, \
         patch("spotify_data_pipeline.helpers.track_helper.upload_parquet_to_blob") as mock_upload, \
         patch("spotify_data_pipeline.helpers.track_helper.move_blob_to_archive") as mock_move:

        mock_list.return_value = []

        process_silver_tracks("long")

        mock_download.assert_not_called()
        mock_upload.assert_not_called()
        mock_move.assert_not_called()

def test_process_silver_tracks_empty_blob():
    blob_paths = ["top_tracks_long/top_tracks_2026-02-23T10-00-00.json"]

    with patch("spotify_data_pipeline.helpers.track_helper.list_blobs") as mock_list, \
         patch("spotify_data_pipeline.helpers.track_helper.download_json_blob") as mock_download, \
         patch("spotify_data_pipeline.helpers.track_helper.upload_parquet_to_blob") as mock_upload, \
         patch("spotify_data_pipeline.helpers.track_helper.move_blob_to_archive") as mock_move:

        mock_list.return_value = blob_paths
        mock_download.return_value = json.dumps([]).encode()

        process_silver_tracks("long")

        mock_upload.assert_not_called()
        mock_move.assert_not_called()