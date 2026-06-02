from pathlib import Path
from datetime import datetime
import pandas as pd
import pytest
from unittest.mock import patch, Mock
from spotify_data_pipeline.helpers.artist_helper import process_silver_artists

def test_process_silver_artists_unit():
    dummy_files = [Path("file_20260223.json"), Path("file_20260224.json")]
    dummy_df = pd.DataFrame({"artist": ["a", "b"]})

    with patch("spotify_data_pipeline.helpers.artist_helper.list_json_files") as mock_list, \
         patch("spotify_data_pipeline.helpers.artist_helper.load_jsons_to_df") as mock_load, \
         patch("spotify_data_pipeline.helpers.artist_helper.extract_date_from_filename") as mock_date, \
         patch("spotify_data_pipeline.helpers.artist_helper.transform_silver_artist") as mock_transform, \
         patch("pandas.DataFrame.to_parquet") as mock_to_parquet, \
         patch("spotify_data_pipeline.helpers.artist_helper.move_to_archive") as mock_move, \
         patch("pathlib.Path.mkdir") as mock_mkdir:

        mock_list.return_value = dummy_files
        mock_load.side_effect = [dummy_df, dummy_df]
        mock_date.side_effect = [pd.Timestamp("2026-02-23"), pd.Timestamp("2026-02-24")]
        mock_transform.side_effect = lambda df: df

        process_silver_artists("long_term")

        assert mock_load.call_count == 2
        assert mock_transform.call_count == 2
        assert mock_to_parquet.call_count == 2
        assert mock_move.call_count == 2
        assert mock_date.call_count == 2   

def test_process_silver_artists_no_files():

    with patch("spotify_data_pipeline.helpers.artist_helper.list_json_files") as mock_list, \
         patch("spotify_data_pipeline.helpers.artist_helper.load_jsons_to_df") as mock_load, \
         patch("pandas.DataFrame.to_parquet") as mock_to_parquet, \
         patch("spotify_data_pipeline.helpers.artist_helper.move_to_archive") as mock_move:

        mock_list.return_value = []

        process_silver_artists("long_term")

        mock_load.assert_not_called()
        mock_to_parquet.assert_not_called()
        mock_move.assert_not_called()
            