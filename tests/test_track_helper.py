from unittest.mock import patch, Mock
import pandas as pd
from pathlib import Path
from spotify_data_pipeline.helpers.track_helper import process_silver_tracks

def test_process_silver_tracks_unit():
    dummy_files = [Path("file_20260223.json"), Path("file_20260224.json")]
    dummy_df = pd.DataFrame({"track": ["t1", "t2"]})

    with patch("spotify_data_pipeline.helpers.track_helper.list_json_files") as mock_list, \
         patch("spotify_data_pipeline.helpers.track_helper.load_jsons_to_df") as mock_load, \
         patch("spotify_data_pipeline.helpers.track_helper.extract_date_from_filename") as mock_date, \
         patch("spotify_data_pipeline.helpers.track_helper.transform_silver_track") as mock_transform, \
         patch("pandas.DataFrame.to_parquet") as mock_to_parquet, \
         patch("spotify_data_pipeline.helpers.track_helper.move_to_archive") as mock_move, \
         patch("pathlib.Path.mkdir") as mock_mkdir:

        mock_list.return_value = dummy_files
        mock_load.side_effect = [dummy_df, dummy_df]
        mock_date.side_effect = [pd.Timestamp("2026-02-23"), pd.Timestamp("2026-02-24")]
        mock_transform.side_effect = lambda df: df 

        process_silver_tracks("long_term")

        assert mock_list.call_count == 1
        assert mock_load.call_count == 2
        assert mock_transform.call_count == 2
        assert mock_to_parquet.call_count == 2
        assert mock_move.call_count == 2
        assert mock_date.call_count == 2

def test_process_silver_tracks_no_files():
    with patch("spotify_data_pipeline.helpers.track_helper.list_json_files") as mock_list, \
         patch("spotify_data_pipeline.helpers.track_helper.load_jsons_to_df") as mock_load, \
         patch("pandas.DataFrame.to_parquet") as mock_to_parquet, \
         patch("spotify_data_pipeline.helpers.track_helper.move_to_archive") as mock_move:

        mock_list.return_value = [] 

        process_silver_tracks("long_term")

        mock_load.assert_not_called()
        mock_to_parquet.assert_not_called()
        mock_move.assert_not_called()        