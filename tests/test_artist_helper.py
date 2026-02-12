import pandas as pd
from unittest.mock import patch
from spotify_data_pipeline.helpers.artist_helper import process_silver_artists

def test_process_silver_artists_calls_parquet(tmp_path):
    mock_df = pd.DataFrame({"artist": ["A1", "A2"]})
    mock_snapshot_date = pd.Timestamp("2026-02-12T00:00:00")

    with patch("spotify_data_pipeline.helpers.artist_helper.list_json_files", return_value=["file1.json"]), \
         patch("spotify_data_pipeline.helpers.artist_helper.load_jsons_to_df", return_value=mock_df), \
         patch("spotify_data_pipeline.helpers.artist_helper.extract_date_from_filename", return_value=mock_snapshot_date), \
         patch("spotify_data_pipeline.helpers.artist_helper.transform_silver", return_value=mock_df), \
         patch("pandas.DataFrame.to_parquet") as mock_parquet, \
         patch("spotify_data_pipeline.helpers.artist_helper.move_to_archive") as mock_move:

        process_silver_artists("short_term")

        assert mock_parquet.called
        parquet_path = mock_parquet.call_args[0][0]

        assert "top_artists_2026-02-12T00-00-00.parquet" in str(parquet_path)
        mock_move.assert_called_once()
