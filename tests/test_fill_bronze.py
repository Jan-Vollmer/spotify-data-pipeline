from unittest.mock import patch, Mock, ANY
from spotify_data_pipeline.Bronze.fill_bronze import fill_bronze

def test_fill_bronze_calls_all(tmp_path):
    with patch("spotify_data_pipeline.Bronze.fill_bronze.get_or_refresh_token") as mock_token, \
         patch("spotify_data_pipeline.Bronze.fill_bronze.get_top_tracks") as mock_top_tracks, \
         patch("spotify_data_pipeline.Bronze.fill_bronze.get_top_artists") as mock_top_artists, \
         patch("spotify_data_pipeline.Bronze.fill_bronze.get_recent_tracks") as mock_recent_tracks, \
         patch("spotify_data_pipeline.Bronze.fill_bronze.fetch_and_write") as mock_fetch_write, \
         patch("spotify_data_pipeline.Bronze.fill_bronze.write_bronze_batch") as mock_write_batch, \
         patch("pathlib.Path.mkdir") as mock_mkdir:

        mock_token.return_value = "fake_token"
        mock_top_tracks.return_value = [{"track":"t1"}]
        mock_top_artists.return_value = [{"artist":"a1"}]
        mock_recent_tracks.return_value = [{"track":"song"}]

        fill_bronze(limit_top=5, limit_recent=3)

        assert mock_token.call_count == 2
        mock_token.assert_any_call("user-top-read")
        mock_token.assert_any_call("user-read-recently-played")

        assert mock_fetch_write.call_count == 2

        mock_write_batch.assert_called_once()

        mock_mkdir.assert_called_once()