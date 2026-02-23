from unittest.mock import patch, Mock
import pytest
from spotify_data_pipeline.Bronze.get_top_tracks import get_top_tracks

mock_response_data = {"items": [{"track": "song1"}, {"track": "song2"}]}

def test_get_top_tracks_all_params():
    with patch("spotify_data_pipeline.Bronze.get_top_tracks.request_with_retry") as mock_req:
        mock_resp = Mock()
        mock_resp.json.return_value = mock_response_data
        mock_req.return_value = mock_resp

        result = get_top_tracks("fake_token", limit=5, time_range="long_term")
        mock_req.assert_called_once_with(
            "https://api.spotify.com/v1/me/top/tracks",
            headers={"Authorization": "Bearer fake_token"},
            params={"limit": 5, "time_range": "long_term"}
        )
        assert result == mock_response_data["items"]

def test_get_top_tracks_limit_missing():
    with patch("spotify_data_pipeline.Bronze.get_top_tracks.request_with_retry") as mock_req:
        mock_resp = Mock()
        mock_resp.json.return_value = mock_response_data
        mock_req.return_value = mock_resp

        result = get_top_tracks("fake_token", time_range="long_term")
        mock_req.assert_called_once_with(
            "https://api.spotify.com/v1/me/top/tracks",
            headers={"Authorization": "Bearer fake_token"},
            params={"limit": 10, "time_range": "long_term"}  # default limit=10
        )
        assert result == mock_response_data["items"]          