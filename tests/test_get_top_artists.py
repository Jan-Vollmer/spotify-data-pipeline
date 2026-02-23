from unittest.mock import patch, Mock
import pytest
from spotify_data_pipeline.Bronze.get_top_artists import get_top_artists

mock_response_data = {"items": [{"artist": "artist1"}, {"artist": "artist2"}]}

def test_get_top_artists_all_params():
    with patch("spotify_data_pipeline.Bronze.get_top_artists.request_with_retry") as mock_req:
        mock_resp = Mock()
        mock_resp.json.return_value = mock_response_data
        mock_req.return_value = mock_resp

        result = get_top_artists("fake_token", limit=5, time_range="long_term")
        mock_req.assert_called_once_with(
            "https://api.spotify.com/v1/me/top/artists",
            headers={"Authorization": "Bearer fake_token"},
            params={"limit": 5, "time_range": "long_term"}
        )
        assert result == mock_response_data["items"]

def test_get_top_artists_limit_missing():
    with patch("spotify_data_pipeline.Bronze.get_top_artists.request_with_retry") as mock_req:
        mock_resp = Mock()
        mock_resp.json.return_value = mock_response_data
        mock_req.return_value = mock_resp

        result = get_top_artists("fake_token", time_range="long_term")
        mock_req.assert_called_once_with(
            "https://api.spotify.com/v1/me/top/artists",
            headers={"Authorization": "Bearer fake_token"},
            params={"limit": 10, "time_range": "long_term"}  # default limit=10
        )
        assert result == mock_response_data["items"]          