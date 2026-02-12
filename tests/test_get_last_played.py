import pytest
from spotify_data_pipeline.Bronze.get_recent_tracks import get_recent_tracks

class MockResponse:
    def __init__(self, json_data, status_code=200):
        self._json_data = json_data
        self.status_code = status_code

    def json(self):
        return self._json_data

def test_get_recent_tracks(mocker):
    mock_get = mocker.patch("spotify_data_pipeline.Bronze.get_recent_tracks.requests.get")
    
    mock_get.return_value = MockResponse({
        "items": [{"track": {"name": "Song1"}}, {"track": {"name": "Song2"}}]
    })

    result = get_recent_tracks("fake-token", limit=2, after=12345)

    mock_get.assert_called_once()
    called_url = mock_get.call_args[0][0]
    called_headers = mock_get.call_args[1]["headers"]
    called_params = mock_get.call_args[1]["params"]

    assert called_url == "https://api.spotify.com/v1/me/player/recently-played"
    assert called_headers["Authorization"] == "Bearer fake-token"
    assert called_params["limit"] == 2
    assert called_params["after"] == 12345

    assert len(result) == 2
    assert result[0]["track"]["name"] == "Song1"