from unittest.mock import patch, Mock
import pytest
import requests
from spotify_data_pipeline.Bronze.error_handler import (
    request_with_retry,
    RetryableError,
    AuthError,
    SpotifyAPIError,
)

@patch("spotify_data_pipeline.Bronze.error_handler.requests.get")
@patch("spotify_data_pipeline.Bronze.error_handler.sleep")
@patch("spotify_data_pipeline.Bronze.error_handler.refresh_access_token")
def test_request_with_retry_success(mock_refresh, mock_sleep, mock_get):
    # Mock Response für erfolgreichen Request
    mock_response = Mock()
    mock_response.status_code = 200
    mock_get.return_value = mock_response

    # Aufruf
    resp = request_with_retry("https://api.spotify.com/v1/me/player/recently-played")

    # Assertions
    assert resp == mock_response
    mock_get.assert_called_once()
    mock_sleep.assert_not_called()
    mock_refresh.assert_not_called()

@patch("spotify_data_pipeline.Bronze.error_handler.requests.get")
@patch("spotify_data_pipeline.Bronze.error_handler.sleep")
@patch("spotify_data_pipeline.Bronze.error_handler.refresh_access_token")
def test_request_with_retry_retryable_error(mock_refresh, mock_sleep, mock_get):
    mock_response = Mock()
    mock_response.status_code = 429
    mock_response.headers = {"Retry-After": "1"}
    mock_get.side_effect = [mock_response, Mock(status_code=200)]

    resp = request_with_retry("https://api.spotify.com/v1/me/player/recently-played")

    assert resp.status_code == 200
    assert mock_get.call_count == 2
    mock_sleep.assert_called_once()

@patch("spotify_data_pipeline.Bronze.error_handler.requests.get")
@patch("spotify_data_pipeline.Bronze.error_handler.sleep")
@patch("spotify_data_pipeline.Bronze.error_handler.refresh_access_token")
def test_request_with_retry_auth_error(mock_refresh, mock_sleep, mock_get):
    mock_response = Mock()
    mock_response.status_code = 401
    mock_get.side_effect = [mock_response, Mock(status_code=200)]

    resp = request_with_retry("https://api.spotify.com/v1/me/player/recently-played")

    assert resp.status_code == 200
    assert mock_get.call_count == 2
    mock_refresh.assert_called_once()

@patch("spotify_data_pipeline.Bronze.error_handler.requests.get")
def test_request_with_retry_spotify_api_error(mock_get):
    mock_response = Mock()
    mock_response.status_code = 403
    mock_get.return_value = mock_response

    with pytest.raises(SpotifyAPIError):
        request_with_retry("https://api.spotify.com/v1/me/player/recently-played")

@patch("spotify_data_pipeline.Bronze.error_handler.requests.get")
def test_request_with_retry_max_retries(mock_get):
    mock_response = Mock()
    mock_response.status_code = 500
    mock_get.return_value = mock_response

    with pytest.raises(RuntimeError, match="Unreachable"):
        request_with_retry("https://api.spotify.com/v1/me/player/recently-played", max_retries=1)
