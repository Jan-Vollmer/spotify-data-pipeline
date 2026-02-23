from unittest.mock import Mock
import pytest
from spotify_data_pipeline.Bronze.error_handler import handle_http_error, AuthError, SpotifyAPIError, RetryableError

def test_handle_http_error_200():
    response = Mock()
    response.json = Mock(return_value={})
    response.status_code = 200
    handle_http_error(response)

def test_handle_http_error_401():
    response = Mock()
    response.status_code = 401
    with pytest.raises(AuthError):
        handle_http_error(response)

def test_handle_http_error_403():
    response = Mock()
    response.status_code = 403
    response.json.return_value = {"error": {"message": "Forbidden"}}
    with pytest.raises(SpotifyAPIError) as excinfo:
        handle_http_error(response)
    assert "Forbidden" in str(excinfo.value)

def test_handle_http_error_429():
    response = Mock()
    response.status_code = 429
    response.headers = {"Retry-After": "5"}
    with pytest.raises(RetryableError) as excinfo:
        handle_http_error(response)
    assert excinfo.value.wait == 5

def test_handle_http_error_500():
    response = Mock()
    response.status_code = 500
    with pytest.raises(RetryableError):
        handle_http_error(response)

def test_handle_http_error_other():
    response = Mock()
    response.status_code = 404
    response.text = "Not Found"
    with pytest.raises(SpotifyAPIError) as excinfo:
        handle_http_error(response)
    assert "Not Found" in str(excinfo.value)
