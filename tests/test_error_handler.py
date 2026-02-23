from unittest.mock import patch, Mock
import pytest
from spotify_data_pipeline.Bronze.error_handler import handle_http_error, AuthError, SpotifyAPIError, RetryableError, request_with_retry

def test_handle_http_error_200():
    resp = Mock()
    resp.status_code = 200
    handle_http_error(resp)

def test_handle_http_error_401():
    resp = Mock()
    resp.status_code = 401
    with pytest.raises(AuthError):
        handle_http_error(resp)

def test_handle_http_error_403():
    resp = Mock()
    resp.status_code = 403
    resp.json.return_value = {"error":{"message":"Forbidden"}}
    with pytest.raises(SpotifyAPIError) as e:
        handle_http_error(resp)
    assert "Forbidden" in str(e.value)

def test_handle_http_error_429():
    resp = Mock()
    resp.status_code = 429
    resp.headers = {"Retry-After":"5"}
    with pytest.raises(RetryableError) as e:
        handle_http_error(resp)
    assert e.value.wait == 5

def test_handle_http_error_500():
    resp = Mock()
    resp.status_code = 500
    with pytest.raises(RetryableError):
        handle_http_error(resp)

def test_handle_http_error_other():
    resp = Mock()
    resp.status_code = 404
    resp.text = "Not Found"
    resp.json.side_effect = ValueError("No JSON body")
    with pytest.raises(SpotifyAPIError) as e:
        handle_http_error(resp)
    assert "Not Found" in str(e.value)

import pytest
from spotify_data_pipeline.Bronze.error_handler import request_with_retry, RetryableError

def test_request_with_retry_integration_success():
    resp = request_with_retry("https://httpbin.org/status/200")
    assert resp.status_code == 200

def test_request_with_retry_500():
    with pytest.raises(RuntimeError):
        request_with_retry("https://httpbin.org/status/500", max_retries=1)