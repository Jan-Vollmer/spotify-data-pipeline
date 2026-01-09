import pytest
import spotify_data_pipeline.auth as auth

from spotify_data_pipeline.auth import (
    get_refresh_token_file,
    get_auth_url,
    save_refresh_token,
    load_refresh_token,
    refresh_access_token,
    request_token_with_code,
    get_or_refresh_token,
)

def test_get_refresh_token_file_sanitizes_scope():
    scope = "user-read-recently-played"
    path = get_refresh_token_file(scope)

    assert "user_read_recently_played" in path
    assert path.endswith(".txt")


def test_get_auth_url_contains_required_params():
    scope = "user-read-recently-played"
    url = get_auth_url(scope)

    assert "accounts.spotify.com/authorize" in url
    assert "client_id=" in url
    assert "redirect_uri=" in url
    assert f"scope={scope}" in url    

def test_save_and_load_refresh_token(tmp_path, monkeypatch):
    monkeypatch.setattr(auth, "TOKEN_DIR", tmp_path)

    scope = "test-scope"
    token = "my-refresh-token"

    save_refresh_token(token, scope)
    loaded = load_refresh_token(scope)

    assert loaded == token

class MockResponse:
    def __init__(self, json_data):
        self._json = json_data

    def raise_for_status(self):
        pass

    def json(self):
        return self._json    
    
def test_request_token_with_code(mocker):
    fake_response = {
        "access_token": "access123",
        "refresh_token": "refresh123"
    }

    mocker.patch(
        "spotify_data_pipeline.auth.requests.post",
        return_value=MockResponse(fake_response)
    )

    result = request_token_with_code("dummy-code")

    assert result["access_token"] == "access123"
    assert result["refresh_token"] == "refresh123"

def test_refresh_access_token(mocker):
    mocker.patch(
        "spotify_data_pipeline.auth.requests.post",
        return_value=MockResponse({"access_token": "new-token"})
    )

    token = refresh_access_token("refresh-token")

    assert token == "new-token"

def test_get_or_refresh_token_uses_refresh_token(mocker):
    mocker.patch("spotify_data_pipeline.auth.load_refresh_token", return_value="refresh123")
    mocker.patch("spotify_data_pipeline.auth.refresh_access_token", return_value="access123")

    token = get_or_refresh_token("scope")

    assert token == "access123"

def test_get_or_refresh_token_full_flow(mocker):
    mocker.patch("spotify_data_pipeline.auth.load_refresh_token", return_value=None)
    mocker.patch("spotify_data_pipeline.auth.get_code_via_local_server", return_value="code123")
    mocker.patch(
        "spotify_data_pipeline.auth.request_token_with_code",
        return_value={
            "access_token": "access123",
            "refresh_token": "refresh123"
        }
    )
    mocker.patch("spotify_data_pipeline.auth.save_refresh_token")

    token = get_or_refresh_token("scope")

    assert token == "access123"