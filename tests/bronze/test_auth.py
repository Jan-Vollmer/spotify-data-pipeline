import pytest
import spotify_data_pipeline.Bronze.auth as auth
from spotify_data_pipeline.Bronze.auth import (
    load_refresh_token,
    refresh_access_token,
    get_access_token,
)


class MockResponse:
    def __init__(self, json_data):
        self._json = json_data

    def raise_for_status(self):
        pass

    def json(self):
        return self._json


def test_refresh_access_token(mocker):
    mocker.patch(
        "spotify_data_pipeline.Bronze.auth.requests.post",
        return_value=MockResponse({"access_token": "new-token"})
    )
    token = refresh_access_token("refresh-token")
    assert token == "new-token"


def test_load_refresh_token_returns_token(mocker):
    mock_blob = mocker.MagicMock()
    mock_blob.download_blob.return_value.readall.return_value = b"my-refresh-token"
    mocker.patch("spotify_data_pipeline.Bronze.auth._blob_client", return_value=mock_blob)

    token = load_refresh_token("user-top-read")
    assert token == "my-refresh-token"


def test_load_refresh_token_returns_none_on_error(mocker):
    mocker.patch(
        "spotify_data_pipeline.Bronze.auth._blob_client",
        side_effect=Exception("Blob not found")
    )
    token = load_refresh_token("user-top-read")
    assert token is None


def test_get_access_token_raises_if_no_refresh_token(mocker):
    mocker.patch("spotify_data_pipeline.Bronze.auth.load_refresh_token", return_value=None)

    with pytest.raises(RuntimeError, match="Kein Refresh Token"):
        get_access_token("user-top-read")


def test_get_access_token_returns_access_token(mocker):
    mocker.patch("spotify_data_pipeline.Bronze.auth.load_refresh_token", return_value="refresh123")
    mocker.patch("spotify_data_pipeline.Bronze.auth.refresh_access_token", return_value="access123")

    token = get_access_token("user-top-read")
    assert token == "access123"


def test_load_refresh_token_scope_sanitization(mocker):
    mock_blob = mocker.MagicMock()
    mock_blob.download_blob.return_value.readall.return_value = b"token"
    blob_mock = mocker.patch("spotify_data_pipeline.Bronze.auth._blob_client", return_value=mock_blob)

    load_refresh_token("user-read-recently-played")

    call_arg = blob_mock.call_args[0][0]
    assert "user_read_recently_played" in call_arg
    assert "-" not in call_arg.split("tokens/")[1]