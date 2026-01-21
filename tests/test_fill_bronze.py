from unittest.mock import patch
from datetime import datetime

import spotify_data_pipeline.fill_bronze as fb

TOP_TRACKS = [
    {"name": "Track 1", "artists": [{"name": "Artist A"}]},
]

TOP_ARTISTS = [
    {"name": "Artist A", "genres": ["rock"]},
]

RECENT_TRACKS = [
    {"track": {"name": "Track X", "artists": [{"name": "Artist Z"}]}},
]

@patch("spotify_data_pipeline.fill_bronze.write_bronze_batch")
@patch("spotify_data_pipeline.fill_bronze.get_recent_tracks")
@patch("spotify_data_pipeline.fill_bronze.get_top_artists")
@patch("spotify_data_pipeline.fill_bronze.get_top_tracks")
@patch("spotify_data_pipeline.fill_bronze.get_or_refresh_token")
def test_fill_bronze_happy_path(
    mock_get_token,
    mock_get_top_tracks,
    mock_get_top_artists,
    mock_get_recent_tracks,
    mock_write_bronze,
):

    mock_get_token.side_effect = ["token_top", "token_recent"]
    mock_get_top_tracks.return_value = TOP_TRACKS
    mock_get_top_artists.return_value = TOP_ARTISTS
    mock_get_recent_tracks.return_value = RECENT_TRACKS

    result = fb.fill_bronze(limit_top=10, limit_recent=5, time_range="short_term")

    assert result == (TOP_TRACKS, TOP_ARTISTS, RECENT_TRACKS)

    mock_get_token.assert_any_call("user-top-read")
    mock_get_token.assert_any_call("user-read-recently-played")

    mock_get_top_tracks.assert_called_once_with(
        "token_top", limit=10, time_range="short_term"
    )
    mock_get_top_artists.assert_called_once_with(
        "token_top", limit=10, time_range="short_term"
    )
    mock_get_recent_tracks.assert_called_once_with(
        "token_recent", limit=5
    )

    assert mock_write_bronze.call_count == 3

@patch("spotify_data_pipeline.fill_bronze.write_bronze_batch")
@patch("spotify_data_pipeline.fill_bronze.get_recent_tracks", return_value=RECENT_TRACKS)
@patch("spotify_data_pipeline.fill_bronze.get_top_artists", return_value=TOP_ARTISTS)
@patch("spotify_data_pipeline.fill_bronze.get_top_tracks", return_value=TOP_TRACKS)
@patch("spotify_data_pipeline.fill_bronze.get_or_refresh_token", return_value="token")
def test_fill_bronze_default_time_range(
    mock_token,
    mock_tracks,
    mock_artists,
    mock_recent,
    mock_write,
):
    fb.fill_bronze()

    _, kwargs = mock_write.call_args_list[0]

    assert kwargs["subdir"] == "time_range=default"