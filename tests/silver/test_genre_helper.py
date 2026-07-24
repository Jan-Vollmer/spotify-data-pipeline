import pandas as pd
from unittest.mock import patch
from spotify_data_pipeline.helpers.genre_helper import build_initial_artist_genre_dim, get_missing_artist_ids
from unittest.mock import patch, MagicMock
from spotify_data_pipeline.helpers.genre_helper import get_artist_genres

def test_build_initial_artist_genre_dim(monkeypatch):
    df_short = pd.DataFrame({"id": ["a1"], "name": ["Artist1"], "genres": [["pop"]]})
    df_medium = pd.DataFrame({"id": ["a1"], "name": ["Artist1"], "genres": [["pop"]]})
    df_long = pd.DataFrame()

    with patch("spotify_data_pipeline.helpers.genre_helper.load_silver",
               side_effect=[df_short, df_medium, df_long]), \
         patch("spotify_data_pipeline.helpers.genre_helper.upload_parquet_to_blob") as mock_upload:

        result = build_initial_artist_genre_dim()

        assert len(result) == 1
        assert result.iloc[0]["artist_id"] == "a1"
        mock_upload.assert_called_once()

def test_get_missing_artist_ids_excludes_known():
    df_recent = pd.DataFrame({"artist_ids": [("a1", "a2")]})
    df_top_short = pd.DataFrame({"artist_ids": [("a2", "a3")]})
    df_top_medium = pd.DataFrame()
    df_top_long = pd.DataFrame()
    existing_dim = pd.DataFrame({"artist_id": ["a1"]})

    with patch("spotify_data_pipeline.helpers.genre_helper.load_all_recent_tracks_silver",
               return_value=df_recent), \
         patch("spotify_data_pipeline.helpers.genre_helper.load_silver",
               side_effect=[df_top_short, df_top_medium, df_top_long]):

        result = get_missing_artist_ids(existing_dim)

        assert result == {"a2", "a3"}

def test_get_missing_artist_ids_empty_dim_returns_all():
    df_recent = pd.DataFrame({"artist_ids": [("a1",)]})

    with patch("spotify_data_pipeline.helpers.genre_helper.load_all_recent_tracks_silver",
               return_value=df_recent), \
         patch("spotify_data_pipeline.helpers.genre_helper.load_silver",
               return_value=pd.DataFrame()):

        result = get_missing_artist_ids(pd.DataFrame())
        assert result == {"a1"}        



def test_get_artist_genres_calls_correct_endpoint():
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"artists": [{"id": "a1", "name": "Artist1", "genres": ["pop"]}]}

    with patch("spotify_data_pipeline.Bronze.get_artist_genres.request_with_retry",
               return_value=mock_resp) as mock_request:

        result = get_artist_genres("token123", ["a1", "a2"])

        assert result == [{"id": "a1", "name": "Artist1", "genres": ["pop"]}]
        args, kwargs = mock_request.call_args
        assert args[0] == "https://api.spotify.com/v1/artists"
        assert kwargs["params"]["ids"] == "a1,a2"