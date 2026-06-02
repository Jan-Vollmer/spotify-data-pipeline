import json
import pytest
from unittest.mock import MagicMock, patch
from spotify_data_pipeline.helpers.bronze_helper import write_bronze_batch


def test_write_bronze_batch_uploads_correct_blob_name():
    with patch("spotify_data_pipeline.helpers.bronze_helper.BlobServiceClient") as mock_service:
        mock_blob_client = MagicMock()
        mock_service.from_connection_string.return_value.get_blob_client.return_value = mock_blob_client

        write_bronze_batch(
            entity="recent_tracks",
            payload=[{"track": "song"}],
            downloaded_at="2026-01-01T00-00-00"
        )

        call_kwargs = mock_service.from_connection_string.return_value.get_blob_client.call_args[1]
        assert call_kwargs["blob"] == "bronze/recent_tracks/recent_tracks_2026-01-01T00-00-00.json"
        mock_blob_client.upload_blob.assert_called_once()


def test_write_bronze_batch_uploads_correct_blob_name_with_subdir():
    with patch("spotify_data_pipeline.helpers.bronze_helper.BlobServiceClient") as mock_service:
        mock_blob_client = MagicMock()
        mock_service.from_connection_string.return_value.get_blob_client.return_value = mock_blob_client

        write_bronze_batch(
            entity="top_tracks",
            payload=[{"track": "song"}],
            downloaded_at="2026-01-01T00-00-00",
            subdir="short_term"
        )

        call_kwargs = mock_service.from_connection_string.return_value.get_blob_client.call_args[1]
        assert call_kwargs["blob"] == "bronze/top_tracks/short_term/top_tracks_2026-01-01T00-00-00.json"


def test_write_bronze_batch_uploads_valid_json():
    with patch("spotify_data_pipeline.helpers.bronze_helper.BlobServiceClient") as mock_service:
        mock_blob_client = MagicMock()
        mock_service.from_connection_string.return_value.get_blob_client.return_value = mock_blob_client

        payload = [{"track": "song", "id": 1}]
        write_bronze_batch(
            entity="recent_tracks",
            payload=payload,
            downloaded_at="2026-01-01T00-00-00"
        )

        uploaded_data = mock_blob_client.upload_blob.call_args[0][0]
        assert json.loads(uploaded_data) == payload


def test_write_bronze_batch_raises_on_upload_failure():
    with patch("spotify_data_pipeline.helpers.bronze_helper.BlobServiceClient") as mock_service:
        mock_blob_client = MagicMock()
        mock_blob_client.upload_blob.side_effect = Exception("Upload failed")
        mock_service.from_connection_string.return_value.get_blob_client.return_value = mock_blob_client

        with pytest.raises(Exception, match="Upload failed"):
            write_bronze_batch(
                entity="recent_tracks",
                payload=[],
                downloaded_at="2026-01-01T00-00-00"
            )