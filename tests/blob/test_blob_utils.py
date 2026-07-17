import io
import pandas as pd
from unittest.mock import patch, MagicMock

from spotify_data_pipeline.helpers.blob_utils import (
    get_blob_service_client, list_blobs, download_json_blob,
    move_blob_to_archive, download_parquet_from_blob, upload_parquet_to_blob,
)

BLOB_UTILS = "spotify_data_pipeline.helpers.blob_utils"


# --- get_blob_service_client -------------------------------------------------

def test_get_blob_service_client_uses_env_connection_string():
    with patch.dict(f"{BLOB_UTILS}.os.environ", {"AZURE_CONNECTION_STRING": "conn-str"}), \
         patch(f"{BLOB_UTILS}.BlobServiceClient.from_connection_string") as mock_from_conn:

        get_blob_service_client()

        mock_from_conn.assert_called_once_with("conn-str")


# --- list_blobs ----------------------------------------------------------

def test_list_blobs_returns_names_with_prefix():
    blob1 = MagicMock(name="top_tracks_short/f1.parquet")
    blob1.name = "top_tracks_short/f1.parquet"
    blob2 = MagicMock(name="top_tracks_short/f2.parquet")
    blob2.name = "top_tracks_short/f2.parquet"

    mock_container_client = MagicMock()
    mock_container_client.list_blobs.return_value = [blob1, blob2]

    mock_client = MagicMock()
    mock_client.get_container_client.return_value = mock_container_client

    with patch(f"{BLOB_UTILS}.get_blob_service_client", return_value=mock_client):
        result = list_blobs("silver", "top_tracks_short")

        assert result == ["top_tracks_short/f1.parquet", "top_tracks_short/f2.parquet"]
        mock_client.get_container_client.assert_called_once_with("silver")
        mock_container_client.list_blobs.assert_called_once_with(name_starts_with="top_tracks_short")

def test_list_blobs_empty_container_returns_empty_list():
    mock_container_client = MagicMock()
    mock_container_client.list_blobs.return_value = []

    mock_client = MagicMock()
    mock_client.get_container_client.return_value = mock_container_client

    with patch(f"{BLOB_UTILS}.get_blob_service_client", return_value=mock_client):
        result = list_blobs("silver", "no_match")
        assert result == []


# --- download_json_blob ----------------------------------------------------

def test_download_json_blob_returns_bytes():
    mock_blob_client = MagicMock()
    mock_blob_client.download_blob.return_value.readall.return_value = b'{"a": 1}'

    mock_client = MagicMock()
    mock_client.get_blob_client.return_value = mock_blob_client

    with patch(f"{BLOB_UTILS}.get_blob_service_client", return_value=mock_client):
        result = download_json_blob("bronze", "top_tracks_short/f1.json")

        assert result == b'{"a": 1}'
        mock_client.get_blob_client.assert_called_once_with(container="bronze", blob="top_tracks_short/f1.json")


# --- move_blob_to_archive ----------------------------------------------------

def test_move_blob_to_archive_copies_and_deletes_original():
    mock_src_client = MagicMock()
    mock_src_client.url = "https://storageacc.blob.core.windows.net/bronze/top_tracks_short/f1.json"

    mock_dest_client = MagicMock()

    mock_client = MagicMock()

    def get_blob_client(container, blob):
        if blob == "top_tracks_short/f1.json":
            return mock_src_client
        if blob == "top_tracks_short/archive/f1.json":
            return mock_dest_client
        raise AssertionError(f"unexpected blob path {blob}")

    mock_client.get_blob_client.side_effect = get_blob_client

    with patch(f"{BLOB_UTILS}.get_blob_service_client", return_value=mock_client):
        move_blob_to_archive("bronze", "top_tracks_short/f1.json")

        mock_dest_client.start_copy_from_url.assert_called_once_with(mock_src_client.url)
        mock_src_client.delete_blob.assert_called_once()

def test_move_blob_to_archive_builds_correct_archive_path():
    mock_client = MagicMock()

    with patch(f"{BLOB_UTILS}.get_blob_service_client", return_value=mock_client):
        move_blob_to_archive("bronze", "top_tracks_short/nested/f1.json")

        called_blob_paths = [call.kwargs["blob"] for call in mock_client.get_blob_client.call_args_list]
        assert "top_tracks_short/nested/archive/f1.json" in called_blob_paths


# --- download_parquet_from_blob ----------------------------------------------

def test_download_parquet_from_blob_returns_dataframe():
    df = pd.DataFrame({"a": [1, 2]})
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    raw_bytes = buffer.getvalue()

    mock_blob_client = MagicMock()
    mock_blob_client.download_blob.return_value.readall.return_value = raw_bytes

    mock_client = MagicMock()
    mock_client.get_blob_client.return_value = mock_blob_client

    with patch(f"{BLOB_UTILS}.get_blob_service_client", return_value=mock_client):
        result = download_parquet_from_blob("silver", "top_tracks_short/f1.parquet")

        assert result["a"].tolist() == [1, 2]

def test_download_parquet_from_blob_returns_empty_df_on_error():
    mock_blob_client = MagicMock()
    mock_blob_client.download_blob.side_effect = Exception("blob not found")

    mock_client = MagicMock()
    mock_client.get_blob_client.return_value = mock_blob_client

    with patch(f"{BLOB_UTILS}.get_blob_service_client", return_value=mock_client):
        result = download_parquet_from_blob("silver", "missing.parquet")

        assert isinstance(result, pd.DataFrame)
        assert result.empty

def test_download_parquet_from_blob_returns_empty_df_on_corrupt_parquet():
    mock_blob_client = MagicMock()
    mock_blob_client.download_blob.return_value.readall.return_value = b"not a parquet file"

    mock_client = MagicMock()
    mock_client.get_blob_client.return_value = mock_blob_client

    with patch(f"{BLOB_UTILS}.get_blob_service_client", return_value=mock_client):
        result = download_parquet_from_blob("silver", "corrupt.parquet")

        assert result.empty


# --- upload_parquet_to_blob ----------------------------------------------

def test_upload_parquet_to_blob_calls_upload_with_correct_path():
    df = pd.DataFrame({"a": [1, 2]})

    mock_blob_client = MagicMock()
    mock_client = MagicMock()
    mock_client.get_blob_client.return_value = mock_blob_client

    with patch(f"{BLOB_UTILS}.get_blob_service_client", return_value=mock_client):
        upload_parquet_to_blob(df, "gold", "top_tracks/short/top_tracks_2026-07-17.parquet")

        mock_client.get_blob_client.assert_called_once_with(
            container="gold", blob="top_tracks/short/top_tracks_2026-07-17.parquet"
        )
        mock_blob_client.upload_blob.assert_called_once()
        _, kwargs = mock_blob_client.upload_blob.call_args
        assert kwargs.get("overwrite") is True

def test_upload_parquet_to_blob_writes_readable_parquet_buffer():
    df = pd.DataFrame({"a": [1, 2, 3]})

    mock_blob_client = MagicMock()
    mock_client = MagicMock()
    mock_client.get_blob_client.return_value = mock_blob_client

    with patch(f"{BLOB_UTILS}.get_blob_service_client", return_value=mock_client):
        upload_parquet_to_blob(df, "gold", "recent_tracks/recent_tracks_2026-07-17.parquet")

        uploaded_buffer = mock_blob_client.upload_blob.call_args[0][0]
        uploaded_buffer.seek(0)
        result_df = pd.read_parquet(uploaded_buffer)
        assert result_df["a"].tolist() == [1, 2, 3]