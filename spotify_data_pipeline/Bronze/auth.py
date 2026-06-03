import os

import requests
import logging
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

load_dotenv()

CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")

AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING")
AZURE_CONTAINER = os.getenv("AZURE_CONTAINER")
TOKEN_BLOB_PREFIX = "tokens/"


def _blob_client(blob_name: str):
    service = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    return service.get_blob_client(container=AZURE_CONTAINER, blob=blob_name)


def load_refresh_token(scope: str) -> str:
    blob_name = f"{TOKEN_BLOB_PREFIX}refresh_token_{scope.replace(' ', '_').replace('-', '_')}.txt"
    logging.info(f"Suche Token: container={AZURE_CONTAINER}, blob={blob_name}")
    logging.info(f"Connection String vorhanden: {bool(AZURE_CONNECTION_STRING)}")
    try:
        return _blob_client(blob_name).download_blob().readall().decode().strip()
    except Exception:
        return None


def refresh_access_token(refresh_token: str) -> str:
    resp = requests.post(
        "https://accounts.spotify.com/api/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        },
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def get_access_token(scope: str) -> str:
    refresh_token = load_refresh_token(scope)
    if not refresh_token:
        raise RuntimeError(
            f"Kein Refresh Token für Scope '{scope}' im Blob gefunden. "
            "Führe auth_bootstrap.py lokal aus."
        )
    return refresh_access_token(refresh_token)