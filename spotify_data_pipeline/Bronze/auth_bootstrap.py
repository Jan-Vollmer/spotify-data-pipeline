import os
import requests
from dotenv import load_dotenv
from urllib.parse import urlencode, urlparse, parse_qs
from http.server import HTTPServer, BaseHTTPRequestHandler
import time
import threading
from azure.storage.blob import BlobServiceClient

load_dotenv()

CLIENT_ID = os.getenv("SPOTIPY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIPY_CLIENT_SECRET")
REDIRECT_URI = os.getenv("SPOTIPY_REDIRECT_URI")

AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING")
AZURE_CONTAINER = os.getenv("AZURE_CONTAINER")
TOKEN_BLOB_PREFIX = "tokens/"


def _blob_client(blob_name: str):
    service = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    return service.get_blob_client(container=AZURE_CONTAINER, blob=blob_name)


def save_refresh_token(token: str, scope: str):
    blob_name = f"{TOKEN_BLOB_PREFIX}refresh_token_{scope.replace(' ', '_').replace('-', '_')}.txt"
    _blob_client(blob_name).upload_blob(token, overwrite=True)


def get_auth_url(scope: str) -> str:
    params = {
        "client_id": CLIENT_ID,
        "response_type": "code",
        "redirect_uri": REDIRECT_URI,
        "scope": scope
    }
    return f"https://accounts.spotify.com/authorize?{urlencode(params)}"


def request_token_with_code(code: str) -> dict:
    token_url = "https://accounts.spotify.com/api/token"
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": REDIRECT_URI,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }
    resp = requests.post(token_url, data=data)
    resp.raise_for_status()
    return resp.json()


def get_code_via_local_server(scope: str) -> str:
    class CodeHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            self.server.code = query.get("code", [None])[0]

            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(b"Authorization erfolgreich! Du kannst das Fenster schliessen.")

        def log_message(self, format, *args):
            return

    parsed_redirect = urlparse(REDIRECT_URI)
    host = parsed_redirect.hostname
    port = parsed_redirect.port

    server = HTTPServer((host, port), CodeHandler)

    thread = threading.Thread(target=server.serve_forever)
    thread.start()

    print("Öffne den folgenden Link im Browser, um Spotify zu autorisieren:")
    print(get_auth_url(scope))

    while not hasattr(server, "code") or server.code is None:
        time.sleep(0.1)

    code = server.code
    server.shutdown()
    return code


def bootstrap(scope: str):
    code = get_code_via_local_server(scope)
    tokens = request_token_with_code(code)

    refresh_token = tokens.get("refresh_token")
    if not refresh_token:
        raise RuntimeError("Kein Refresh Token in der Antwort enthalten.")

    save_refresh_token(refresh_token, scope)
    print("Refresh Token gespeichert.")


if __name__ == "__main__":
    for scope in ["user-read-recently-played", "user-top-read"]:
        print(f"\nBootstrapping scope: {scope}")
        bootstrap(scope)