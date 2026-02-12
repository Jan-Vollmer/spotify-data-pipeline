import os
import requests
from dotenv import load_dotenv
from urllib.parse import urlencode, urlparse, parse_qs
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading

load_dotenv()

CLIENT_ID = os.getenv("SPOTIPY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIPY_CLIENT_SECRET")
REDIRECT_URI = os.getenv("SPOTIPY_REDIRECT_URI")  # z.B. http://127.0.0.1:8000/callback

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TOKEN_DIR = os.path.join(BASE_DIR, "tokens")
os.makedirs(TOKEN_DIR, exist_ok=True)

def get_refresh_token_file(scope: str) -> str:
    safe_scope = scope.replace(" ", "_").replace("-", "_")
    return os.path.join(TOKEN_DIR, f"refresh_token_{safe_scope}.txt")


def save_refresh_token(token: str, scope: str):
    file_path = get_refresh_token_file(scope)
    with open(file_path, "w") as f:
        f.write(token)


def load_refresh_token(scope: str) -> str:
    file_path = get_refresh_token_file(scope)
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            return f.read().strip()
    return None


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


def refresh_access_token(refresh_token: str) -> str:
    url = "https://accounts.spotify.com/api/token"
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }
    resp = requests.post(url, data=data)
    resp.raise_for_status()
    return resp.json()["access_token"]


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
            return  # keine Logs auf Konsole

    parsed_redirect = urlparse(REDIRECT_URI)
    host = parsed_redirect.hostname
    port = parsed_redirect.port

    server = HTTPServer((host, port), CodeHandler)

    thread = threading.Thread(target=server.serve_forever)
    thread.start()

    print("Öffne den folgenden Link im Browser, um Spotify zu autorisieren:")
    print(get_auth_url(scope))

    while not hasattr(server, "code") or server.code is None:
        pass

    code = server.code
    server.shutdown()
    thread.join()
    return code


def get_or_refresh_token(scope: str) -> str:
    refresh_token = load_refresh_token(scope)
    if refresh_token:
        return refresh_access_token(refresh_token)

    code = get_code_via_local_server(scope)
    token_data = request_token_with_code(code)
    save_refresh_token(token_data["refresh_token"], scope)
    return token_data["access_token"]