import requests
import time
from .error_handler import handle_http_error

def get_last_played(access_token: str, limit: int = None, after: int = None) -> list:

    headers = {"Authorization": f"Bearer {access_token}"}

    if limit is None:
        limit = 10

    params = {"limit": limit}
    if after is not None:
        params["after"] = after  # in ms

    url = "https://api.spotify.com/v1/me/player/recently-played"
    resp = requests.get(url, headers=headers, params=params)
    handle_http_error(resp)

    return resp.json().get("items", [])