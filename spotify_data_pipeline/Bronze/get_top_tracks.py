import requests
from spotify_data_pipeline.Bronze.error_handler import handle_http_error

def get_top_tracks(access_token: str, limit: int = None, time_range: str = None) -> list:

    headers = {"Authorization": f"Bearer {access_token}"}

    if limit is None:
        limit = 10

    params = {
        "limit": limit,
        "time_range": time_range
    }

    url = "https://api.spotify.com/v1/me/top/tracks"
    resp = requests.get(url, headers=headers, params=params)
    handle_http_error(resp)
    
    return resp.json().get("items", [])