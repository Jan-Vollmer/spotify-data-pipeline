from spotify_data_pipeline.Bronze.error_handler import handle_http_error, request_with_retry

def get_recent_tracks(access_token: str, limit: int = None, after: int = None) -> list:

    headers = {"Authorization": f"Bearer {access_token}"}

    if limit is None:
        limit = 10

    params = {"limit": limit}
    if after is not None:
        params["after"] = after  # in ms

    url = "https://api.spotify.com/v1/me/player/recently-played"
    resp = request_with_retry(url, headers=headers, params=params)

    return resp.json().get("items", [])