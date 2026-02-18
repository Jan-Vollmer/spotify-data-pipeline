from spotify_data_pipeline.Bronze.error_handler import handle_http_error, request_with_retry

def get_top_artists(access_token: str, limit: int = None, time_range: str = None) -> list:

    headers = {"Authorization": f"Bearer {access_token}"}

    if limit is None:
        limit = 10

    params = {
        "limit": limit,
        "time_range": time_range
    }

    url = f"https://api.spotify.com/v1/me/top/artists"
    resp = request_with_retry(url, headers=headers, params=params)

    return resp.json()["items"]

