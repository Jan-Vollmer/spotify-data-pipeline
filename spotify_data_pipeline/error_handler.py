import logging
from requests import Response

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

class SpotifyAPIError(Exception):
    def __init__(self, status_code, message, details=None):
        self.status_code = status_code
        self.message = message
        self.details = details
        super().__init__(f"{status_code}: {message} | {details}")


def handle_http_error(response: Response):
    if 200 <= response.status_code < 300:
        return

    try:
        error_json = response.json()
        message = error_json.get("error", {}).get("message", "Unknown error")
    except ValueError:
        error_json = None
        message = response.text or "No JSON body returned"

    status = response.status_code

    if status == 401:
        raise SpotifyAPIError(status, "Unauthorized (token expired?)", error_json)

    if status == 403:
        raise SpotifyAPIError(status, "Forbidden (insufficient permissions)", error_json)

    if status == 429:
        retry_after = response.headers.get("Retry-After", "unknown")
        raise SpotifyAPIError(status, f"Rate limited, retry after: {retry_after}s", error_json)

    if 500 <= status <= 599:
        raise SpotifyAPIError(status, "Spotify server error. Try again later.", error_json)

    raise SpotifyAPIError(status, message, error_json)