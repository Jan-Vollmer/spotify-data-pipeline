import logging
from requests import Response
from .auth import refresh_access_token

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

class RetryableError(Exception):
    def __init__(self, wait=None):
        self.wait = wait

class AuthError(Exception):
    pass

def handle_http_error(response: Response, max_retries=3):
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
        raise AuthError()
    
    if status == 403:
        raise SpotifyAPIError(status, "Forbidden (insufficient permissions)", error_json)

    if status == 429:
        wait = int(response.headers.get("Retry-After", 1))
        raise RetryableError(wait=wait)

    if 500 <= status <= 599:
        raise RetryableError()

    raise SpotifyAPIError(status, message, error_json)

def request_with_retry(url, headers=None, params=None, max_retries=3):
    from time import sleep
    import requests

    for attempt in range(max_retries + 1):
        try:
            resp = requests.get(url, headers=headers, params=params)
            handle_http_error(resp)
            return resp

        except RetryableError as e:
            sleep(e.wait or backoff(attempt))

        except AuthError:
            refresh_access_token()
            continue

        except SpotifyAPIError:
            raise    
    raise RuntimeError("Unreachable")

def backoff(attempt):
    import random
    return min((2 ** attempt) + random.uniform(0, 1), 30)        