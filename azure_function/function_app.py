# function_app.py
import azure.functions as func
from spotify_data_pipeline.Bronze.get_recent_tracks import get_recent_tracks
from spotify_data_pipeline.Bronze.get_top_tracks import get_top_tracks
from spotify_data_pipeline.Bronze.get_top_artists import get_top_artists
from spotify_data_pipeline.helpers.bronze_helper import write_bronze_batch
from spotify_data_pipeline.Bronze.auth import get_access_token
from datetime import datetime
import logging

app = func.FunctionApp()

JOBS = {
    "recent_tracks": {
        "func": get_recent_tracks,
        "scope": "user-read-recently-played",
        "kwargs": {"limit": 50},
    },
    "top_tracks_short": {
        "func": get_top_tracks,
        "scope": "user-top-read",
        "kwargs": {"limit": 50, "time_range": "short_term"},
    },
    "top_tracks_medium": {
        "func": get_top_tracks,
        "scope": "user-top-read",
        "kwargs": {"limit": 50, "time_range": "medium_term"},
    },
    "top_tracks_long": {
        "func": get_top_tracks,
        "scope": "user-top-read",
        "kwargs": {"limit": 50, "time_range": "long_term"},
    },
    "top_artists_short": {
        "func": get_top_artists,
        "scope": "user-top-read",
        "kwargs": {"limit": 50, "time_range": "short_term"},
    },
    "top_artists_medium": {
        "func": get_top_artists,
        "scope": "user-top-read",
        "kwargs": {"limit": 50, "time_range": "medium_term"},
    },
    "top_artists_long": {
        "func": get_top_artists,
        "scope": "user-top-read",
        "kwargs": {"limit": 50, "time_range": "long_term"},
    }
}

def execute(job_name: str):
    job = JOBS[job_name]
    downloaded_at = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    token = get_access_token(job["scope"])
    items = job["func"](token, **job["kwargs"])
    write_bronze_batch(entity=job_name, payload=items, downloaded_at=downloaded_at)
    logging.info(f"[{job_name}] wrote {len(items)} items")


@app.timer_trigger(schedule=0 0 0 * * *, arg_name="timer", run_on_startup=False)
def recent_tracks(timer: func.TimerRequest):
    execute("recent_tracks")

@app.timer_trigger(schedule="0 0 0 1 * *", arg_name="timer", run_on_startup=False)
def top_tracks_short(timer: func.TimerRequest):
    execute("top_tracks_short")

@app.timer_trigger(schedule="0 0 0 1 */6 *", arg_name="timer", run_on_startup=False)
def top_tracks_medium(timer: func.TimerRequest):
    execute("top_tracks_medium")

@app.timer_trigger(schedule="0 0 0 1 1 *", arg_name="timer", run_on_startup=False)
def top_tracks_long(timer: func.TimerRequest):
    execute("top_tracks_long")

@app.timer_trigger(schedule="0 0 0 1 * *", arg_name="timer", run_on_startup=False)
def top_artists_short(timer: func.TimerRequest):
    execute("top_artists_short")

@app.timer_trigger(schedule="0 0 0 1 */6 *", arg_name="timer", run_on_startup=False)
def top_artists_medium(timer: func.TimerRequest):
    execute("top_artists_medium")

@app.timer_trigger(schedule="0 0 0 1 1 *", arg_name="timer", run_on_startup=False)
def top_artists_long(timer: func.TimerRequest):
    execute("top_artists_long")        