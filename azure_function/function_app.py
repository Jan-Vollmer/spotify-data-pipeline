# function_app.py
import azure.functions as func

from spotify_data_pipeline.Bronze.get_recent_tracks import get_recent_tracks
from spotify_data_pipeline.Bronze.get_top_tracks import get_top_tracks
from spotify_data_pipeline.Bronze.get_top_artists import get_top_artists
from spotify_data_pipeline.helpers.bronze_helper import write_bronze_batch
from spotify_data_pipeline.Bronze.auth import get_access_token
from spotify_data_pipeline.Silver.fill_silver_recent_tracks import fill_silver_recent_tracks
from spotify_data_pipeline.helpers.artist_helper import process_silver_artists
from spotify_data_pipeline.helpers.track_helper import process_silver_tracks
from spotify_data_pipeline.helpers.gold_helper import build_gold_artist, build_gold_top_tracks, build_gold_recent_tracks
from spotify_data_pipeline.helpers.schema_validation import validate_item_schema
from spotify_data_pipeline.helpers.genre_helper import update_recent_artist_genres as run_genre_update
from datetime import datetime
from functools import partial
import logging

app = func.FunctionApp()

JOBS = {
    "recent_tracks": {
        "func": get_recent_tracks,
        "silver_func": fill_silver_recent_tracks,
        "scope": "user-read-recently-played",
        "kwargs": {"limit": 50},
    },
    "top_tracks_short": {
        "func": get_top_tracks,
        "silver_func": partial(process_silver_tracks, "short"),
        "gold_func": partial(build_gold_top_tracks, "short"),
        "scope": "user-top-read",
        "kwargs": {"limit": 50, "time_range": "short_term"},
    },
    "top_tracks_medium": {
        "func": get_top_tracks,
        "silver_func": partial(process_silver_tracks, "medium"),
        "gold_func": partial(build_gold_top_tracks, "medium"),
        "scope": "user-top-read",
        "kwargs": {"limit": 50, "time_range": "medium_term"},
    },
    "top_tracks_long": {
        "func": get_top_tracks,
        "silver_func": partial(process_silver_tracks, "long"),
        "gold_func": partial(build_gold_top_tracks, "long"),
        "scope": "user-top-read",
        "kwargs": {"limit": 50, "time_range": "long_term"},
    },
    "top_artists_short": {
        "func": get_top_artists,
        "silver_func": partial(process_silver_artists, "short"),
        "gold_func": partial(build_gold_artist, "short"),
        "scope": "user-top-read",
        "kwargs": {"limit": 50, "time_range": "short_term"},
    },
    "top_artists_medium": {
        "func": get_top_artists,
        "silver_func": partial(process_silver_artists, "medium"),
        "gold_func": partial(build_gold_artist, "medium"),
        "scope": "user-top-read",
        "kwargs": {"limit": 50, "time_range": "medium_term"},
    },
    "top_artists_long": {
        "func": get_top_artists,
        "silver_func": partial(process_silver_artists, "long"),
        "gold_func": partial(build_gold_artist, "long"),
        "scope": "user-top-read",
        "kwargs": {"limit": 50, "time_range": "long_term"},
    }
}

def execute(job_name: str):
    job = JOBS[job_name]
    downloaded_at = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    token = get_access_token(job["scope"])
    items = job["func"](token, **job["kwargs"])
    if items:
        missing = validate_item_schema(items[0], job_name)
        if missing:
            logging.error(f"[{job_name}] Schema mismatch — missing fields: {missing}")
            raise ValueError(f"Spotify API schema changed for {job_name}: missing {missing}")

    write_bronze_batch(entity=job_name, payload=items, downloaded_at=downloaded_at)
    logging.info(f"[{job_name}] wrote {len(items)} items")
    silver_func = JOBS[job_name].get("silver_func")
    gold_func = JOBS[job_name].get("gold_func")
    if silver_func:
        silver_func()
    if gold_func:
        gold_func()



@app.timer_trigger(schedule="0 0 0 * * *", arg_name="timer", run_on_startup=False)
def recent_tracks(timer: func.TimerRequest):
    execute("recent_tracks")

@app.timer_trigger(schedule="0 0 3 * * 1", arg_name="timer", run_on_startup=False)
def gold_recent_tracks(timer: func.TimerRequest):
    build_gold_recent_tracks(year=str(datetime.now().year))

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

@app.timer_trigger(schedule="0 0 4 * * 1", arg_name="timer", run_on_startup=False)
def update_recent_artist_genres(timer: func.TimerRequest):
    token = get_access_token("user-top-read")
    run_genre_update(token)            