import pandas as pd
from pathlib import Path
from spotify_data_pipeline.helpers.artist_helper import process_silver_artists

def fill_silver_top_artists():
    process_silver_artists("long_term")
    process_silver_artists("medium_term")
    process_silver_artists("short_term")