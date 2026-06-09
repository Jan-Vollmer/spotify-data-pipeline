from spotify_data_pipeline.helpers.artist_helper import process_silver_artists

def fill_silver_top_artists():
    process_silver_artists("long")
    process_silver_artists("medium")
    process_silver_artists("short")