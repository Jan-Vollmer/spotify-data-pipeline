from spotify_data_pipeline.helpers.track_helper import process_silver_tracks

def fill_silver_top_tracks():
    process_silver_tracks("long_term")
    process_silver_tracks("medium_term")
    process_silver_tracks("short_term")
