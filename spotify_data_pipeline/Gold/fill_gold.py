from spotify_data_pipeline.helpers.gold_helper import build_gold_artist, build_gold_tracks

def fill_gold():
    build_gold_artist()
    build_gold_tracks("top_tracks")
    build_gold_tracks("recent_tracks")