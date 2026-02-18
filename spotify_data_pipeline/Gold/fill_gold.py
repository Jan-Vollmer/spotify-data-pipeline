from spotify_data_pipeline.helpers.gold_helper import build_gold_artist, build_gold_top_tracks, build_gold_recent_tracks

def fill_gold():
    build_gold_artist()
    build_gold_top_tracks()
    build_gold_recent_tracks("2026")

if __name__ == "__main__":
    fill_gold()    