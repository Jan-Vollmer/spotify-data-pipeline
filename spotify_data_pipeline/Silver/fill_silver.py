from .fill_silver_recent_tracks import fill_silver_recent_tracks
from spotify_data_pipeline.Silver.fill_silver_top_tracks import fill_silver_top_tracks
from spotify_data_pipeline.Silver.fill_silver_top_artists import fill_silver_top_artists

def fill_silver():
    fill_silver_recent_tracks()
    fill_silver_top_artists()
    fill_silver_top_tracks()

if __name__ == "__main__":
    fill_silver()    