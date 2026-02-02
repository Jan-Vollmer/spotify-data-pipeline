from .fill_silver_recent_tracks import fill_silver_recent_tracks
# from spotify_data_pipeline.fill_silver_top_tracks import fill_silver_top_tracks
from spotify_data_pipeline.fill_silver_top_artists import fill_silver_top_artists

def fill_silver():
    # Recent Tracks
    fill_silver_recent_tracks()

    # Top Artists
    fill_silver_top_artists()
    
    # Top Tracks
    # fill_silver_top_tracks()
