from spotify_data_pipeline.helpers.gold_helper import unify_silver, load_silver

def fill_gold():
    df_l = load_silver("top_artists", "long_term")
    df_m = load_silver("top_artists", "medium_term")
    df_s = load_silver("top_artists", "short_term")