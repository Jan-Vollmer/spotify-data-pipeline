from spotify_data_pipeline.helpers.gold_helper import unify_silver, load_silver, write_gold

def fill_gold():
    df_l = load_silver("top_artists", "long_term")
    df_m = load_silver("top_artists", "medium_term")
    df_s = load_silver("top_artists", "short_term")

    df_all = unify_silver(
        {
            "l": df_l,
            "m": df_m,
            "s": df_s
        },
        scope="top_artists"
    )

    write_gold(df_all, "top_artists")