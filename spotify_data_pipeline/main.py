from .fill_bronze import fill_bronze
# from fill_silver import fill_silver


def main():
    top_tracks, top_artists, recent_tracks = fill_bronze(limit_top=20, limit_recent=50, time_range="long_term")
 #   fill_silver()

if __name__ == "__main__":
    main()