from .fill_bronze import fill_bronze
from .fill_silver import fill_silver
from .fill_gold import fill_gold


def main():
    downloaded_at = fill_bronze(limit_top=20, limit_recent=50)
    fill_silver()
    fill_gold()

if __name__ == "__main__":
    main()