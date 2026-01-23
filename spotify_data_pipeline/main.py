from .fill_bronze import fill_bronze
from .fill_silver import fill_silver


def main():
    downloaded_at = fill_bronze(limit_top=20, limit_recent=50)
    fill_silver()

if __name__ == "__main__":
    main()