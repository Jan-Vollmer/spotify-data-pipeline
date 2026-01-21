import json
from pathlib import Path
import pandas as pd

def fill_silver_recent_tracks():
    bronze_dir = Path("data/bronze/recent_tracks")
    silver_dir = Path("data/silver/recent_tracks")
    silver_dir.mkdir(parents=True, exist_ok=True)

    entity_files = sorted(bronze_dir.glob("*.json"))
    all_rows = []

    if not entity_files:
        print(f"[WARN] Keine Bronze-Dateien für recent_tracks gefunden")
        return

    for f in entity_files:
        with open(f, "r", encoding="utf-8") as file:
            data = json.load(file)

        downloaded_at = f.stem.split("_")[-1]

        items = data.get("items", data)

        for item in items:
            row = {}
            for k, v in item.items():
                if isinstance(v, (dict, list)):
                    row[k] = json.dumps(v, ensure_ascii=False)
                else:
                    row[k] = v
            row["downloaded_at"] = downloaded_at
            all_rows.append(row)

    if not all_rows:
        print(f"[WARN] Keine Daten für recent_tracks")
        return

    silver_path = silver_dir / "recent_tracks.parquet"
    df = pd.DataFrame(all_rows)
    df.to_parquet(silver_path, index=False)
    print(f"[INFO] {silver_path} erstellt ({len(df)} Zeilen)")