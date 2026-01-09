import json
import csv
from pathlib import Path

def flatten_for_csv(item: dict) -> dict:
    flat_item = {}
    for k, v in item.items():
        if isinstance(v, (dict, list)):
            flat_item[k] = json.dumps(v, ensure_ascii=False)
        else:
            flat_item[k] = v
    return flat_item

def fill_silver():
    Path("data/silver").mkdir(parents=True, exist_ok=True)
    
    files = [
        ("top_tracks", "top_tracks.json"),
        ("top_artists", "top_artists.json"),
        ("recent_tracks", "recent_tracks.json")
    ]
    
    for name, filename in files:
        bronze_path = Path("data/bronze") / filename
        silver_path = Path("data/silver") / f"{name}.csv"
        
        with open(bronze_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        if not data:
            print(f"[WARN] Keine Daten in {filename}")
            continue
        
        fieldnames = data[0].keys()
        
        with open(silver_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for item in data:
                writer.writerow(flatten_for_csv(item))
        
        print(f"[INFO] {silver_path} erstellt ({len(data)} Zeilen)")