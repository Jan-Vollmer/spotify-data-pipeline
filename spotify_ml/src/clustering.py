from pathlib import Path
import duckdb
import pandas as pd
import numpy as np
import gower
import kmedoids


DB_PATH = str(Path(__file__).parent.parent.parent / "data" / "warehouse.duckdb")

con = duckdb.connect(DB_PATH)
df = con.sql("SELECT * FROM fct_artist_features").df()

df_clean = df[df['genres'].apply(lambda g: isinstance(g, (list, np.ndarray)))].copy()

features = df_clean[['artist_id', 'artist_name', 'avg_position', 'position_stddev', 'genres']].copy()
features['position_stddev'] = features['position_stddev'].fillna(0)


def jaccard_distance(set_a, set_b):
    a, b = set(set_a), set(set_b)
    if not a and not b:
        return 0.0
    return 1 - len(a & b) / len(a | b)

genre_lists = features['genres'].apply(list).tolist()
n = len(genre_lists)
jaccard_matrix = np.zeros((n, n))

for i in range(n):
    for j in range(i+1, n):
        d = jaccard_distance(genre_lists[i], genre_lists[j])
        jaccard_matrix[i, j] = d
        jaccard_matrix[j, i] = d

gower_numeric = gower.gower_matrix(features[['avg_position', 'position_stddev']])
combined_distance = 0.5 * gower_numeric + 0.5 * jaccard_matrix

final_result_combined = kmedoids.fasterpam(combined_distance, 8, random_state=42)
features['cluster'] = final_result_combined.labels
print(features['cluster'].value_counts().sort_index())

con.execute("""
    CREATE OR REPLACE TABLE artist_clusters AS
    SELECT artist_id, artist_name, cluster
    FROM features
""")
con.close()

print("Saved to artist_clusters table")