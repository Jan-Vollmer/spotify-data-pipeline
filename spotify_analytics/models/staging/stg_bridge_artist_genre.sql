select 
    artist_id, 
    genre_name 
from {{ source('silver', 'bridge_artist_genre') }}