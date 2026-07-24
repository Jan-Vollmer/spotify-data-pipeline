select 
    artist_id, 
    term_id, 
    position, 
    snapshot_date 
from {{ source('silver', 'fact_artist_rankings') }}