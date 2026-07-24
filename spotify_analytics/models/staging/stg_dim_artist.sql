select
    artist_id,
    artist_name
from {{ source('silver', 'dim_artist') }}