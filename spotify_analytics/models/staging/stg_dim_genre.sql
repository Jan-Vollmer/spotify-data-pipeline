select genre_name 
from {{ source('silver', 'dim_genre') }}