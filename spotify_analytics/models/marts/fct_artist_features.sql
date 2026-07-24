{{ config(materialized='table') }}

with rankings as (
    select
        artist_id,
        term_id,
        position,
        snapshot_date,
        date_trunc('month', snapshot_date) as snapshot_month
    from {{ ref('stg_fact_artist_rankings') }}
),

genres as (
    select
        artist_id,
        list(genre_name) as genres
    from {{ ref('stg_bridge_artist_genre') }}
    group by artist_id
),

aggregated as (
    select
        artist_id,
        term_id,
        snapshot_month,
        avg(position) as avg_position,
        stddev(position) as position_stddev,
        count(*) as snapshot_count,
        case
            when snapshot_count > 1 then regr_slope(position, epoch(snapshot_date))
            else null
        end as position_trend_slope
    from rankings
    group by artist_id, term_id, snapshot_month
)

select
    a.artist_id,
    art.artist_name,
    a.term_id,
    a.snapshot_month,
    a.avg_position,
    a.position_stddev,
    a.position_trend_slope,
    a.snapshot_count,
    g.genres
from aggregated a
left join {{ ref('stg_dim_artist') }} art on a.artist_id = art.artist_id
left join genres g on a.artist_id = g.artist_id