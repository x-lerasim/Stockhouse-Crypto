{{ config(
    materialized='table'
) }}

select
    load_date,
    asset_id,
    symbol,
    asset_name,
    market_rank,
    price_usd,
    price_change_24h_pct,
    price_change_usd,
    market_cap,
    market_share_pct,
    rank_category,
    price_movement_category

from {{ ref('int_crypto__enriched_assets') }}
where market_rank <= 100
order by load_date desc, market_rank asc