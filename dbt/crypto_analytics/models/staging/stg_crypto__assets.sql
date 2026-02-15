select
    asset_id,
    symbol,
    asset_name,
    rank as market_rank,
    price_usd,
    change_24h as price_change_24h_pct,
    market_cap,
    load_datetime as loaded_at,
    load_date 
from {{ source('crypto_staging', 'dim_assets') }}
