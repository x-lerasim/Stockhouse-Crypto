{{ config(
    materialized='table'
) }}

select
    load_date,
    count() as total_assets,
    sum(market_cap) as total_market_cap,
    avg(market_cap) as avg_market_cap,
    avg(price_usd) as avg_price,
    max(price_usd) as max_price,
    min(price_usd) as min_price,
    avg(price_change_24h_pct) as avg_change_24h,
    countIf(price_change_24h_pct > 0) as growing_count,
    countIf(price_change_24h_pct < 0) as declining_count,
    countIf(rank_category = 'Top_10') as top_10_count,
    countIf(rank_category = 'Top_50') as top_50_count,
    countIf(price_movement_category = 'Strong_Growth') as strong_growth_count,
    countIf(price_movement_category = 'Growth') as growth_count,
    countIf(price_movement_category = 'Decline') as decline_count,
    countIf(price_movement_category = 'Strong_Decline') as strong_decline_count

from {{ ref('int_crypto__enriched_assets') }}
group by load_date
order by load_date desc