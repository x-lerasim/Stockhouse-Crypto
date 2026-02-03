{{ config(
    materialized='table'
) }}

with btc as (
    select market_cap as btc_market_cap
    from {{ ref('int_crypto__enriched_assets') }}
    where symbol = 'BTC'
      and load_date = (select max(load_date) from {{ ref('int_crypto__enriched_assets') }})
),

eth as (
    select market_cap as eth_market_cap
    from {{ ref('int_crypto__enriched_assets') }}
    where symbol = 'ETH'
      and load_date = (select max(load_date) from {{ ref('int_crypto__enriched_assets') }})
),

totals as (
    select
        load_date,
        sum(market_cap) as total_market_cap,
        avg(price_change_24h_pct) as avg_change_24h,
        argMax(symbol, price_usd) as highest_price_symbol,
        argMax(symbol, price_change_24h_pct) as top_gainer_symbol,
        argMin(symbol, price_change_24h_pct) as top_loser_symbol
    from {{ ref('int_crypto__enriched_assets') }}
    where load_date = (select max(load_date) from {{ ref('int_crypto__enriched_assets') }})
    group by load_date
)

select
    t.load_date,
    t.total_market_cap,
    round((b.btc_market_cap / t.total_market_cap) * 100, 2) as btc_dominance_pct,
    round((e.eth_market_cap / t.total_market_cap) * 100, 2) as eth_dominance_pct,
    t.avg_change_24h,
    t.highest_price_symbol,
    t.top_gainer_symbol,
    t.top_loser_symbol,
    case
        when t.avg_change_24h > 5 then 'Very Bullish'
        when t.avg_change_24h > 0 then 'Bullish'
        when t.avg_change_24h > -5 then 'Bearish'
        else 'Very Bearish'
    end as market_sentiment

from totals t
cross join btc b
cross join eth e