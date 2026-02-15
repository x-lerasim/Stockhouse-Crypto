with staging as (
    select * from {{ ref('stg_crypto__assets') }}
),


metrics as (
    select
        *,
       
        round((market_cap / sum(market_cap) over ()) * 100, 2) as market_share_pct,
        
        case 
            when market_rank <= 10 then 'Top_10'
            when market_rank <= 50 then 'Top_50'
            when market_rank <= 100 then 'Top_100'
            else 'Other'
        end as rank_category,
        case
            when price_change_24h_pct > 10 then 'Strong_Growth'
            when price_change_24h_pct > 0 then 'Growth'
            when price_change_24h_pct > -10 then 'Decline'
            else 'Strong_Decline'
        end as price_movement_category,
        round(price_usd * (price_change_24h_pct / 100), 2) as price_change_usd
        
    from staging
)

select * from metrics
