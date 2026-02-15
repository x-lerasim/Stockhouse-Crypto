{{ config(materialized='view') }}
select * from crypto.mart_daily_summary_target