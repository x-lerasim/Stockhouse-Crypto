{{ config(materialized='view') }}
SELECT * FROM crypto.mart_daily_summary_target FINAL;