{{ config(materialized='incremental', unique_key=['symbol', 'ingested_at']) }}

select
    symbol,
    price,
    open,
    high,
    low,
    prev_close,
    volume,
    change,
    change_pct,
    is_positive_day,
    price_range,
    price_range_pct,
    event_time,
    provider,
    silver_processed_at,
    load_date
from {{ source('silver', 'stock_quotes') }}
where price > 0
  and symbol is not null

{% if is_incremental() %}
  and silver_processed_at > (select max(silver_processed_at) from {{ this }})
{% endif %}
