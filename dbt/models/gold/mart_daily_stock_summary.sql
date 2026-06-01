{{ config(materialized='table') }}

select
    symbol,
    date_trunc('day', event_time)           as trade_date,
    first_value(price) over w               as open_price,
    max(high)          over w               as day_high,
    min(low)           over w               as day_low,
    last_value(price)  over w               as close_price,
    sum(volume)        over w               as total_volume,
    avg(price)         over w               as avg_price,
    avg(change_pct)    over w               as avg_change_pct,
    stddev(price)      over w               as price_volatility,
    count(*)           over w               as tick_count
from {{ ref('int_stock_quotes') }}
window w as (
    partition by symbol, date_trunc('day', event_time)
    order by event_time
    rows between unbounded preceding and unbounded following
)
