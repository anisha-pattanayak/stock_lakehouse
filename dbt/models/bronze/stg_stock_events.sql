{{ config(materialized='incremental', unique_key='kafka_offset') }}

select
    kafka_key,
    raw_json,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_timestamp,
    bronze_loaded_at,
    load_date
from {{ source('bronze', 'stock_events') }}

{% if is_incremental() %}
  where bronze_loaded_at > (select max(bronze_loaded_at) from {{ this }})
{% endif %}
