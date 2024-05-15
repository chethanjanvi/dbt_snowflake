{{
    config(
        materialized = 'incremental',
        unique_key='cityid',
        incremental_strategy='merge',
        merge_update_columns = ['city_name'],
        on_schema_change='fail'
) }}
WITH tgt_ds2_cities AS (
    SELECT * FROM {{ ref('tgt_ds2_cities') }}
)
SELECT 
  *
  FROM tgt_ds2_cities
{% if is_incremental() %}
  where timestamp_column > (select max(timestamp_column) from {{ this }})
{% endif %}
