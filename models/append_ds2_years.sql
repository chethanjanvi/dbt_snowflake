{{
    config(
        materialized = 'incremental',
        unique_key='yearid',
        incremental_strategy='append',
        on_schema_change='fail'
) }}
WITH tgt_ds2_years AS (
    SELECT * FROM {{ ref('tgt_ds2_years') }}
)
SELECT 
  *
FROM tgt_ds2_years
{% if is_incremental() %}
  where timestamp_column > (select max(timestamp_column) from {{ this }})
{% endif %}