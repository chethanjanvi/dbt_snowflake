{{
    config(
        materialized = 'incremental',
        unique_key='empid',
        incremental_strategy='merge',
        merge_exclude_columns = ['empid','kind','timestamp_column'],
        on_schema_change='fail'
) }}

WITH tgt_ds2_personal AS (
    SELECT * FROM {{ ref('tgt_ds2_personal') }}
)
SELECT 
  *
FROM tgt_ds2_personal
{% if is_incremental() %}
  where timestamp_column > (select max(timestamp_column) from {{ this }})
{% endif %}