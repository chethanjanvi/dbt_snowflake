{{
    config(
        materialized = 'incremental',
        unique_key='childid',
        incremental_strategy='merge',
        merge_exclude_columns = ['empid','timestamp_column'],
        on_schema_change='fail'
) }}
WITH tgt_ds2_children AS (
    SELECT * FROM {{ ref('tgt_ds2_children') }}
)
SELECT 
  *
  FROM tgt_ds2_children
{% if is_incremental() %}
  where timestamp_column> (select max(timestamp_column) from {{ this }})
{% endif %}