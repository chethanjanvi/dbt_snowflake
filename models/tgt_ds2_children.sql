{{
    config(
        materialized = 'incremental',
        unique_key='childid',
        on_schema_change='fail'
) }}
WITH children AS (
    SELECT * FROM {{ ref('children') }}
)
SELECT 
  {{ dbt_utils.generate_surrogate_key(['empid','name','gender']) }} AS childid,
  empid,
  name,
  gender,
  age,
  CURRENT_TIMESTAMP AS timestamp_column
  FROM children
GROUP BY empid, name, gender,age,timestamp_column
{% if is_incremental() %}
  having timestamp_column > (select max(timestamp_column) from {{ this }})
{% endif %}