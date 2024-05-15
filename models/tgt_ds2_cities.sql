{{
    config(
        materialized = 'incremental',
        unique_key='cityid',
        on_schema_change='fail'
) }}
WITH stage_src_ds2_employee AS (
    SELECT * FROM {{ ref('stage_src_ds2_employee') }}
)
SELECT 
  {{ dbt_utils.generate_surrogate_key(['empid', 'city_name','timestamp_column']) }} AS cityid,
  empid,
  city_name,
  CURRENT_TIMESTAMP AS timestamp_column
  FROM stage_src_ds2_employee
GROUP BY empid, city_name, timestamp_column
{% if is_incremental() %}
  having timestamp_column > (select max(timestamp_column) from {{ this }})
{% endif %}