{{
    config(
        materialized = 'incremental',
        unique_key='yearid',
        on_schema_change='fail'
) }}
WITH stage_src_ds2_employee AS (
    SELECT * FROM {{ ref('stage_src_ds2_employee') }}
)
SELECT 
  {{ dbt_utils.generate_surrogate_key(['empid', 'year']) }} AS yearid,
  empid,
  year,
 CURRENT_TIMESTAMP AS timestamp_column
FROM stage_src_ds2_employee
GROUP BY empid, year, timestamp_column
{% if is_incremental() %}
  having timestamp_column > (select max(timestamp_column) from {{ this }})
{% endif %}