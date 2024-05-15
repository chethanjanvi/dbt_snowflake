{{
    config(
        materialized = 'incremental',
        unique_key='empid',
        on_schema_change='fail'
) }}

WITH stage_src_ds2_employee AS (
    SELECT * FROM {{ ref('stage_src_ds2_employee') }}
)
SELECT 
  empid,
  kind,
  fullName,
  age,
  gender,
  areaCode,
  phoneNumber,
  CURRENT_TIMESTAMP AS timestamp_column
FROM stage_src_ds2_employee
GROUP BY empid, kind, fullName, age, gender, areaCode, phoneNumber, timestamp_column
{% if is_incremental() %}
  having timestamp_column > (select max(timestamp_column) from {{ this }})
{% endif %}
