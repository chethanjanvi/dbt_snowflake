WITH src_ds2_employee AS (
    SELECT * FROM {{ ref('src_ds2_employee') }}
)
SELECT 
    *
FROM src_ds2_employee