{{ config(materialized='table', schema='gold') }}

WITH silver_date AS (
    SELECT
        date_key,
        full_date,
        year,
        month,
        day,
        quarter
    FROM {{ ref('date') }}
)

SELECT * FROM silver_date