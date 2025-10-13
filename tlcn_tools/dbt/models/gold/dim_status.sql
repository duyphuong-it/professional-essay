{{ config(materialized='table', schema='gold') }}

WITH silver_status AS (
    SELECT
        status_key,
        status_name
    FROM {{ ref('status') }}
)

SELECT * FROM silver_status