{{ config(materialized='table', schema='gold') }}

WITH silver_payments AS (
    SELECT
        payment_key,
        payment_method,
        category
    FROM {{ ref('payments') }}
)

SELECT * FROM silver_payments