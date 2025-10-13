{{ config(materialized='view', schema='silver') }}

WITH transformed_payments AS (
    SELECT
        payment_method,
        category,
        encode(DIGEST(CAST(payment_method AS VARCHAR), 'sha256'), 'hex') AS payment_key
    FROM {{ source('bronze', 'payments') }}
)

SELECT * FROM transformed_payments