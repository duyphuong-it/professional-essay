{{ config(materialized='table', schema='gold') }}

WITH silver_time AS (
    SELECT
        time_key,
        time,
        hour,
        minute,
        second,
        time_of_day
    FROM {{ ref('time') }}
)

SELECT * FROM silver_time