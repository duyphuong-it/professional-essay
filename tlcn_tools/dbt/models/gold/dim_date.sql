{{ 
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='date_key',
    schema='gold'
  )
}}

WITH silver_date AS (
    SELECT
        date_key,
        full_date,
        year,
        month,
        day,
        quarter
    FROM {{ ref('date') }}
    {% if is_incremental() %}
      WHERE date_key NOT IN (SELECT date_key FROM {{ this }})
    {% endif %}
)

SELECT * FROM silver_date