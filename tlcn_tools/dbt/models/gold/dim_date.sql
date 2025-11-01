{{ 
  config(
    materialized='incremental',
    incremental_strategy='append',
    unique_key='date_key',
    on_schema_change='sync_all_columns',
    schema='gold'
  )
}}

WITH new_dates AS (
    SELECT
        d.date_key,
        d.full_date,
        d.year,
        d.month,
        d.day,
        d.quarter
    FROM {{ ref('date') }} d
    {% if is_incremental() %}
      LEFT JOIN {{ this }} g
        ON d.date_key = g.date_key
      WHERE g.date_key IS NULL
    {% endif %}
)

SELECT * FROM new_dates
