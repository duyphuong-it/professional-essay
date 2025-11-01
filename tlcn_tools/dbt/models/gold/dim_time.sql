{{ 
  config(
    materialized='incremental',
    incremental_strategy='append',
    unique_key='time_key',
    on_schema_change='sync_all_columns',
    schema='gold'
  )
}}

WITH new_times AS (
    SELECT
        t.time_key,
        t.time,
        t.hour,
        t.minute,
        t.second,
        t.time_of_day
    FROM {{ ref('time') }} t
    {% if is_incremental() %}
      LEFT JOIN {{ this }} g
        ON t.time_key = g.time_key
      WHERE g.time_key IS NULL
    {% endif %}
)

SELECT * FROM new_times
