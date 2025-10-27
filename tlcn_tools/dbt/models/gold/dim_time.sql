{{ 
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='time_key',
    schema='gold'
  )
}}

WITH silver_time AS (
    SELECT
        time_key,
        time,
        hour,
        minute,
        second,
        time_of_day
    FROM {{ ref('time') }}
    {% if is_incremental() %}
      WHERE time_key NOT IN (SELECT time_key FROM {{ this }})
    {% endif %}
)

SELECT * FROM silver_time