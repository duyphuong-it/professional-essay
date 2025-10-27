{{ 
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='status_key',
    schema='gold'
  )
}}

WITH silver_status AS (
    SELECT
        status_key,
        status_name
    FROM {{ ref('status') }}
    {% if is_incremental() %}
      WHERE status_key NOT IN (SELECT status_key FROM {{ this }})
    {% endif %}
)

SELECT * FROM silver_status