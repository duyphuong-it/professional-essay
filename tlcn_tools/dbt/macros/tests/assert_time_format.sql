{% test assert_time_format(model, column_name) %}
-- Kiểm tra định dạng HH:MI:SS (24h)
SELECT *
FROM {{ model }}
WHERE {{ column_name }} IS NOT NULL
  AND (
    CAST({{ column_name }} AS TEXT) !~ '^(2[0-3]|[0-1][0-9]):[0-5][0-9]:[0-5][0-9]$'
    OR TO_TIMESTAMP(CAST({{ column_name }} AS TEXT), 'HH24:MI:SS') IS NULL
  )
{% endtest %}