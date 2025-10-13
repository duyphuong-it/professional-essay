{% test assert_date_format(model, column_name) %}
-- Kiểm tra định dạng YYYY-MM-DD (chuẩn ISO)
SELECT *
FROM {{ model }}
WHERE {{ column_name }} IS NOT NULL
  AND (
    CAST({{ column_name }} AS TEXT) !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
    OR TO_DATE(CAST({{ column_name }} AS TEXT), 'YYYY-MM-DD') IS NULL
  )
{% endtest %}