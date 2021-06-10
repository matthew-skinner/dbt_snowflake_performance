{{ config(
    materialized = 'view',
) }}

SELECT
    TO_DATE(start_time) AS DATE,
    COUNT (
        DISTINCT query_id
    ) AS query_count,
    warehouse_name,
    warehouse_size
FROM
    {{ source(
        'snowflake',
        'query_history'
    ) }}
WHERE
    1 = 1
    AND TO_DATE(start_time) BETWEEN DATE_TRUNC(months, ADD_MONTHS(CURRENT_DATE(), -12))
    AND getdate()
GROUP BY
    DATE
