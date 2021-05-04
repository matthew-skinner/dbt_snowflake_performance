{{ config(
    materialized = 'view',
) }}

SELECT
    COUNT (
        DISTINCT query_id
    ) AS query_id,
    TO_DATE(start_time) AS start_date,
    warehouse_name
FROM
    {{ source(
        'snowflake',
        'query_history'
    ) }}
WHERE
    1 = 1
    AND start_date BETWEEN DATE_TRUNC(months, ADD_MONTHS(CURRENT_DATE(), -3))
    AND getdate()
    AND (
        queued_overload_time / 1000 + queued_provisioning_time / 1000 + queued_repair_time / 1000
    ) > 5
GROUP BY
    start_date,
    warehouse_name
