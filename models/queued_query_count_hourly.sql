{{ config(
    materialized = 'view',
) }}

SELECT
    COUNT (
        DISTINCT query_id
    ) AS query_id,
    HOUR(start_time) AS hour_mst,
    warehouse_name
FROM
    {{ source(
        'snowflake',
        'query_history'
    ) }}
WHERE
    1 = 1
    AND TO_DATE(start_time) BETWEEN DATE_TRUNC(months, ADD_MONTHS(CURRENT_DATE(), -3))
    AND getdate()
    AND (
        queued_overload_time / 1000 + queued_provisioning_time / 1000 + queued_repair_time / 1000
    ) > 5
GROUP BY
    hour_mst,
    warehouse_name
