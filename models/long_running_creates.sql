{{ config(
    materialized = 'view',
) }}

SELECT
    query_id,
    query_text,
    query_type,
    warehouse_name,
    warehouse_size,
    warehouse_type,
    query_load_percent,
    total_elapsed_time,
    compilation_time,
    execution_time,
    bytes_written,
    rows_produced,
    queued_provisioning_time,
    queued_overload_time,
    queued_repair_time,
    transaction_blocked_time
FROM
    {{ source(
        'snowflake',
        'query_history'
    ) }}
WHERE
    query_load_percent = 100
    AND query_type = 'CREATE_TABLE_AS_SELECT'
    AND TO_DATE(start_time) = DATEADD(DAY, -1, CURRENT_DATE())
    AND queued_overload_time = 0
    AND total_elapsed_time > 300000
ORDER BY
    total_elapsed_time DESC
