{{ config(
    materialized = 'incremental',
    unique_key = 'query_id'
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
    AND query_type = 'SELECT'
    {# COMMENT THE DATE FILTER OUT TO EMULATE --FULL-REFRESH  #}
    AND TO_DATE(start_time) = DATEADD(DAY, -1, CURRENT_DATE())
    AND queued_overload_time = 0
    AND total_elapsed_time > 300000