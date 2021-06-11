{{ config(
    materialized = 'incremental',
    unique_key = 'query_id'
) }}

SELECT
    TO_DATE(start_time) AS DATE,
    warehouse_name,
    warehouse_size,
    database_name,
    schema_name,
    query_type,
    query_id,
    query_text,
    user_name,
    bytes_scanned,
    total_elapsed_time,
    execution_time,
    queued_overload_time,
    queued_provisioning_time,
    queued_repair_time,
    query_load_percent
    CREDITS_USED_CLOUD_SERVICES
FROM
    {{ source(
        'snowflake',
        'query_history'
    ) }}

