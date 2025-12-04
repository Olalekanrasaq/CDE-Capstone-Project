{{ config(
    materialized='incremental',
    unique_key='call_id'
) }}

select
    call_id,
    customer_id,
    complaint_category,
    agent_id,
    cast(call_start_time as timestamp) as call_start_time,
    cast(call_end_time as timestamp) as call_end_time,
    resolution_status,
    calllogsgenerationdate,
    ingested_at
from {{ source('coretelecom', 'call_logs') }}

{% if is_incremental() %}
    where ingested_at > (select max(ingested_at) from {{ this }})
{% endif %}