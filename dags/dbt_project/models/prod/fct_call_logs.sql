{{ config(
    materialized='incremental',
    unique_key='call_id',
    sort=['ingested_at', 'calllogsgenerationdate', 'call_id'],
    sort_type='interleaved'
) }}

with stg_customers as (
    select
        customer_id,
        customer_name,
        gender,
        customer_email
    from {{ ref('stg_coretelecom_customers') }}
),

stg_agents as (
    select
        *
    from {{ ref('stg_coretelecom_agents') }}
),

stg_call_logs as (
    select
        *
    from {{ ref('stg_coretelecom_call_logs') }}
)

select 
    cl.call_id,
    c.customer_name,
    c.customer_email,
    c.gender,
    cl.complaint_category,
    a.agent_name,
    a.experience,
    a.state as agent_state,
    cl.call_start_time,
    cl.call_end_time,
    datediff(minute, cl.call_start_time, cl.call_end_time) as call_duration_minutes,
    cl.resolution_status,
    cl.calllogsgenerationdate,
    cl.ingested_at
from stg_call_logs cl
left join stg_customers c on cl.customer_id = c.customer_id
left join stg_agents a on cl.agent_id = a.agent_id

{% if is_incremental() %}
  where cl.ingested_at > (select max(ingested_at) from {{ this }})
{% endif %}