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
    from {{ ref('stg_coretelecom_callLogs') }}
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
    datediff(minute, cl.call_start_time, cl.call_end_time) as call_duration_minutes,
    cl.resolution_status,
    cl.calllogsgenerationdate
from stg_call_logs cl
left join stg_customers c on cl.customer_id = c.customer_id
left join stg_agents a on cl.agent_id = a.agent_id