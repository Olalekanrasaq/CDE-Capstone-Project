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

stg_web_forms as (
    select
        *
    from {{ ref('stg_coretelecom_websiteForms') }}
)

select 
    wf.request_id,
    c.customer_name,
    c.customer_email,
    c.gender,
    wf.complaint_category,
    a.agent_name,
    a.experience,
    a.state as agent_state,
    wf.request_date,
    wf.resolution_date,
    wf.resolution_status,
    wf.webformgenerationdate
from stg_web_forms wf
left join stg_customers c on wf.customer_id = c.customer_id
left join stg_agents a on wf.agent_id = a.agent_id