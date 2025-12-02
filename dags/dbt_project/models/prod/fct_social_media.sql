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

stg_social_media as (
    select
        *
    from {{ ref('stg_coretelecom_socialMedia') }}
)

select 
    sm.complaint_id,
    c.customer_name,
    c.customer_email,
    c.gender,
    sm.complaint_category,
    a.agent_name,
    a.experience,
    a.state as agent_state,
    sm.resolution_status,
    sm.media_channel,
    sm.request_date,
    sm.resolution_date,
    sm.mediacomplaintgenerationdate
from stg_social_media sm
left join stg_customers c on sm.customer_id = c.customer_id
left join stg_agents a on sm.agent_id = a.agent_id