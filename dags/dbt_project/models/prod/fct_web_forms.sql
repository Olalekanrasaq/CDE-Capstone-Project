{{ config(
    materialized='incremental',
    unique_key='request_id',
    sort=['ingested_at', 'webformgenerationdate', 'request_id'],
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

stg_web_forms as (
    select
        *
    from {{ ref('stg_coretelecom_website_forms') }}
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
    wf.webformgenerationdate,
    wf.ingested_at
from stg_web_forms wf
left join stg_customers c on wf.customer_id = c.customer_id
left join stg_agents a on wf.agent_id = a.agent_id

{% if is_incremental() %}
  where wf.ingested_at > (select max(ingested_at) from {{ this }})
{% endif %}