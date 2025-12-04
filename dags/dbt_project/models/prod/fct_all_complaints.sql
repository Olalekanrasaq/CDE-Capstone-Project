{{ config(
    materialized='incremental',
    unique_key='complaint_id',
    sort=['loaded_at', 'complaint_date', 'complaint_category', 'resolution_status'],
    sort_type='interleaved'
) }}


with all_sources as (
    select
        call_id as complaint_id,
        customer_name,
        customer_email,
        gender,
        complaint_category,
        agent_name,
        experience,
        call_start_time as request_date,
        resolution_status,
        calllogsgenerationdate as complaint_date,
        'Call Logs' as source_system,
        ingested_at as loaded_at
    from {{ ref('fct_call_logs') }}

    union all

    select
        request_id as complaint_id,
        customer_name,
        customer_email,
        gender,
        complaint_category,
        agent_name,
        experience,
        request_date,
        resolution_status,
        webformgenerationdate as complaint_date,
        'Web Forms' as source_system,
        ingested_at as loaded_at
    from {{ ref('fct_web_forms') }}

    union all

    select
        complaint_id,
        customer_name,
        customer_email,
        gender,
        complaint_category,
        agent_name,
        experience,
        request_date,
        resolution_status,
        mediacomplaintgenerationdate as complaint_date,
        'Social Media' as source_system,
        ingested_at as loaded_at
    from {{ ref('fct_social_media') }}
)

select * 
from all_sources

{% if is_incremental() %}
  where loaded_at > (select max(loaded_at) from {{ this }})
{% endif %}

