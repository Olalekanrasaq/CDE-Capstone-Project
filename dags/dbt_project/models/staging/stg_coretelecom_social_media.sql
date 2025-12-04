{{ config(
    materialized='incremental',
    unique_key='complaint_id'
) }}

select
    complaint_id,
    customer_id,
    complaint_category,
    agent_id,
    resolution_status,
    CAST(NULLIF(TRIM(request_date), '') as timestamp) as request_date,
    CAST(NULLIF(TRIM(resolution_date), '') as timestamp) as resolution_date,
    media_channel,
    mediacomplaintgenerationdate,
    ingested_at
from {{ source('coretelecom', 'social_media') }}

{% if is_incremental() %}
    where ingested_at > (select max(ingested_at) from {{ this }})
{% endif %}