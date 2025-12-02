with raw_social_media as (
    select * 
    from {{ source('coretelecom', 'social_media') }}
),

ranked_social_media as (
    select
        *,
        row_number() over (
            partition by complaint_id
            order by mediacomplaintgenerationdate desc
        ) as rn
    from raw_social_media
)

select
    complaint_id,
    customer_id,
    complaint_category,
    agent_id,
    resolution_status,
    CAST(NULLIF(TRIM(request_date), '') as timestamp) as request_date,
    CAST(NULLIF(TRIM(resolution_date), '') as timestamp) as resolution_date,
    media_channel,
    mediacomplaintgenerationdate
from ranked_social_media
where rn = 1
