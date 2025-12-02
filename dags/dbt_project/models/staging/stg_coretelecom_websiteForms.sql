with raw_website_forms as (
    select * 
    from {{ source('coretelecom', 'website_forms') }}
),

ranked_website_forms as (
    select
        *,
        row_number() over (
            partition by request_id
            order by webformgenerationdate desc
        ) as rn
    from raw_website_forms
)

select
    request_id,
    customer_id,
    complaint_category,
    agent_id,
    resolution_status,
    CAST(NULLIF(TRIM(request_date), '') as timestamp) as request_date,
    CAST(NULLIF(TRIM(resolution_date), '') as timestamp) as resolution_date,
    webformgenerationdate
from ranked_website_forms
where rn = 1
