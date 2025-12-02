with raw_calls as (
    select * 
    from {{ source('coretelecom', 'call_logs') }}
),

ranked_calls as (
    select
        *,
        row_number() over (
            partition by call_id
            order by calllogsgenerationdate desc
        ) as rn
    from raw_calls
)

select
    call_id,
    customer_id,
    complaint_category,
    agent_id,
    cast(call_start_time as timestamp) as call_start_time,
    cast(call_end_time as timestamp) as call_end_time,
    resolution_status,
    calllogsgenerationdate
from ranked_calls
where rn = 1
