{{ config(
    materialized = 'view'
) }}

select
    complaint_date::date as complaint_date,
    complaint_category,
    count(case when resolution_status = 'Resolved' then 1 end) as resolved_count,
    count(case when resolution_status = 'In-Progress' then 1 end) as pending_count,
    count(case when resolution_status = 'Blocked' then 1 end) as blocked_count,
    count(case when resolution_status = 'Backlog' then 1 end) as backlog_count
from {{ ref('fct_all_complaints') }}
group by cube(complaint_date::date, complaint_category)
order by 1, 2
