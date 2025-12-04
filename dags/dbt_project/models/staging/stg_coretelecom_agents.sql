{{ config(
    materialized='incremental',
    unique_key='agent_id'
) }}

with dedup_agents as (
    select * 
    from (
        select
            *,
            row_number() over (
                partition by agent_id
                order by ingested_at desc
            ) as rn
        from {{ source('coretelecom', 'agents') }}
    ) as t
    where rn = 1
)

select
    agent_id,
    name as agent_name,
    experience,
    state,
	ingested_at
from dedup_agents
