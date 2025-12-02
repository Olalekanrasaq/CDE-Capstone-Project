select DISTINCT
    agent_id,
    name as agent_name,
    experience,
    state
from {{ source('coretelecom', 'agents') }}
