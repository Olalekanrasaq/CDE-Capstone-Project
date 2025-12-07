{{ config(
    materialized='incremental',
    unique_key='customer_id'
) }}


with dedup_customers as (
    select * 
    from (
        select
            *,
            row_number() over (
                partition by customer_id
                order by ingested_at desc
            ) as rn
        from {{ source('coretelecom', 'customers') }}
    ) as t
    where rn = 1
)

select
    customer_id,
    name as customer_name,
    gender,
    cast(birth_date as date) as birth_date,
    cast(signup_date as date) as signup_date,
    (
	    regexp_replace(
		    regexp_replace(
		        split_part(email, '@', 1),
		        '^[A-Za-z0-9]*[0-9]+',
		        ''
		    ),
		    '(.{3})$',
		    ''
	 	)
	    || '@' ||
	    case
	      when lower(split_part(email, '@', 2)) like '%gmai%' then 'gmail.com'
	      when lower(split_part(email, '@', 2)) like '%hotma%' then 'hotmail.com'
	      when lower(split_part(email, '@', 2)) like '%yaho%' then 'yahoo.com'
	      when lower(split_part(email, '@', 2)) like '%ymail%' then 'ymail.com'
	      else lower(split_part(email, '@', 2))
	    end
	) as customer_email,
    address,
	ingested_at
from dedup_customers