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
	    REGEXP_REPLACE(
		    REGEXP_REPLACE(
		        split_part(email, '@', 1),
		        '^[A-Za-z0-9]*[0-9]+',
		        ''
		    ),
		    '(.{3})$',
		    ''
	 	)
	    || '@' ||
	    CASE
	      WHEN LOWER(split_part(email,'@',2)) LIKE '%gmai%' THEN 'gmail.com'
	      WHEN LOWER(split_part(email,'@',2)) LIKE '%hotma%' THEN 'hotmail.com'
	      WHEN LOWER(split_part(email,'@',2)) LIKE '%yaho%'  THEN 'yahoo.com'
	      WHEN LOWER(split_part(email,'@',2)) LIKE '%ymail%' THEN 'ymail.com'
	      ELSE LOWER(split_part(email,'@',2))
	    END
	) as customer_email,
    address,
	ingested_at
from dedup_customers