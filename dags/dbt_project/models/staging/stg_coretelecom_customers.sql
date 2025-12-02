with raw_customers as (
    select * 
    from {{ source('coretelecom', 'customers') }}
),

ranked_customers as (
    select
        *,
        row_number() over (
            partition by customer_id
            order by signup_date desc
        ) as rn
    from raw_customers
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
    address
from ranked_customers
where rn = 1
