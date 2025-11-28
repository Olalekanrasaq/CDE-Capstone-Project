-- Load customers table
COPY landing.customers
FROM 's3://cde-capstone-olalekan/customers'
IAM_ROLE 'arn:aws:iam::068355631913:role/capstone-redshift-role'
FORMAT AS PARQUET
JOB CREATE cde_s3_auto_copy_customers
AUTO ON;

-- Load agents tables
COPY landing.agents
FROM 's3://cde-capstone-olalekan/agents'
IAM_ROLE 'arn:aws:iam::068355631913:role/capstone-redshift-role'
FORMAT AS PARQUET
JOB CREATE cde_s3_auto_copy_agents
AUTO ON;

-- Load call_logs tables
COPY landing.call_logs
FROM 's3://cde-capstone-olalekan/call logs'
IAM_ROLE 'arn:aws:iam::068355631913:role/capstone-redshift-role'
FORMAT AS PARQUET
JOB CREATE cde_s3_auto_copy_callLogs
AUTO ON;

-- Load social_media tables
COPY landing.social_media
FROM 's3://cde-capstone-olalekan/social_media'
IAM_ROLE 'arn:aws:iam::068355631913:role/capstone-redshift-role'
FORMAT AS PARQUET
JOB CREATE cde_s3_auto_copy_socialMedia
AUTO ON;

-- Load website_Forms tables
COPY landing.website_forms
FROM 's3://cde-capstone-olalekan/website_forms'
IAM_ROLE 'arn:aws:iam::068355631913:role/capstone-redshift-role'
FORMAT AS PARQUET
JOB CREATE cde_s3_auto_copy_websiteForms
AUTO ON;