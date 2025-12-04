-- create redshift schemas
CREATE SCHEMA IF NOT EXISTS landing;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS production;


-- create tables to load raw s3 data into landing tables

-- create customers table
CREATE TABLE IF NOT EXISTS landing.customers (
    customer_id VARCHAR,
    name VARCHAR,
    gender CHAR(1),
    birth_date VARCHAR,
    signup_date VARCHAR,
    email VARCHAR,
    address VARCHAR,
    ingested_at TIMESTAMP
);

-- create agents table
CREATE TABLE IF NOT EXISTS landing.agents (
    agent_id BIGINT,
    name VARCHAR,
    experience VARCHAR,
    state VARCHAR,
    ingested_at TIMESTAMP
);

-- create call_logs table
CREATE TABLE IF NOT EXISTS landing.call_logs (
    call_id VARCHAR, 
    customer_id VARCHAR,
    complaint_category VARCHAR,
    agent_id BIGINT,
    call_start_time VARCHAR,
    call_end_time VARCHAR,
    resolution_status VARCHAR,
    callLogsGenerationDate VARCHAR,
    ingested_at TIMESTAMP
);

-- create call_logs table
CREATE TABLE IF NOT EXISTS landing.social_media (
    complaint_id VARCHAR,
    customer_id VARCHAR,
    complaint_category VARCHAR,
    agent_id BIGINT,
    resolution_status VARCHAR,
    request_date VARCHAR,
    resolution_date VARCHAR,
    media_channel VARCHAR,
    MediaComplaintGenerationDate VARCHAR,
    ingested_at TIMESTAMP
);

-- create call_logs table
CREATE TABLE IF NOT EXISTS landing.website_forms (
    request_id VARCHAR,
    customer_id VARCHAR,
    complaint_category VARCHAR,
    agent_id BIGINT,
    resolution_status VARCHAR,
    request_date VARCHAR,
    resolution_date VARCHAR,
    webFormGenerationDate VARCHAR,
    ingested_at TIMESTAMP
);