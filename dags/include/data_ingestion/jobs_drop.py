from airflow.providers.postgres.hooks.postgres import PostgresHook
import psycopg2


def delete_copy_job(jobs_to_drop):
    
    redshift_conn_id = 'redshift_conn' 
    hook = PostgresHook(postgres_conn_id=redshift_conn_id)
    
    # Get the connection object from the hook
    # This will contain the host, port, user, password, and database
    conn_details = hook.get_connection(redshift_conn_id)
    
    # Extract individual connection parameters
    host = conn_details.host
    port = conn_details.port
    user = conn_details.login
    password = conn_details.password
    database = conn_details.schema
    
    # Construct the connection string for psycopg2
    conn_string = f"host={host} port={port} user={user} password={password} dbname={database}"

    try:
        conn = psycopg2.connect(conn_string)
        conn.autocommit = True  # Enable autocommit
        cursor = conn.cursor()
        
        # Example: Execute a query
        query_string = f"SELECT job_name FROM SYS_COPY_JOB WHERE job_name in {jobs_to_drop};"
        cursor.execute(query_string)
        results = cursor.fetchall()
        

        if results:
                # Drop the job first
            for job_name in results:
                cursor.execute(f"COPY JOB DROP {job_name[0]};")
                print(f"COPY JOB {job_name} is dropped!!")
        
        cursor.close()
        conn.close()
        
    except psycopg2.Error as e:
        print(f"Error connecting to Redshift: {e}")

    