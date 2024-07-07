from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.sensors.filesystem import FileSensor
from airflow.hooks.postgres_hook import PostgresHook
from scripts.utils import fetch_rss, fetch_api, fetch_csv, Job, clean_and_format_salary


def fetch_rss_data(**context):
    """
    Fetch job data from an RSS feed.
    """
    RSS_URL = context.get('RSS_URL', 'default_URL_if_not_provided')
    JOB_ROLE = context.get('JOB_ROLE', 'default_role_if_not_provided')
    jobs = fetch_rss(RSS_URL, JOB_ROLE)
    context['task_instance'].xcom_push(key='raw_data', value=jobs)
    if jobs:
        for job in jobs:
            print(job)
    else:
        print("No jobs found.")


def fetch_api_data(**context):
    """
    Fetch job data from an API.
    """
    API_URL = context.get('API_URL', 'default_URL_if_not_provided')
    JOB_ROLE = context.get('JOB_ROLE', 'default_role_if_not_provided')
    jobs = fetch_api(API_URL, JOB_ROLE)
    context['task_instance'].xcom_push(key='raw_data', value=jobs)
    if jobs:
        for job in jobs:
            print(job)
    else:
        print("No jobs found.")


def fetch_csv_data(**context):
    """
    Fetch job data from a CSV file.
    """
    CSV_PATH = context.get('CSV_PATH', 'default_path_if_not_provided')
    JOB_ROLE = context.get('JOB_ROLE', 'default_role_if_not_provided')
    jobs = fetch_csv(CSV_PATH, JOB_ROLE)
    context['task_instance'].xcom_push(key='raw_data', value=jobs)
    if jobs:
        for job in jobs:
            print(job)
    else:
        print("No jobs found.")


def upload_dataset(**context):
    """
    Upload job data to the database.
    """
    ti = context['task_instance']
    jobs_dict_list = ti.xcom_pull(task_ids=['fetch_rss', 'fetch_api', 'fetch_file'], key='raw_data')
    jobs_dict_list = [job_dict for job_dict in jobs_dict_list if job_dict]

    jobs = [Job.from_dict(job_dict) for sublist in jobs_dict_list for job_dict in sublist]

    # Clean the salary field in the dictionary
    for job in jobs:
        job.salary = clean_and_format_salary(job.salary)

    if jobs:
        pg_hook = PostgresHook(postgres_conn_id='postgres_localhost')
        insert_sql = """
            INSERT INTO jobs (timestamp, title, company, link, job_type, region, salary)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (timestamp, title)
            DO UPDATE SET
                company = EXCLUDED.company,
                link = EXCLUDED.link,
                job_type = EXCLUDED.job_type,
                region = EXCLUDED.region,
                salary = EXCLUDED.salary;
        """
        for job in jobs:
            pg_hook.run(insert_sql, parameters=(
                job.timestamp, job.title, job.company, job.link, job.job_type, job.region, job.salary))
    else:
        print("No jobs to upload.")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'fetch_dag_v1',
    default_args=default_args,
    description='A simple fetch DAG',
    schedule_interval='@daily',
    catchup=False,
)

fetch_jobs_task1 = PythonOperator(
    task_id='fetch_rss',
    python_callable=fetch_rss_data,
    op_kwargs={
        'RSS_URL': "https://weworkremotely.com/categories/all-other-remote-jobs.rss",
        'JOB_ROLE': "data engineer"
    },
    provide_context=True,
    dag=dag,
)

fetch_jobs_task2 = PythonOperator(
    task_id='fetch_api',
    python_callable=fetch_api_data,
    op_kwargs={
        'API_URL': "https://remotive.com/api/remote-jobs",
        'JOB_ROLE': "data engineer"
    },
    provide_context=True,
    dag=dag,
)

# Define FileSensor to check for CSV file existence
file_sensor_task = FileSensor(
    task_id='file_sensor_task',
    filepath='jobs.csv',
    fs_conn_id='fs_default',
    poke_interval=10,
    timeout=600,
    dag=dag,
)

fetch_jobs_task3 = PythonOperator(
    task_id='fetch_file',
    python_callable=fetch_csv_data,
    op_kwargs={
        'CSV_PATH': "/opt/airflow/data/jobs.csv",
        'JOB_ROLE': "data engineer"
    },
    provide_context=True,
    dag=dag,
)

upload_dataset_task = PythonOperator(
    task_id='upload_dataset',
    python_callable=upload_dataset,
    provide_context=True,
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_localhost',
    sql="""
    CREATE TABLE IF NOT EXISTS jobs (
        timestamp VARCHAR,
        title VARCHAR,
        company VARCHAR,
        link VARCHAR,
        job_type VARCHAR,
        region VARCHAR,
        salary VARCHAR,
        PRIMARY KEY (timestamp, title)
    );
    """,
    dag=dag,
)

create_views_task = PostgresOperator(
    task_id='create_views',
    postgres_conn_id='postgres_localhost',
    sql="""
    -- Check if the views exist and create them if they do not
    DO $$ 
    BEGIN
        -- New Jobs View
        IF NOT EXISTS (SELECT 1 FROM pg_views WHERE viewname = 'new_jobs_per_day') THEN
            CREATE VIEW new_jobs_per_day AS
            SELECT
                DATE(timestamp) AS date,
                COUNT(*) AS new_job_ads
            FROM
                jobs
            WHERE
                title ILIKE '%data engineer%'
            GROUP BY
                DATE(timestamp);
        END IF;

        -- Remote Jobs View
        IF NOT EXISTS (SELECT 1 FROM pg_views WHERE viewname = 'remote_jobs_per_day') THEN
            CREATE VIEW remote_jobs_per_day AS
            SELECT
                DATE(timestamp) AS date,
                COUNT(*) AS remote_job_ads
            FROM
                jobs
            WHERE
                title ILIKE '%data engineer%'
                AND (
                    region ILIKE '%remote%'
                    OR region ILIKE '%worldwide%'
                )
            GROUP BY
                DATE(timestamp);
        END IF;

        -- Total Salary Statistics View
        IF NOT EXISTS (SELECT 1 FROM pg_views WHERE viewname = 'total_salary_statistics') THEN
            CREATE VIEW total_salary_statistics AS
            SELECT
                MAX(CAST(salary AS numeric)) AS max_salary,
                MIN(CAST(salary AS numeric)) AS min_salary,
                AVG(CAST(salary AS numeric)) AS avg_salary,
                STDDEV(CAST(salary AS numeric)) AS stddev_salary
            FROM
                jobs
            WHERE
                title ILIKE '%data engineer%'
                AND salary ~ '^[0-9]+$';  -- Only numeric values (non-range) are considered
        END IF;

        -- Daily Salary Statistics View
        IF NOT EXISTS (SELECT 1 FROM pg_views WHERE viewname = 'daily_salary_statistics') THEN
            CREATE VIEW daily_salary_statistics AS
            SELECT
                DATE(timestamp) AS date,
                MAX(CAST(salary AS numeric)) AS max_salary,
                MIN(CAST(salary AS numeric)) AS min_salary,
                AVG(CAST(salary AS numeric)) AS avg_salary,
                STDDEV(CAST(salary AS numeric)) AS stddev_salary
            FROM
                jobs
            WHERE
                title ILIKE '%data engineer%'
                AND salary ~ '^[0-9]+$'  -- Only numeric values (non-range) are considered
            GROUP BY
                DATE(timestamp);
        END IF;
    END $$;
    """,
    dag=dag,
)

create_table_task >> create_views_task >> [fetch_jobs_task1, fetch_jobs_task2, fetch_jobs_task3]
file_sensor_task >> fetch_jobs_task3
[fetch_jobs_task1, fetch_jobs_task2, fetch_jobs_task3] >> upload_dataset_task
