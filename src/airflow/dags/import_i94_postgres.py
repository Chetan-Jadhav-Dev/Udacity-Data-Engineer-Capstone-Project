"""Apache Airflow DAG to stage and extract I94 immigration data from data lake to data warehouse"""

from datetime import datetime
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators import StageParquetToPostgresOperator
from airflow.operators import DataQualityOperator
from helpers import PostgresSqlQueries


DATABASE_CONNECTION_ID = "capstone-postgres"
DATA_LAKE_PATH = "/data/datalake"

default_arguments = {
    'owner': 'udacity',
    'description': 'Import immigration staging data to various supporting dimension tables and fact table',
    'start_date': datetime(2016, 5, 1),
    'end_date': datetime(2016, 5, 3),
}

# Partitioned path broken down by year, month, and day or arrival
templated_partition_path = \
    "{{execution_date.strftime('/data/datalake/immigration_data/year=%Y/month=%-m/arrival_day=%-d')}}"

dag = DAG('import_i94_postgres',
          default_args=default_arguments,
          schedule_interval='@daily',
          concurrency=3,
          max_active_runs=1)

# Stage I94 immigration data for partition path to database
stage_immigration_data_task = StageParquetToPostgresOperator(
    task_id="stage_immigration_data",
    dag=dag,
    postgres_conn_id=DATABASE_CONNECTION_ID,
    parquet_path=templated_partition_path,
    table_name="staging_immigration",
    truncate_table=True
)


def extract_ports():
    """
    Extract ports data from staging tables
    :return:
    """
    postgres_hook = PostgresHook(DATABASE_CONNECTION_ID)
    postgres_hook.run(PostgresSqlQueries.extract_ports)


extract_ports_task = PythonOperator(
    task_id="extract_ports",
    dag=dag,
    python_callable=extract_ports
)


def extract_airports():
    """
    Extract airport data from staging tables
    :return:
    """
    postgres_hook = PostgresHook(DATABASE_CONNECTION_ID)
    postgres_hook.run(PostgresSqlQueries.extract_airports)


extract_airports_task = PythonOperator(
    task_id="extract_airports",
    dag=dag,
    python_callable=extract_airports
)


def extract_demographics():
    """
    Extract demographics data from staging tables
    :return:
    """
    postgres_hook = PostgresHook(DATABASE_CONNECTION_ID)
    postgres_hook.run(PostgresSqlQueries.extract_demographics)


extract_demographics_task = PythonOperator(
    task_id="extract_demographics",
    dag=dag,
    python_callable=extract_demographics
)


def extract_time_data():
    """
    Extract time data from staging tables
    :return:
    """
    postgres_hook = PostgresHook(DATABASE_CONNECTION_ID)
    postgres_hook.run(PostgresSqlQueries.extract_time_data)


extract_time_data_task = PythonOperator(
    task_id="extract_time_data",
    dag=dag,
    python_callable=extract_time_data
)


def extract_countries():
    """
    Extract countries data from staging tables
    """
    postgres_hook = PostgresHook(DATABASE_CONNECTION_ID)
    postgres_hook.run(PostgresSqlQueries.extract_countries)


extract_countries_task = PythonOperator(
    task_id="extract_countries",
    dag=dag,
    python_callable=extract_countries
)


def extract_immigration_data():
    """
    Extract I94 immigration data from staging tables
    :return:
    """
    postgres_hook = PostgresHook(DATABASE_CONNECTION_ID)
    postgres_hook.run(PostgresSqlQueries.extract_immigration_data)


extract_immigration_data_task = PythonOperator(
    task_id="extract_immigration_data",
    dag=dag,
    python_callable=extract_immigration_data
)

# Ensure that fact table receives expected number of records
staging_to_fact_data_quality_check = DataQualityOperator(
    task_id='staging_to_fact_data_quality_check',
    dag=dag,
    conn_id=DATABASE_CONNECTION_ID,
    sql_check_query=PostgresSqlQueries.staging_to_fact_data_quality_check,
    expected_results=lambda records_not_inserted: records_not_inserted == 0
)

# Ensure that staging table receives at least 1 record
staging_count_data_quality_check = DataQualityOperator(
    task_id='staging_count_data_quality_check',
    dag=dag,
    conn_id=DATABASE_CONNECTION_ID,
    sql_check_query=PostgresSqlQueries.staging_count_data_quality_check,
    expected_results=lambda records_inserted: records_inserted != 0
)

# Setup DAG pipeline
start_operator = DummyOperator(task_id='Begin_execution', dag=dag)
end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> stage_immigration_data_task >> staging_count_data_quality_check

staging_count_data_quality_check >> extract_ports_task
staging_count_data_quality_check >> extract_time_data_task >> extract_immigration_data_task
staging_count_data_quality_check >> extract_countries_task >> extract_immigration_data_task

extract_ports_task >> extract_airports_task >> extract_immigration_data_task
extract_ports_task >> extract_demographics_task >> extract_immigration_data_task

extract_immigration_data_task >> staging_to_fact_data_quality_check

staging_to_fact_data_quality_check >> end_operator
