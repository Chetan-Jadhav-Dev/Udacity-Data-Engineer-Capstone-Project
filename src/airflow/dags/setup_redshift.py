"""Apache Airflow DAG to setup database schema in Postgres database and import static staging data"""

import datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import StageToRedshiftOperator
from helpers import RedshiftSqlQueries

DATA_LAKE_PATH = "s3://msf-capstone"
DATABASE_CONNECTION_ID = "capstone-redshift"
AWS_CREDENTIALS_ID = "aws_credentials"

default_args = {"owner": "udacity"}

dag = DAG('setup_redshift',
          default_args=default_args,
          description='Create table schema in database and load static staging data',
          schedule_interval=None,
          start_date=datetime.datetime(2019, 1, 1),
          concurrency=3
          )

# Create staging table for immigration data
create_staging_immigration_table = PostgresOperator(
    task_id="create_staging_immigration_table",
    dag=dag,
    postgres_conn_id=DATABASE_CONNECTION_ID,
    sql=RedshiftSqlQueries.create_staging_immigration
)

# Create staging table for country data
create_staging_countries_table = PostgresOperator(
    task_id="create_staging_countries_table",
    dag=dag,
    postgres_conn_id=DATABASE_CONNECTION_ID,
    sql=RedshiftSqlQueries.create_staging_countries
)

# Create staging table fot port data
create_staging_ports_table = PostgresOperator(
    task_id="create_staging_ports_table",
    dag=dag,
    postgres_conn_id=DATABASE_CONNECTION_ID,
    sql=RedshiftSqlQueries.create_staging_ports_table
)

# Create staging table for airport data
create_staging_airports_table = PostgresOperator(
    task_id="create_staging_airports_table",
    dag=dag,
    postgres_conn_id=DATABASE_CONNECTION_ID,
    sql=RedshiftSqlQueries.create_staging_airports_table
)

# Create staging table for demographics data
create_staging_demographics_table = PostgresOperator(
    task_id="create_staging_demographics_table",
    dag=dag,
    postgres_conn_id=DATABASE_CONNECTION_ID,
    sql=RedshiftSqlQueries.create_staging_demographics_table
)

# Create dimension table for country data
create_countries_table = PostgresOperator(
    task_id="create_countries_table",
    dag=dag,
    postgres_conn_id=DATABASE_CONNECTION_ID,
    sql=RedshiftSqlQueries.create_countries_table
)

# Create dimension table for port data
create_ports_table = PostgresOperator(
    task_id="create_ports_table",
    dag=dag,
    postgres_conn_id=DATABASE_CONNECTION_ID,
    sql=RedshiftSqlQueries.create_ports_table
)

# Create dimension table for airport data
create_airports_table = PostgresOperator(
    task_id="create_airports_table",
    dag=dag,
    postgres_conn_id=DATABASE_CONNECTION_ID,
    sql=RedshiftSqlQueries.create_airports_table
)

# Create dimension table for demographics data
create_demographics_table = PostgresOperator(
    task_id="create_demographics_table",
    dag=dag,
    postgres_conn_id=DATABASE_CONNECTION_ID,
    sql=RedshiftSqlQueries.create_demographics_table
)

# Create dimension table for time data
create_time_table = PostgresOperator(
    task_id="create_time_table",
    dag=dag,
    postgres_conn_id=DATABASE_CONNECTION_ID,
    sql=RedshiftSqlQueries.create_time_table
)

# Create fact table for immigration data
create_fact_immigration_table = PostgresOperator(
    task_id="create_fact_immigration_table",
    dag=dag,
    postgres_conn_id=DATABASE_CONNECTION_ID,
    sql=RedshiftSqlQueries.create_fact_immigration_table
)

# Create city temperatures staging table
create_staging_city_temps_table = PostgresOperator(
    task_id="create_staging_city_temps_table",
    dag=dag,
    postgres_conn_id=DATABASE_CONNECTION_ID,
    sql=RedshiftSqlQueries.create_staging_city_temps_table
)

# Create country temperatures staging table
create_staging_country_temps_table = PostgresOperator(
    task_id="create_staging_country_temps_table",
    dag=dag,
    postgres_conn_id=DATABASE_CONNECTION_ID,
    sql=RedshiftSqlQueries.create_staging_country_temps_table
)

# Load port data into staging table
stage_port_codes_task = StageToRedshiftOperator(
    task_id="stage_port_codes",
    redshift_conn_id=DATABASE_CONNECTION_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    dag=dag,
    table_name="public.staging_ports",
    s3_path=f"{DATA_LAKE_PATH}/i94_ports.csv",
    copy_format='CSV IGNOREHEADER 1',
    truncate_table=True
)

# Load country codes into staging table
stage_country_codes_task = StageToRedshiftOperator(
    task_id="stage_country_codes",
    dag=dag,
    redshift_conn_id=DATABASE_CONNECTION_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    table_name='staging_countries',
    s3_path=f'{DATA_LAKE_PATH}/i94_countries.csv',
    copy_format='CSV IGNOREHEADER 1',
    truncate_table=False,
)

# Load airport data into staging table
stage_airports_codes_task = StageToRedshiftOperator(
    task_id="stage_airports_codes",
    redshift_conn_id=DATABASE_CONNECTION_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    dag=dag,
    table_name="staging_airports",
    s3_path=f"{DATA_LAKE_PATH}/airport-codes_csv.csv",
    copy_format='CSV IGNOREHEADER 1',
    truncate_table=True
)

# Load demographics data into staging table
stage_demographics_task = StageToRedshiftOperator(
    task_id="stage_demographics",
    redshift_conn_id=DATABASE_CONNECTION_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    dag=dag,
    table_name="staging_demographics",
    s3_path=f"{DATA_LAKE_PATH}/us-cities-demographics.csv",
    truncate_table=True,
    copy_format="CSV DELIMITER ';' IGNOREHEADER 1",
)

#  Load city temperature data into staging table
stage_city_temperatures_task = StageToRedshiftOperator(
    task_id="stage_city_temperatures",
    dag=dag,
    redshift_conn_id=DATABASE_CONNECTION_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    s3_path=f"{DATA_LAKE_PATH}/us_city_temperature_data/*.parquet",
    table_name="staging_city_temperatures",
    copy_format="PARQUET",
    truncate_table=True
)

#  Load country temperature data into staging table
stage_country_temperatures_task = StageToRedshiftOperator(
    task_id="stage_country_temperatures",
    dag=dag,
    redshift_conn_id=DATABASE_CONNECTION_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    s3_path=f"{DATA_LAKE_PATH}/country_temperature_data/*.parquet",
    table_name="staging_country_temperatures",
    copy_format="PARQUET",
    truncate_table=True
)


# Setup DAG pipeline
start_operator = DummyOperator(task_id='Begin_execution', dag=dag)
end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> create_staging_immigration_table
start_operator >> create_staging_countries_table
start_operator >> create_staging_ports_table
start_operator >> create_staging_airports_table
start_operator >> create_staging_demographics_table
start_operator >> create_time_table
start_operator >> create_ports_table
start_operator >> create_countries_table
start_operator >> create_staging_city_temps_table
start_operator >> create_staging_country_temps_table

create_ports_table >> create_airports_table
create_ports_table >> create_demographics_table

create_staging_countries_table >> stage_country_codes_task
create_staging_ports_table >> stage_port_codes_task
create_staging_airports_table >> stage_airports_codes_task
create_staging_demographics_table >> stage_demographics_task

create_time_table >> create_fact_immigration_table
create_ports_table >> create_fact_immigration_table
create_countries_table >> create_fact_immigration_table

create_staging_city_temps_table >> stage_city_temperatures_task
create_staging_country_temps_table >> stage_country_temperatures_task

create_staging_immigration_table >> end_operator
stage_country_codes_task >> end_operator
stage_port_codes_task >> end_operator
stage_airports_codes_task >> end_operator
stage_demographics_task >> end_operator
stage_city_temperatures_task >> end_operator
stage_country_temperatures_task >> end_operator
