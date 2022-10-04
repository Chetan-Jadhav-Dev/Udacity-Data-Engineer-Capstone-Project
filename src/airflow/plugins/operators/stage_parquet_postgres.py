"""Airflow Operator to load Parquet data from files into PostgreSQL staging table"""
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd


class StageParquetToPostgresOperator(BaseOperator):
    """
    Operator to load Parquet data from file into PostgreSQL

    :param postgres_conn_id: Connection id of the PostgreSQL connection to use
    :type postgres_conn_id: str
    
    :param table_name: Postgres staging table name
    :type table_name: str

    :param parquet_path: Path to Parquet data
    :type parquet_path: str

    """
    ui_color = '#358140'

    template_fields = ['parquet_path']

    @apply_defaults
    def __init__(self,
                 postgres_conn_id,
                 parquet_path,
                 table_name,
                 truncate_table=False,
                 *args, **kwargs):
        super(StageParquetToPostgresOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.table_name = table_name
        self.truncate_table = truncate_table
        self.parquet_path = parquet_path

    def execute(self, context):

        postgres_hook = PostgresHook(self.postgres_conn_id)
        self.log.info(f"Reading data from path {self.parquet_path}")
        df = pd.read_parquet(self.parquet_path)

        self.log.info(f"Copying data to staging table: {self.table_name}")
        engine = postgres_hook.get_sqlalchemy_engine()
        if_exists = 'replace' if self.truncate_table is True else 'append'
        df.to_sql(self.table_name, engine, if_exists=if_exists)
        self.log.info(f"Copying completed for table: {self.table_name}")
