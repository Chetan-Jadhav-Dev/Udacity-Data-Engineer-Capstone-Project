"""Airflow Operator to load CSV data from file into PostgreSQL staging table"""

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageCsvToPostgresOperator(BaseOperator):
    """
    Operator to load CSV data from file into PostgreSQL

    :param postgres_conn_id: Connection id of the PostgreSQL connection to use
    :type postgres_conn_id: str
    
    :param table_name: Postgres staging table name
    :type table_name: str

    :param csv_path: Path to CSV file
    :type csv_path: str

    :param delimiter: CSV delimiter
    :type delimiter: str
        
    """
    ui_color = '#358140'

    COPY_SQL = """
    COPY {}
    FROM '{}'
    CSV HEADER
    DELIMITER '{}'
    """

    TRUNCATE_SQL = """
        TRUNCATE TABLE {};
        """

    @apply_defaults
    def __init__(self,
                 postgres_conn_id,
                 csv_path,
                 table_name,
                 delimiter=",",
                 truncate_table=False,
                 *args, **kwargs):
        super(StageCsvToPostgresOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.table_name = table_name
        self.truncate_table = truncate_table
        self.csv_path = csv_path
        self.delimiter = delimiter

    def execute(self, context):
        postgres_hook = PostgresHook(self.postgres_conn_id)

        if self.truncate_table:
            # Truncate table
            self.log.info(f"Truncating table {self.table_name}")
            postgres_hook.run(self.TRUNCATE_SQL.format(self.table_name))

        sql_stmt = self.COPY_SQL.format(
            self.table_name,
            self.csv_path,
            self.delimiter
        )

        self.log.info(f"Copying data to staging table: {self.table_name}")
        postgres_hook.run(sql_stmt)
        self.log.info(f"Copying completed for table: {self.table_name}")
