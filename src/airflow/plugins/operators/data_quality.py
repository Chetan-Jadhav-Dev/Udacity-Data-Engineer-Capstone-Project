"""Performs data quality checks on data in Redshift"""

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
    :param conn_id: Connection id of the database connection to use
    :type conn_id: str

    :param sql_check_query: SQL query for data quality check
    :type sql_check_query: str

    :param expected_results: Lambda expression for predicates to validate data quality query results
        i.e. [lambda num_results: num_results > 0]
    :type expected_results: function

    """
    ui_color = '#89DA59'

    template_fields = ['sql_check_query']

    @apply_defaults
    def __init__(self,
                 conn_id,
                 sql_check_query,
                 expected_results,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.sql_check_query = sql_check_query
        self.expected_results = expected_results

    def execute(self, context):
        postgres_hook = PostgresHook(self.conn_id)
        self.log.info(f"Executing data quality check: {self.sql_check_query}")
        records = postgres_hook.get_records(self.sql_check_query)
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {self.sql_check_query} returned no results")
        num_records = records[0][0]
        if not self.expected_results(num_records):
            raise ValueError(
                f"Data quality check failed. {self.sql_check_query} expected value did not match returned {num_records}")
        self.log.info(f"Data quality query {self.sql_check_query} check passed with expected criteria")
