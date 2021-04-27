from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
    executes quality check on each table. if a table doesn't have any rows, value error is raised

    :returns value error if data quality is not met
    """

    ui_color = '#1bbfa9'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_checks=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        if dq_checks is None:
            dq_checks = []
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        # Setup
        redshift_hook = PostgresHook(self.redshift_conn_id)

        # Check data quality queries
        for check in self.dq_checks:
            sql_query = check['check_sql']
            expected_result = check['expected_result']
            operator = check['operator']

            result = redshift_hook.get_records(sql_query)
            if operator == '=':
                if expected_result != result[0][0]:
                    raise ValueError(
                        f"Value Error: expected {expected_result} but received {result[0][0]} for query {sql_query}")
                else:
                    self.log.info(f"Data Quality Query passed: {sql_query}")
            elif operator == '>':
                if result[0][0] <= expected_result:
                    raise ValueError(
                        f"Value Error: expected {result[0][0]} larger than {expected_result} for query {sql_query}")
                else:
                    self.log.info(f"Data Quality Query passed: {sql_query}")
