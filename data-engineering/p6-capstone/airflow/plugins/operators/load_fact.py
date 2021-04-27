from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    """
        On redshift this method executes given insert statement
        using postgres hook and aws credentials
    """
    ui_color = '#8ae3e0'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 overwrite=False,
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.overwrite = overwrite

    def execute(self, context):
        # Prepare connection
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Clear data
        if self.overwrite:
            self.log.info(f"truncating table {self.table}")
            redshift.run(f"truncate table {self.table}")

        # Execute query
        try:
            self.log.info(f"Loading data to {self.table}")
            redshift.run(self.sql)
            self.log.warning(f"{self.table} table inserted")
        except Exception as ex:
            self.log.error(f'error on insert {self.table} table, exception: {ex}')
            raise ex
