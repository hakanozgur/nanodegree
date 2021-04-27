from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
        Airflow plugin that copies files from s3 into redshift

        Note: deprecated AwsHook and PostgresHook replaced with provides
        requires
            - pip install apache-airflow[postgres]
            - pip install apache-airflow[amazon]
    """

    ui_color = '#4d9be3'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json="auto",
                 file_format="json",
                 region='us-west-2',
                 time_format='epochmillisecs',
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json = json
        self.region = region
        self.file_format = file_format
        self.time_format = time_format
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):

        if not (self.file_format == 'csv' or self.file_format == 'json'):
            raise ValueError(f'{self.file_format} is not supported, expecting json or csv')
        if self.file_format == 'json':
            s3_file_format = "format json '{}'".format(self.json)
        else:
            s3_file_format = "format CSV"

        # Prepare hooks
        aws_hook = AwsBaseHook(self.aws_credentials_id, client_type='redshift')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        credentials = aws_hook.get_credentials()

        # Clear the table
        self.log.info(f"deleting the data inside the {self.table} table ")
        try:
            redshift.run(f"truncate table {self.table}")
        except Exception as ex:
            self.log.warning(f"error clearing table {ex}")

        # Prepare redshift copy command
        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"
        copy_sql = f"""
            copy {self.table}
            from '{s3_path}'
            access_key_id '{credentials.access_key}' 
            secret_access_key '{credentials.secret_key}'
            region '{self.region}'
            {s3_file_format};
        """

        # Execute query on redshift
        try:
            self.log.info(f"copying data from => {s3_path}")
            redshift.run(copy_sql)
            self.log.warning(f"{self.table} table inserted")
        except Exception as ex:
            self.log.error(f'error on insert {self.table} table, exception: {ex}')
            raise ex
