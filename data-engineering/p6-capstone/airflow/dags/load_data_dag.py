from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from helpers import SqlQueries
from operators.data_quality import DataQualityOperator
from operators.load_dimension import LoadDimensionOperator
from operators.load_fact import LoadFactOperator
from operators.stage_redshift import StageToRedshiftOperator

from airflow import DAG

REDSHIFT_CONN_ID = 'redshift'
AWS_CREDENTIALS_ID = 'aws_credentials'

S3_BUCKET = "capstone-hkn-e"
REGION = 'us-east-2'

FILE_S3_AUTHOR = 'sample_authors.json'
FILE_S3_ENTRY = 'sample_entries.json'
FILE_S3_TOPIC = 'sample_topics.json'

default_args = {
    'owner': 'hozgur',
    'start_date': datetime(2021, 4, 18),
    'retry_delay': timedelta(minutes=5),
    'retries': 3,
    'catchup': False,
    'depends_on_past': False,
    'email_on_retry': False,
}

dag = DAG('load_data_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval=None,
          max_active_runs=1,
          tags=['data load']
          )

# Dummy operator prints the start date
BashOperator.ui_color = '#caf0f8'
start_operator = BashOperator(
    task_id='begin_execution',
    depends_on_past=False,
    bash_command='date',
    dag=dag
)

stage_topics_to_redshift = StageToRedshiftOperator(
    task_id="stage_topics_to_redshift",
    dag=dag,
    table="staging_topics",
    redshift_conn_id=REDSHIFT_CONN_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    s3_bucket=S3_BUCKET,
    s3_key=FILE_S3_TOPIC,
    region=REGION,
    file_format='json',
    json='auto'
)

stage_authors_to_redshift = StageToRedshiftOperator(
    task_id="stage_authors_to_redshift",
    dag=dag,
    table="staging_author_details",
    redshift_conn_id=REDSHIFT_CONN_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    s3_bucket=S3_BUCKET,
    s3_key=FILE_S3_AUTHOR,
    region=REGION,
    file_format='json',
    json='auto'
)

stage_entries_to_redshift = StageToRedshiftOperator(
    task_id="stage_entries_to_redshift",
    dag=dag,
    table="staging_entries",
    redshift_conn_id=REDSHIFT_CONN_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    s3_bucket=S3_BUCKET,
    s3_key=FILE_S3_ENTRY,
    region=REGION,
    file_format='json',
    json='auto'
)

dimension_operator = BashOperator(
    task_id='dimension_operator',
    depends_on_past=False,
    bash_command='date',
    dag=dag
)

load_topics_table = LoadDimensionOperator(
    task_id='load_topics_table',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    table="topics",
    sql=SqlQueries.topics_table_insert,
    overwrite=True
)

load_authors_table = LoadDimensionOperator(
    task_id='load_authors_table',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    table="authors",
    sql=SqlQueries.authors_table_insert,
    overwrite=True
)

load_entry_update_dates_table = LoadDimensionOperator(
    task_id='load_entry_update_dates_table',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    table="entry_update_dates",
    sql=SqlQueries.entry_update_dates_table_insert,
    overwrite=True
)

load_entry_dates_table = LoadDimensionOperator(
    task_id='load_entry_dates_table',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    table="entry_dates",
    sql=SqlQueries.entry_dates_table_insert,
    overwrite=True
)

load_entries_table = LoadFactOperator(
    task_id='load_entries_table',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    table="entries",
    sql=SqlQueries.entry_table_insert,
    overwrite=True
)

# Runs quality check on tables, if a table doesn't have any value in it a value exception is raised
# Null checks and count checks
dq_checks = [
    {'check_sql': 'select count(*) from entries where entry_id is null', 'expected_result': 0, 'operator': '='},
    {'check_sql': 'select count(*) from entry_dates where date_created is null', 'expected_result': 0, 'operator': '='},
    {'check_sql': 'select count(*) from entry_update_dates where date_updated is null', 'expected_result': 0,
     'operator': '='},
    {'check_sql': 'select count(*) from authors where author_id is null', 'expected_result': 0, 'operator': '='},
    {'check_sql': 'select count(*) from topics where topic_id is null', 'expected_result': 0, 'operator': '='},

    {'check_sql': 'select count(*) from entries', 'expected_result': 0, 'operator': '>'},
    {'check_sql': 'select count(*) from entry_dates', 'expected_result': 0, 'operator': '>'},
    {'check_sql': 'select count(*) from entry_update_dates', 'expected_result': 0, 'operator': '>'},
    {'check_sql': 'select count(*) from authors', 'expected_result': 0, 'operator': '>'},
    {'check_sql': 'select count(*) from topics', 'expected_result': 0, 'operator': '>'},
]

run_quality_checks = DataQualityOperator(
    task_id='run_data_quality_checks',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    dq_checks=dq_checks
)

# prints the finish date
end_operator = BashOperator(
    task_id='stop_execution',
    depends_on_past=False,
    bash_command='date',
    dag=dag
)

start_operator >> stage_topics_to_redshift >> dimension_operator
start_operator >> stage_authors_to_redshift >> dimension_operator
start_operator >> stage_entries_to_redshift >> dimension_operator

dimension_operator >> load_topics_table >> load_authors_table
dimension_operator >> load_entry_update_dates_table >> load_authors_table
dimension_operator >> load_entry_dates_table >> load_authors_table

load_authors_table >> load_entries_table >> run_quality_checks >> end_operator
