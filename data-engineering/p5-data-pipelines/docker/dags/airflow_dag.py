from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from helpers import SqlQueries
from operators.data_quality import DataQualityOperator
from operators.load_dimension import LoadDimensionOperator
from operators.load_fact import LoadFactOperator
from operators.stage_redshift import StageToRedshiftOperator

REDSHIFT_CONN_ID = 'redshift'
AWS_CREDENTIALS_ID = 'aws_credentials'
S3_BUCKET = "udacity-lake-hkn"

# merged log files into single jsonl inorder to create quick feedback loop
LOG_DATA = 'logs_combined.jsonl'
SONG_DATA = 'songs_combined.jsonl'
REGION = 'us-east-2'

# debug flag for dropping and creating databases
create_database = True

"""
dag arguments:
    - doesn't have dependency on the past runs
    - on fail tasks are retried 3 times
    - retry interval is 5 minutes
    - catchup is disabled
    - doesn't email on retry
"""
default_args = {
    'owner': 'hozgur',
    'start_date': datetime(2021, 4, 3),
    'retry_delay': timedelta(minutes=5),
    'retries': 3,
    'catchup': False,
    'depends_on_past': False,
    'email_on_retry': False,
}

# Hourly scheduled dag
dag = DAG('airflow_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          max_active_runs=1,
          tags=['project', 'udacity']
          )

# Dummy operator prints the start date
BashOperator.ui_color = '#caf0f8'
start_operator = BashOperator(
    task_id='begin_execution',
    depends_on_past=False,
    bash_command='date',
    dag=dag
)

# Copies json formatted log data form s3 to aws redshift
stage_events_to_redshift = StageToRedshiftOperator(
    task_id="stage_events_to_redshift",
    dag=dag,
    table="staging_events",
    redshift_conn_id=REDSHIFT_CONN_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    s3_bucket=S3_BUCKET,
    s3_key=LOG_DATA,
    region=REGION,
    format='json',
    json='s3://udacity-lake-hkn/log_json_path.json'
)

# Copies json formatted song data form s3 to aws redshift
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="stage_songs_to_redshift",
    dag=dag,
    table="staging_songs",
    redshift_conn_id=REDSHIFT_CONN_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    s3_bucket=S3_BUCKET,
    s3_key=SONG_DATA,
    region=REGION,
    format='json'
)

# Load methods inserts data from staging tables to fact and dimension tables
load_songplays_table = LoadFactOperator(
    task_id='load_songplays_fact_table',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    table="songplays",
    sql=SqlQueries.songplay_table_insert,
    overwrite=True
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='load_user_dim_table',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    table="users",
    sql=SqlQueries.user_table_insert,
    overwrite=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='load_song_dim_table',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    table="songs",
    sql=SqlQueries.song_table_insert,
    overwrite=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='load_artist_dim_table',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    table="artists",
    sql=SqlQueries.artist_table_insert,
    overwrite=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='load_time_dim_table',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    table="time",
    sql=SqlQueries.time_table_insert,
    overwrite=True,
)

# Runs quality check on tables, if a table doesn't have any value in it a value exception is raised
# Null checks and count checks
dq_checks=[
    {'check_sql': 'select count(*) from users where userid is null', 'expected_result': 0, 'operator': '='},
    {'check_sql': 'select count(*) from songs where songid is null', 'expected_result': 0, 'operator': '='},
    {'check_sql': 'select count(*) from songplays where playid is null', 'expected_result': 0, 'operator': '='},
    {'check_sql': 'select count(*) from artists where artistid is null', 'expected_result': 0, 'operator': '='},
    {'check_sql': 'select count(*) from time where start_time is null', 'expected_result': 0, 'operator': '='},

    {'check_sql': 'select count(*) from users', 'expected_result': 0, 'operator': '>'},
    {'check_sql': 'select count(*) from songs', 'expected_result': 0, 'operator': '>'},
    {'check_sql': 'select count(*) from songplays', 'expected_result': 0, 'operator': '>'},
    {'check_sql': 'select count(*) from artists', 'expected_result': 0, 'operator': '>'},
    {'check_sql': 'select count(*) from time', 'expected_result': 0, 'operator': '>'},
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

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

# load fact table
stage_songs_to_redshift >> load_songplays_table
stage_events_to_redshift >> load_songplays_table

# dimension tables can be created in parallel
load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks

# wait all for quality checks and finish
run_quality_checks >> end_operator
