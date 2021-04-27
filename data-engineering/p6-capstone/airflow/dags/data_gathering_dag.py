import json
import logging
import time
from datetime import datetime
from datetime import timedelta

import requests
from airflow.operators.python import PythonOperator

from airflow import DAG

default_args = {
    'owner': 'hgozgur',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=10),
}

# runs once at the end of the day
with DAG(
        'data_gathering_dag',
        default_args=default_args,
        description='Daily task to gather new data',
        start_date=datetime(year=2021, month=4, day=18),
        schedule_interval='15 23 * * *',  # todo update date dynamically
        tags=['data gathering'],
) as dag:

    # docker is bridged to access local host endpoint
    CRAWLER_API_ENDPOINT = "http://172.19.48.1:8000"


    def parse_todays_topics(**kwargs):
        """
        parses topics received an entry today

        :param kwargs:
        :return: ValueError if failed
        """
        response = requests.get(CRAWLER_API_ENDPOINT + "/get_todays_topics")

        if response.status_code == requests.codes.ok:
            response_json = json.loads(response.content)
            if response_json['success']:
                logging.info(f"Todays topics passed: {response_json['data']}")
            else:
                raise ValueError('failed to parse tasks' + str(response_json['error']))


    def parse_todays_entries(**kwargs):
        """
        crawls topics and their entries filtered by today

        :param kwargs:
        :return: ValueError if failed
        """
        response = requests.get(CRAWLER_API_ENDPOINT + "/get_entries_from_todays_topics")

        if response.status_code == requests.codes.ok:
            response_json = json.loads(response.content)
            if response_json['success']:
                logging.info(f"Getting today's entries: {response_json['data']}")
            else:
                raise ValueError('failed to parse tasks' + str(response_json['error']))


    def check_parse_complete(**kwargs):
        """
        periodically checks if the parse operation is finished

        :param kwargs:
        :return: ValueError if failed
        """
        retry_in_seconds = 30
        retry_count = 8
        # try 8 times with 30 sec waits
        response_json = ""
        for index in range(retry_count):
            response = requests.get(CRAWLER_API_ENDPOINT + "/check_parse_complete")
            if response.status_code == requests.codes.ok:
                response_json = json.loads(response.content)
                if response_json['success']:
                    logging.info(f"Getting today's entries: {response_json['data']}")
                    return
            time.sleep(retry_in_seconds)
            logging.info(f"retrying in {retry_in_seconds} seconds ")
        raise ValueError('failed to validate parse complete: ' + str(response_json))


    def preprocess_entries(**kwargs):
        """
        changes downloaded entries from raw format to staging format

        :param kwargs:
        :return: ValueError if failed
        """
        response = requests.get(CRAWLER_API_ENDPOINT + "/preprocess_entries")
        if response.status_code == requests.codes.ok:
            response_json = json.loads(response.content)
            if response_json['success']:
                logging.info(f"Preprocessed entries: {response_json['data']}")
                return
            else:
                raise ValueError('Error in preprocessing entries ' + str(response_json))


    def preprocess_topics(**kwargs):
        """
        changes downloaded entries from raw topics to staging format
        todays topics has unused parameters they are removed and missing parameters which requires another api call
        this call is omitted for now #todo get topic slug and creation date

        :param kwargs:
        :return: ValueError if failed
        """
        response = requests.get(CRAWLER_API_ENDPOINT + "/preprocess_topics")
        if response.status_code == requests.codes.ok:
            response_json = json.loads(response.content)
            if response_json['success']:
                logging.info(f"Preprocessed topics: {response_json['data']}")
                return
            else:
                raise ValueError('Error in preprocessing topics ' + str(response_json))


    load_todays_topics = PythonOperator(
        task_id='load_todays_topics',
        python_callable=parse_todays_topics,
    )

    load_todays_entries = PythonOperator(
        task_id='load_todays_entries',
        python_callable=parse_todays_entries,
    )

    check_parse_complete = PythonOperator(
        task_id='check_parse_complete',
        python_callable=check_parse_complete,
    )

    transform_preprocess_entries = PythonOperator(
        task_id='preprocess_entries',
        python_callable=preprocess_entries,
    )

    transform_preprocess_topics = PythonOperator(
        task_id='preprocess_topics',
        python_callable=preprocess_topics,
    )

    load_todays_topics >> load_todays_entries >> check_parse_complete
    check_parse_complete >> transform_preprocess_entries
    check_parse_complete >> transform_preprocess_topics
