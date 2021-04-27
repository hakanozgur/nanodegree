"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

import topic_check

logger = logging.getLogger(__name__)

KSQL_URL = "http://localhost:8088"

KSQL_STATEMENT = """
CREATE TABLE turnstile (
    station_id int,
    station_name varchar,
    line varchar
) WITH (
    kafka_topic='org.chicago.cta.turnstile',
    value_format='avro',
    key='station_id'
);

CREATE TABLE TURNSTILE_SUMMARY
WITH (value_format='json') AS
    select station_id, count(station_id) as count
    from turnstile
    group by station_id;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        print("already exists")
        return

    logging.debug("executing ksql statement...")

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    # Ensure that a 2XX status code was returned
    resp.raise_for_status()
    print('done')


if __name__ == "__main__":
    execute_statement()
