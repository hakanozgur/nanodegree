## Local
- docker compose up 
- http://localhost:8080/
- login -> airflow:airflow 
- setup aws connection -> key/secret as id/pass
- docker-compose run airflow-worker airflow info
- docker-compose run airflow-worker airflow list_dags

## Airflow
- pip install apache-airflow[postgres]
- pip install apache-airflow[amazon]
