# FastApi

conda activate capstone cd .\apis\eksi\
run.bat

# Airflow

docker-compose -f .\docker\airflow-compose.yaml up -d
http://localhost:8080/   -> airflow:airflow

# Local db

docker-compose -f .\docker\local-db.yaml up -d
