## Docker

docker-compose.exe -f .\bitnami-spark.yaml up -d docker-compose.exe -f .\data-europe-spark.yml up -d

## Urls

spark master -> http://localhost:8080/
http://localhost:9080/

hadoop nameserver -> http://localhost:9000/
hadoop ui interface -> http://localhost:9870/

## Install

pip install hdfs pip install spark-nlp==3.0.1 pyspark==3.1.1

## Cmd

hadoop fs -ls