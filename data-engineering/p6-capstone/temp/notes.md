## development

(on windows -> conda init powershell && powershell -> Set-ExecutionPolicy RemoteSigned)
install jdk 1.8 -> https://adoptopenjdk.net/?variant=openjdk8&jvmVariant=hotspot

conda create -n capstone python=3.7 -y

conda create -n hkn_capstone python=3.7 -y conda activate hkn_capstone pip install spark-nlp==3.0.1 pyspark==3.1.1 pip
install jupyter pip install pandas

## Architecture

- raw data to s3
- emr
- spark
- s3 parquet

## Cleaning

- null
- double
- filter
- format
- timestamp

## new tables

- dimensions
- genrated new from existing data

## Maybe use

- Amazon Kinesis Data Firehose

## Mix

1/6 -> ---
~20 badges 432 -> tweet profile 320 -> instagram 73 -> facebook 510 -> banned user 1170 -> caylak
~ -> karma levels (karma name and value)

# Commands

https://www.liquid-technologies.com/online-json-to-schema-converter
pip install datamodel-code-generator

datamodel-codegen --input popular_topics.json --input-file-type jsonschema --output popular_topics.py

# Tasks

- popular topics
    - add to db
    - parse topics
        - add entries to db
        - add authors to db

dabbe

- get
- /v2/index/debe/?p1

/index/todayinpast/$year/?p1

# extra data

- city/town names
- football scores
- dolar tl -> uk.investing.com

