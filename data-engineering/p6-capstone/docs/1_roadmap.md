# Intro

Decided to work on a local social network similar to Reddit

# Data Gathering

`/scripts/0_data_gathering`

- [x] Examine crawling options
    - no open API or available data download options
    - site is public, crawling is possible
- [x] Examine site data structure
    - entries, topics, and authors main data objects
- [x] Create crawling logic
    - get today's topics
    - get entries from topics
    - get authors from entries
    - from the author's profile get more topics and entries
    - [x] crawl in this loop
- [x] Download data
- [x] Convert raw data into models
    - decided to use MongoDB for saving data as raw JSON

# Prepare Staging Tables

`/scripts/1_staging_tables`  
`/scripts/2_preprocess`

- [x] Create sample data for testing
- [x] Examine the staging tables
- [x] Preprocess data
    - remove unnecessary columns
    - expand nested models
- [x] Prepare staging create_table sql queries
    - [x] test with local db

# Prepare Star Schema

`/scripts/3_star_schema`  
`/scripts/4_etl_with_all_data`

- [x] Create sample data for testing
- [x] Draw the schema on sqldbm
- [x] Create star schema
- [x] Write create and drop SQL queries for star schema
- [x] Write insert queries
    - [x] convert types
- [x] Test ETL pipeline locally
- [x] Insert entire data
    - [x] bugfixes
- [x] Combine scripts into one script

# Spark

`/scripts/5_spark`  
Stopped working on SparkNLP.

- Spark doesn't play well with windows and sparkNLP makes it even more complicated.
- I need a smaller dataset for faster exploration (possibly start with pandas and move to spark)
- Pretrained models don't include a good Turkish NLP model
- Adding models from huggingface requires more steps than I thought and exploration
- I will continue after I switch the workstation.

- [x] Create local test environment
- [x] Load data from newly created star schema
- [x] Drop duplicates
- [x] Drop columns
- [x] Learn about SparkNLP
    - [x] docs
    - [x] hello world
    - [ ] hello sentiment analysis
    - [ ] language models
    - [ ] sentiment on turkish model
- [ ] Process titles
    - [ ] NER on title
        - [ ] get dates
        - [ ] get person names
        - [ ] get other NER tags (org, loc...)
    - [ ] Sentiment on title
- [ ] Process entries
- [ ] Create new tables from processed data
- [ ] Prepare data validation tasks

# Data Analysis

## Automate Gathering

- [x] Create a method for daily topic parse
    - docker airflow doesn't play well with local dev env
    - expose as API with fastapi
- [x] Create a method for daily entry and author parse
    - [x] expose API
- [x] Create dag for API interaction
    - [x] crawl at the end of the day (local time)
    - [x] after daily topics, parse entries
    - [x] preprocess entries
    - [x] write to local file #: todo create a pipeline to put it into DB and process
    - [ ] mail on +x failures

# AWS Setup

`/scripts/6_redshift`

- [x] Create account, setup keys, users, roles
- [x] Prepare s3, upload staging files
- [x] Create sample JSON and upload to s3
- [x] Prepare Redshift
- [x] Create redshift tables

# Airflow

- [x] Create a project
- [x] Setup Airflow environment
- [x] Setup dag flow for automated data gathering
    - [x] write insert to staging DB flow
- [x] task for loading data from s3 into staging tables
- [x] task for transforming data from staging into
- [x] task for quality checks

# Workspace

- [x] export conda environment
- [x] prepare readme
    - [x] read project instructions
    - [x] link to docs
    - [x] add images
    - [ ] add referances

# Cleanup

- [x] remove AWS creds on remote
- [x] remove creds in code
- [x] go over todos


