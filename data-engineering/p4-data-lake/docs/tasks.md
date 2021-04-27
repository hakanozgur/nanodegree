# Task
- build ETL pipline
  - extract data from S3
  - store it in Redshift
  - transform into dimensional tables (fact-> songplays, dimensions -> users, songs, artists, time)

## Steps

### Create Table Schemas

- [x] Design schemas for your fact and dimension tables
- [x] Write a SQL CREATE statement for each of these tables in sql_queries.py
- [x] Complete the logic in create_tables.py to connect to the database and create these tables
- [x] Write SQL DROP statements to drop tables at the beginning of create_tables.py if the tables already exist. This way, you can run create_tables.py whenever you want to reset your database and test your ETL pipeline.
- [x] Launch a redshift cluster and create an IAM role that has read access to S3.
- [x] Add redshift database and IAM role info to dwh.cfg.
- [x] Test by running create_tables.py and checking the table schemas in your redshift database. You can use Query Editor in the AWS Redshift console for this.

### Build ETL Pipeline

- [x] Implement the logic in etl.py to load data from S3 to staging tables on Redshift.
- [x] Implement the logic in etl.py to load data from staging tables to analytics tables on Redshift.
- [x] Test by running etl.py after running create_tables.py and running the analytic queries on your Redshift database to compare your results with the expected results.
- [ ] Delete your redshift cluster when finished.


### Document Process
Do the following steps in your README.md file.

- [x] Discuss the purpose of this database in the context of the startup, Sparkify, and their analytical goals.
- [x] State and justify your database schema design and ETL pipeline.
- [ ] [Optional] Provide example queries and results for song play analysis.


### More todos

- [x] Replace Serial with redshift version
- [x] Add distribution keys, sort
- [x] Time epoch issue
- [ ] localstack redshift
