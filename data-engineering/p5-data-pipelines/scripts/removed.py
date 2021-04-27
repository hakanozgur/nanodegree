# # Drops tables on Redshift
# # Used only for testing
# PostgresOperator.ui_color = '#40798c'
# drop_tables = PostgresOperator(
#     task_id='drop_tables',
#     dag=dag,
#     postgres_conn_id='redshift',
#     sql='drop_tables.sql'
# )


# # Creates table on Redshift
# create_tables = PostgresOperator(
#     task_id='create_tables',
#     dag=dag,
#     postgres_conn_id='redshift',
#     sql='create_tables.sql'
# )

# this note branches execution according to dev state
# while testing I needed to drop and create the tables so I used this function
# if not in debug mode, execution continues from staging event to redshift
# def create_databases():
#     if create_database:
#         return 'drop_tables'
#     else:
#         return 'stage_events_to_redshift'


# is_debug = BranchPythonOperator(
#     task_id='is_debug',
#     python_callable=create_databases,
#     dag=dag,
# )

# start
# start_operator >> is_debug

# debug control
# is_debug >> drop_tables >> create_tables >> stage_events_to_redshift
# is_debug >> drop_tables >> create_tables >> stage_songs_to_redshift
# is_debug >> stage_events_to_redshift
# is_debug >> stage_songs_to_redshift


# # iterate on tables
# for table in self.tables:
#     result = redshift_hook.get_records(f"select count(*) from {table}")
#     if not result or len(result) < 1 or len(result[0]) < 1:
#         raise ValueError(f"Value Error: {table} returned no results")
#
#     count = result[0][0]
#     if count < 1:
#         raise ValueError(f"Value Error: {table} is empty")
#     self.log.warning(f"{table} has : {count} rows")