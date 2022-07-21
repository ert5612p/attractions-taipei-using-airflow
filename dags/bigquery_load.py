import os
from datetime import datetime

from airflow import models
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryGetDataOperator,
    BigQueryInsertJobOperator,
    BigQueryIntervalCheckOperator,
    BigQueryValueCheckOperator,
    BigQueryGetDatasetTablesOperator,
)
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.utils.trigger_rule import TriggerRule

# QUERY_SQL_PATH = "dags/sqlScript/attractions_information.sql"

with models.DAG(
    'bigquery_queries_location',
    schedule_interval="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "bigquery"],
    # user_defined_macros={"DATASET": "penny_test", "TABLE": "test", "QUERY_SQL_PATH": ""},
) as dag:
    # get_dataset_tables = BigQueryGetDatasetTablesOperator(
    #     task_id="get_dataset_tables", dataset_id='penny_test'
    # )

    t1 = BigQueryOperator(
        task_id='bigquery_test',
        sql='sql_script/attractions_information.sql',
        destination_dataset_table='pennylab.penny_test.bigquery_operator_test',
        write_disposition='WRITE_TRUNCATE',
        use_legacy_sql=False,
        dag=dag,
        )
    t1
