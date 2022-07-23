import os
from pathlib import Path
from datetime import datetime

from airflow import models
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryUpdateDatasetOperator,
    BigQueryExecuteQueryOperator,
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
from airflow.decorators import dag, task
import pendulum

default_args = {
    'owner': 'penny_yang',
}


@dag(
    schedule_interval="@once",
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    tags=['bigquery', 'test'],
    # user_defined_macros={"DATASET": "penny_test", "TABLE": "test", "QUERY_SQL_PATH": ""},
)
def trsform_pipeline():
    @task()
    def data_model():
        table_list = ['attractions_tag', 'attractions_tag_list']
        for source_table in table_list:
            BigQueryExecuteQueryOperator(
                task_id=f'{source_table}',
                sql=f"sql_script/{source_table}.sql",
                destination_dataset_table=f'pennylab.penny_test.{source_table}_test',
                write_disposition='WRITE_TRUNCATE',
                use_legacy_sql=False
            )
    # dashboard = BigQueryExecuteQueryOperator(
    #     task_id='attractions_dashboard',
    #     sql='sql_script/attractions_dashboard.sql',
    #     destination_dataset_table='pennylab.penny_test.attractions_dashboard_test',
    #     write_disposition='WRITE_TRUNCATE',
    #     use_legacy_sql=False,
    #     )
    dashboard = BigQueryInsertJobOperator(
        task_id="attractions_dashboard",
        configuration={
            "query": {
                "query": "{% include 'sql_script/attractions_dashboard.sql' %}",
                "useLegacySql": False,
                "destinationTable": {
                    'projectId': 'pennylab',
                    'datasetId': 'penny_test',
                    'tableId': 'attractions_dashboard_test',
                }
            }
        }
    )
    data_model() >> dashboard

trsform_pipeline_dag = trsform_pipeline()
