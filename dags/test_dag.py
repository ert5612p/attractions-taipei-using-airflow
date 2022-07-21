import json
import requests
import pendulum
from math import inf
from pytz import timezone, utc
from datetime import datetime
from google.cloud import bigquery
from pathlib import Path
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
from airflow.decorators import dag, task

default_args = {
    'owner': 'penny_yang',
}


@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    tags=['pennytest'],
)
def taskflow_api_etl():
    @task()
    def extract():
        once_fetch_count = 30
        total_count, fetch_position, page_param = inf, 0, 1

        url_base = 'https://www.travel.taipei/open-api/zh-tw/Attractions/All?page='
        headers = {
            "accept": "application/json"
        }
        attraction_list = []

        while fetch_position < total_count:
            url = url_base + str(page_param)
            response = requests.get(
                url=url,
                headers=headers
            )
            if response.status_code != 200:
                raise Exception(response.text)
            result = json.loads(response.content)

            attraction_list += result["data"]
            fetch_position += once_fetch_count
            page_param += 1
            if total_count == inf:
                total_count = result["total"]

        if total_count != len(attraction_list):
            raise Exception("Number of attraction list is not equal to total count")

        return attraction_list

    @task()
    def transform(attraction_list: dict):
        current_datetime = datetime.utcnow().replace(tzinfo=utc).astimezone(timezone('Asia/Taipei')).strftime("%Y-%m-%d %H:%M:%S")

        for json_item in attraction_list:
            json_item.update({'import_datetime': current_datetime})
        return attraction_list

    @task()
    def load(attraction_list: dict):
        project_id, dataset_id, table_id = 'pennylab', 'penny_test', 'attractoins_taipei'

        schema_path = Path(__file__).parent / "attractions_schema.json"
        key_path = Path(__file__).parent / "credentials.json"
        print(key_path)
        credentials = service_account.Credentials.from_service_account_file(
            key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id,)

        try:
            bq_client.get_table('%s.%s.%s' % (project_id, dataset_id, table_id))
        except NotFound:
            table = bigquery.Table(
                f'{project_id}.{dataset_id}.{table_id}',
                schema=bq_client.schema_from_json(schema_path)
            )
            table = bq_client.create_table(table)

        job_config = bigquery.LoadJobConfig()
        job_config.ignore_unknown_values = True
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        job_config.schema = bq_client.schema_from_json(schema_path)
        bq_client.load_table_from_json(
            attraction_list,
            project=project_id,
            destination=f'{dataset_id}.{table_id}',
            job_config=job_config,
        ).result()

    attraction_list = extract()
    attraction_list_with_importdatetime = transform(attraction_list)
    load(attraction_list_with_importdatetime)
    # load(attraction_list_with_importdatetime) >> [t1, t2, t3, t4]


attraction_etl_dag = taskflow_api_etl()
