from utils.http_util import copy_file_from_url_to_gcs
from utils.bq_util import create_ext_table_from_gcs_csv as create_ext_table, drop_a_table, query_execute

from logging import getLogger
from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

BUCKET_NAME = 'staging_product_catalog'
LOGGER = getLogger('airflow.task')
GCP_PROJECT = 'och-sandbox'
DATASET_NAME = 'staging'

args = {
    'depends_on_past': True,
    'retries': 2,
    'start_date': datetime(2022, 5, 1, 3, 0),
    'retry_delay': timedelta(minutes=5),
}

# listing the teams and their dedicated data processing
ls_teams = {
    'team_a': 'sql/product_team_a.sql',
    'team_b': 'sql/product_team_b.sql'

}

# listing files to import
ls_files = {
    'product_catalog': {
        'url': 'https://backmarket-data-jobs.s3-eu-west-1.amazonaws.com/data/product_catalog.csv',
        'file_name': 'product_catalog.csv',
        'bucket_name': BUCKET_NAME,
        'blob_path': 'product_catalog/'
    },
    'product_dimensions': {
        'url': 'FAKE_URL',
        'file_name': 'product_dimensions.csv',
        'bucket_name': BUCKET_NAME,
        'blob_path': 'product_dimensions/'
    }
}

with DAG(dag_id='expose_product_catalog', schedule_interval='0 3 * * *', default_args=args) as dag:
    dict_op = {
        'end_files_ingestion': DummyOperator(task_id='end_files_ingestion'),
        'end_drop_create_table': DummyOperator(task_id='end_drop_create_table'),
    }

    #generating operators for each files to ingest
    for file, conf in ls_files.items():
        op_name_copy = f'copy_file_{file}'
        op_name_drop = f'drop_table_{file}'
        op_name_create = f'create_table_{file}'
        table_name = f'gcs_{file}'

        dict_op[op_name_copy] = PythonOperator(
            task_id=op_name_copy,
            python_callable=copy_file_from_url_to_gcs,
            op_kwargs=conf
        )
        dict_op[op_name_copy] >> dict_op['end_files_ingestion']

        dict_op[op_name_drop] = PythonOperator(
            task_id=op_name_drop,
            python_callable=drop_a_table,
            op_kwargs={
                'dataset_name': DATASET_NAME,
                'table_name': table_name
            }
        )

        dict_op[op_name_create] = PythonOperator(
            task_id=op_name_create,
            python_callable=create_ext_table,
            op_kwargs={
                'table_name': table_name,
                'blob_names': [f'gs://{BUCKET_NAME}/{conf["blob_path"]}/{conf["file_name"]}'],
                'dataset_name': DATASET_NAME
            }
        )

        dict_op['end_files_ingestion'] >> dict_op[op_name_drop] >> dict_op[op_name_create] >> dict_op['end_drop_create_table']

    #generating operators to lauch sql for each team
    for team, sql_file in ls_teams.items():
        op_name_team = f'query_execute_{team}'
        dict_op[op_name_team] = PythonOperator(
            task_id=op_name_team,
            python_callable=query_execute,
            op_kwargs={
                'file_path': sql_file,
            }
        )
        dict_op['end_drop_create_table'] >> dict_op[op_name_team]




