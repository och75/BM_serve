from datetime import datetime
from datetime import timedelta
from logging import getLogger

import os

from google.cloud import bigquery

LOGGER = getLogger('airflow.task')


def get_bq_client(project_id='och-sandbox'):
    return bigquery.Client(project=project_id)


def create_ext_table_from_gcs_csv(table_name, blob_names, dataset_name, project_id='och-sandbox'):
    """
    create a Bigquery external table to expose a CSV file (comma separated) located in a GCS bucket
    :param table_name:
    :param blob_names: List of blob paths (can include wild cards) a blob name is : gs://BUCKET/BLOB_PATH/FILE
    :param dataset_name:
    :param project_id: default is och-sandbox

    :return: object Table created
    """

    client = get_bq_client(project_id)
    table_ref = client.get_dataset(dataset_name).table(table_name)
    table = bigquery.Table(table_ref)

    external_config = bigquery.ExternalConfig(bigquery.ExternalSourceFormat.CSV)
    external_config.options.skip_leading_rows = 1
    external_config.source_uris = blob_names
    external_config.autodetect = True

    table.external_data_configuration = external_config
    table.expires = datetime.now() + timedelta(days=7)

    table_created = client.create_table(table)

    return table_created


def get_table(dataset_name, table_name, project_id='och-sandbox'):
    """
    get a BigQuery Table.
    :param dataset_name: dataset name holding the table
    :param table_name: table name
    :param project_id: project on which the dataset is
    :return: Table object if exists or None
    """
    client = get_bq_client(project_id)
    table_ref = client.get_dataset(dataset_name).table(table_name)
    try:
        returned_table = client.get_table(table_ref)
    except Exception as e:
        returned_table = None

    return returned_table


def drop_a_table(dataset_name, table_name, project_id='och-sandbox'):
    """
    drop a table in Bigquery
    :param dataset_name:
    :param table_name:
    :return: True if table were dropped, False if Table did not exist
    """

    client = get_bq_client(project_id)
    table_ref = client.get_dataset(dataset_name).table(table_name)

    re = False
    try:
        client.delete_table(table_ref)
        re = True
    except Exception as e:
        re = False

    return re


def query_execute(file_path, project_id='och-sandbox'):
    """
    execute the query_statement in the file path
    :param file_path : filepath of the query statement to execute
    :param project_id: default is och_sandbox
    :return: row iterator of bigQuery results
    """
    file_path = f'{os.environ["PROJECT_ROOT"]}/{file_path}'

    with open(file_path) as f:
        query_string = f.read()

    job_config = bigquery.QueryJobConfig()
    client = get_bq_client(project_id)
    query_job = client.query(query_string, job_config=job_config)

    rows_iterator = query_job.result()
    return rows_iterator

