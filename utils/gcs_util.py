from logging import getLogger
from typing import List

from google.cloud.storage import Client
from google.cloud.storage.bucket import Bucket
from google.cloud.storage.blob import Blob

LOGGER = getLogger('airflow.task')


def get_storage_client(project_id='och-sandbox'):
    return Client(project=project_id)


def create_bucket(bucket_name, project_id='och-sandbox'):
    """
    create a Storage Bucket
    :param bucket_name: name of the bucket to create
    :param project_id: default is och-sandbox
    :return: Bucket Object
    """
    client = get_storage_client(project_id)
    bucket = client.create_bucket(bucket_name, location='EU')
    LOGGER.info(f"{project_id}.{bucket_name} has been created !")
    return bucket


def list_buckets(project_id='och-sandbox'):
    """
    return the list of the IDs of the buckets of the project
    :param project_id: default is och-sandbox
    :return: List of string representing bucket Ids
    """
    client = get_storage_client(project_id)
    bucket_iterator = client.list_buckets()
    ls_bucket_id = [e.id for e in bucket_iterator]
    return ls_bucket_id


def get_blob(bucket_name, blob_name, project_id='och-sandbox'):
    """
    get a blob in a bucket
    :param bucket_name:
    :param blob_name:
    :param project_id: default is och-sandbox
    :return:
    """
    client_storage = get_storage_client(project_id)
    bucket = client_storage.get_bucket(bucket_name)
    return bucket.get_blob(blob_name)


def list_blobs(bucket_name, prefix=None, project_id='och-sandbox') :
    """
    list blobs in a bucket, with a prefix.
    all files matching the prefix will be returned
    :param bucket_name: Name of the bucket
    :param prefix: Blobs prefix
    :param project_id: defaulting to och-sandbox
    :return: List containing the blobs
    """

    storage_client = get_storage_client(project_id)
    LOGGER.info(f"Listing blobs from bucket {project_id} {bucket_name} {prefix}")
    ls = list(storage_client.list_blobs(bucket_or_name=bucket_name, prefix=prefix))
    return ls


def upload_blob_from_filename(bucket_name, source_file_name, destination_blob_name, project_id='och-sandbox') :
    """
    upload a local file to a GCS bucket
    :param bucket_name:
    :param source_file_name: local file to upload
    :param destination_blob_name: full path of the blob including the name of the file
    :param project_id defaulting to och-sandbox
    :return:
    """
    storage_client = get_storage_client(project_id=project_id)
    bucket = storage_client.get_bucket(bucket_or_name=bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    LOGGER.info(f"Uploaded file {source_file_name} to bucket gs://{bucket_name}/{destination_blob_name}")


