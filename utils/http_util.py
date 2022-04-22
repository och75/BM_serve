from logging import getLogger
import requests, os
from utils.gcs_util import upload_blob_from_filename

LOGGER = getLogger('airflow.task')

TEMP_DIR = os.environ['AIRFLOW_HOME']


def write_response_in_file(response_content, local_file_path, append=False, add_new_lines=True):
    """
    write response content into a file
    :param response_content: response.content
    :param local_file_path: local filepath  where file will be written
    :param append: default False, if True, file is overwritten
    :param add_new_lines : adda cariage return \n at the end of response.content if True, default is True
    :return: None
    """

    mode='a' if append else 'w'
    str_response_content = response_content.decode('utf-8') if not isinstance(response_content, str) else response_content
    str_response_content = str_response_content if not add_new_lines else str_response_content+'\n'

    with open(local_file_path, mode) as file:
        file.write(str_response_content)


def copy_file_from_url_to_gcs(url, file_name, bucket_name, blob_path):
    """
    copy a file from a URL and write the content into a GCS bucket,
    by locally writting in a temp. directory the content
    :param url:
    :param file_name:
    :param bucket_name:
    :param blob_path:
    :return: None
    """
    local_file_path = f'{TEMP_DIR}/{file_name}'
    blob_full_path = f'{blob_path}{file_name}'
    response = requests.get(url)
    write_response_in_file(response.content, local_file_path)
    upload_blob_from_filename(bucket_name, local_file_path, blob_full_path)
    os.remove(local_file_path)


