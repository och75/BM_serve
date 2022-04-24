import os
# TODO UPDATE THE FOLLOWING VALUE
os.environ['AIRFLOW_HOME'] ='/Users/o.charles/PycharmProjects/BM_serve/airflow_config/'

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = f'{os.environ["AIRFLOW_HOME"]}/gcp_bm_airflow_sa.json'
os.environ['GOOGLE_CLOUD_PROJECT'] = 'och-sandbox'

from utils import bq_util
from utils import gcs_util
from utils import http_util

BUCKET_NAME = 'staging_product_catalog'
GCP_PROJECT = 'och-sandbox'
DATASET_NAME = 'staging'



file = 'product_catalog'
conf = {
    'url': 'https://backmarket-data-jobs.s3-eu-west-1.amazonaws.com/data/product_catalog.csv',
    'file_name': 'product_catalog.csv',
    'bucket_name': BUCKET_NAME,
    'blob_path': 'product_catalog/'
}
table_name = f'gcs_{file}'

print("test if bucket exists")
if BUCKET_NAME not in gcs_util.list_buckets():
    print("creating bucket")
    gcs_util.create_bucket(BUCKET_NAME)
else:
    print("bucket exists")

print("DL file")
http_util.copy_file_from_url_to_gcs(**conf)

print("dropping external table")
bq_util.drop_a_table(
    **{
        'dataset_name': DATASET_NAME,
        'table_name': table_name
    }
)

print("creating external table")
bq_util.create_ext_table_from_gcs_csv(
    **{
        'table_name': table_name,
        'blob_names': [f'gs://{BUCKET_NAME}/{conf["blob_path"]}/{conf["file_name"]}'],
        'dataset_name': DATASET_NAME
    }
)

print("execute query")
bq_util.query_execute(file_path='sql/product_team_a.sql')
