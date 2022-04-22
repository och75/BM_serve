#how to install the project ?
install googleSDK
https://cloud.google.com/sdk/docs/install-sdk

cd /Users/o.charles/PycharmProjects/BM_serve

gcloud config set project och-sandbox
gcloud auth activate-service-account --key-file=airflow_config/gcp_bm_airflow_sa.json



AIRFLOW_VERSION=2.2.5
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow[postgres,google]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

pip install -r requirements.txt

export PROJECT_ROOT=/Users/o.charles/PycharmProjects/BM_serve
export AIRFLOW_HOME=$PROJECT_ROOT/airflow_config/
export GOOGLE_APPLICATION_CREDENTIALS=$AIRFLOW_HOME/gcp_bm_airflow_sa.json
export GOOGLE_CLOUD_PROJECT=och-sandbox
export PYTHONPATH=$PYTHONPATH:$PROJECT_ROOT

if using pycharm add it to env interpreter and python console

# TODO replace airflow.cfg hard path with something smoother
airflow webserver

http://localhost:8080/home

speak about airflow SA with too broad permissions
but for demo only
in real prod we would not create bucket with airflow

why py code instead dedicated AF operators ?
easier to debug appart AF 