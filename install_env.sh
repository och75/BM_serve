export PROJECT_ROOT=/Users/o.charles/PycharmProjects/BM_serve

cd $PROJECT_ROOT

sed -e "s+PROJECT_ROOT+${PROJECT_ROOT}+g" < airflow.template.cfg > airflow.cfg

gcloud config set project och-sandbox
gcloud auth activate-service-account --key-file=airflow_config/gcp_bm_airflow_sa.json

# AIRFLOW_VERSION=2.2.5
# PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
# CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
# pip install "apache-airflow[postgres,google]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

pip install -r requirements.txt

export AIRFLOW_HOME=$PROJECT_ROOT/airflow_config/
export GOOGLE_APPLICATION_CREDENTIALS=$AIRFLOW_HOME/gcp_bm_airflow_sa.json
export GOOGLE_CLOUD_PROJECT=och-sandbox
export PYTHONPATH=$PYTHONPATH:$PROJECT_ROOT

env > $PROJECT_ROOT/env.txt

airflow db init
airflow webserver