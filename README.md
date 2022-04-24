#how to install the project ?

#### install google SDK
https://cloud.google.com/sdk/docs/install-sdk

### install env
- create a virtual python environment 
- open the file install.sh with a text editor and update the correct value of the PROJECT_ROOT
- execute install_env.sh 


### Pycharm 
- if using pycharm, add the environment variables to your python console 
AIRFLOW_HOME
GOOGLE_APPLICATION_CREDENTIALS
GOOGLE_CLOUD_PROJECT
PROJECT_ROOT

- if debugging, please add these variables 
(exported in the file $PROJECT_ROOT/env.txt) in the environment variable 
of your python run configuration template

### connect to 
- Airflow: http://localhost:8080/home
- BigQuery: https://console.cloud.google.com/bigquery?hl=fr&project=och-sandbox
- GCS: https://console.cloud.google.com/storage/browser?hl=fr&project=och-sandbox


### code notes


