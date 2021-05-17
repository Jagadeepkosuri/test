from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from operators.neu_dataops_create_dag_operator import NeuDataOpsCreateDagOperator

default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 10, 7),
    'catchup' : 'False',
    'email': ['saurav.mishra@neudesic.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'schedule_interval': '@daily'
} 

dag = NeuDataOpsCreateDagOperator.create_batch_ingestion_dag('demo_onetime_data_ingestion_to_sales_job', default_dag_args)