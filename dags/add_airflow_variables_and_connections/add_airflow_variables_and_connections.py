from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator  
from datetime import datetime
from airflow.models import Variable
from airflow import settings
from airflow.models import Connection

default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 2, 10),
    'catchup': 'False',
    'email': ['saurav.mishra@neudesic.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

def variable_creation():
    Variable.set("AIRFLOW_ENV", "dev")
    Variable.set("AIRFLOW_HOME", "/opt/bitnami/airflow")

def connection_creation():
    dev_demo_mssql_conn = Connection(
        conn_id='dev_demo_mssql_conn',
        conn_type='odbc',
        host='neufwkdataopssqldbs.database.windows.net',
        login='dataopssqladmin',
        password='pass@word1',
        extra='{"Database": "neufwkdataopssqldb","Driver": "ODBC Driver 17 for SQL Server","ApplicationIntent": "ReadOnly","TrustedConnection": "Yes"}'
    )
    session = settings.Session()  # get the session
    session.add(dev_demo_mssql_conn)
    session.commit()


    dev_demo_redshift_conn = Connection(
        conn_id='dev_demo_redshift_conn',
        conn_type='odbc',
        host='redshift-cluster-1.chvw8ltuolv5.us-east-2.redshift.amazonaws.com',
        login='awsuser',
        password='Neudesic1',
        extra='{"Driver":"ODBC Driver 17 for SQL Server","ApplicationIntent":"ReadOnly","TrustedConnection":"Yes"}'
    )
    session = settings.Session()  # get the session
    session.add(dev_demo_redshift_conn)
    session.commit()

    dev_demo_synapse_conn = Connection(
        conn_id='dev_demo_synapse_conn',
        conn_type='odbc',
        host='neufwkdataopssynws.sql.azuresynapse.net',
        login='dataopssqladmin',
        password='pass@word1',
        extra='{"Driver":"ODBC Driver 17 for SQL Server","ApplicationIntent":"ReadOnly","TrustedConnection":"Yes"}'
    )
    session = settings.Session()  # get the session
    session.add(dev_demo_synapse_conn)
    session.commit()

    dev_demo_blob_conn = Connection(
        conn_id='dev_demo_blob_conn',
        conn_type='wasb',
        schema='synapse',
        host='neufwkdataopssa.blob.core.windows.net',
        login='neufwkdataopssa',
        password='UCK0FPEnQz66HZsYPed+7fJ31v7doW0LxaXhYolMFC61ae81suU/cKfKqgeERPIQZcAL/etji1JoJPNA1hyR3w=='
    )
    session = settings.Session()  # get the session
    session.add(dev_demo_blob_conn)
    session.commit()

with DAG('add_airflow_variables_and_connections', schedule_interval=None, default_args=default_dag_args) as dag:
    start = DummyOperator(task_id='start')
    add_variables = PythonOperator(task_id='add.variable', python_callable=variable_creation)
    add_connections = PythonOperator(task_id='add_connections', python_callable=connection_creation)
    end = DummyOperator(task_id='end')
    start >> add_variables >> add_connections >> end