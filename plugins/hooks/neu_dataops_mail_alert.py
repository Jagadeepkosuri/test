from airflow import DAG
# from airflow.operators.email_operator import EmailOperator
# from airflow.operators.python_operator import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

class notify_email():

    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "start_date": datetime(2021,3, 22)
    }

    dag = DAG("email_test", default_args=default_args, schedule_interval=timedelta(days=1))

t1 = EmailOperator(
    task_id="send_mail",
    to='lalitesh.depally@neudesic.com',
    subject='Test mail',
    html_content='<p> You have got mail! <p>',
    dag=dag)


def error_function():
    raise Exception('error')


t2 = PythonOperator(
    task_id='failing_task',
    python_callable=error_function,
    email_on_failure=True,
    emai='lalitesh.depally@neudesic.com',
    dag=dag,
)
