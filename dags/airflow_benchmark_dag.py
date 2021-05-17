# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


from datetime import timedelta
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
import pandas as pd
from airflow.operators.python_operator import PythonOperator
from airflow import settings
from airflow.models import Connection
from airflow.operators.dummy_operator import DummyOperator
import pandas as pd
import numpy as np

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['jai.janyani@neudesic.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'airflow_benchmark_dag',
    default_args=default_args,
    schedule_interval=None,
)


start_task = DummyOperator(task_id='start', dag=dag)
end_task = DummyOperator(task_id='end', dag=dag)


def function_to_execute(**kwargs):
    # df = pd.read_csv('https://storage.googleapis.com/sharing_season_bucket/abc.csv')
    df = pd.DataFrame(np.random.randint(0,100,size=(100, 4)), columns=list('ABCD'))
    df_final = pd.DataFrame()
    for i in range(0, 2):
        df_final = df_final.append(df)
        print("dataframe lenght: ", df_final.shape)
        print("dataframe info: ", df_final.info())
		time.sleep(3600)

t2 = PythonOperator(
    task_id='function_to_execute',
    provide_context=False,
    python_callable=function_to_execute,
    dag=dag)

def function_to_execute_2(**kwargs):
    # df = pd.read_csv('https://storage.googleapis.com/sharing_season_bucket/abc.csv')
    df = pd.DataFrame(np.random.randint(0,100,size=(100, 4)), columns=list('ABCD'))
    df_final = pd.DataFrame()
    for i in range(0, 2):
        df_final = df_final.append(df)
        print("dataframe lenght: ", df_final.shape)
        print("dataframe info: ", df_final.info())

t3 = PythonOperator(
    task_id='function_to_execute_2',
    provide_context=False,
    python_callable=function_to_execute_2,
    dag=dag)


start_task >> t2
start_task >> t3
t2 >> end_task
t3 >> end_task