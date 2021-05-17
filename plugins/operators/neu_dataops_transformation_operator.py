from typing import Iterable, Mapping, Optional, Union
from codecs import open
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.operators import PythonOperator

from airflow.utils.decorators import apply_defaults
import logging
import pandas as pd
from airflow.models import Variable
import json
from datetime import datetime,timedelta
from airflow.contrib.hooks.redshift_hook import RedshiftHook
#from operators import NeuRedshiftSCDOperator
from airflow.utils.db import provide_session
from airflow.models import XCom
from airflow.hooks.postgres_hook import PostgresHook

class NeuDataOpsTransformationOperator(BaseOperator):
    def __init__(
            self,
            source_task_id_list=None,
            trans_function=None,
            destination=None,
            *args, **kwargs
    ):
        super(NeuDataOpsTransformationOperator, self).__init__(*args, **kwargs)
        self.source_task_id_list=source_task_id_list
        self.trans_function=trans_function
        self.destination=destination


    def __NeuRedshiftLoad__(self,data,Redshift_conn_id,table_name):
        """ This method is used to execute the sql scripts in Redshift."""
        """Retreiving the Redshift connection from the airflow connections using RedshiftHook."""
        hook = RedshiftHook(Redshift_conn_id=Redshift_conn_id)
        conn_config = hook._get_conn_params()
        engine=hook.get_sqlalchemy_engine(engine_kwargs={"connect_args":conn_config})
        connection = engine.connect()
        if data.empty:
            logging.info('Dataframe is empty and no temp table to create')
        else:
            data.to_sql(table_name,con=connection,index=False,if_exists='append',index_label=None,chunksize=16000)
        logging.info("Loaded temp table:"+table_name)
        connection.close()
        engine.dispose()
        return True


    def __NeuRedshiftExecuteSql__(self,sql, Redshift_conn_id):
        hook = RedshiftHook(Redshift_conn_id=Redshift_conn_id)
        conn_config = hook._get_conn_params()
        engine=hook.get_sqlalchemy_engine(engine_kwargs={"connect_args":conn_config})
        connection = engine.connect()
        df= hook.get_pandas_df(sql)
        connection.close()
        engine.dispose()
        return df


    def __executePythonTestScript__(self,task_instance=None,job_instance=None):

        trans_fn="self."+self.test_function
        try:
            if self.test_function=='TestBuyerTransformation':
                trans=self.TestBuyerTransformation(task_instance,job_instance)
                return True
        except AirflowException as err:
                raise AirflowException(err)

    def __executePythonTransformScript__(self,task_instance=None,job_instance=None):
        trans_fn="self."+self.trans_function
        try:
            if self.trans_function=='CustomTransformationFunction':
                trans=self.CustomTransformationFunction(task_instance,job_instance)
                return True
        except AirflowException as err:
                raise AirflowException(err)

    def execute(self,context):
        task_instance = context['task_instance']
        job_instance=context['dag_run']
        return self.__executePythonTransformScript__(task_instance,job_instance)



    def CustomTransformationFunction(self,task_instance,job_instance):
        """
        This is custom function for test
        :param context: current context
        :return: dataframe
        """
        # reading data from xcom and putting in Redshift
        for task in range(0, len(self.source_task_id_list), 2):
            source_df_1=task_instance.xcom_pull(task_ids=self.source_task_id_list[task])
            source_df_2=task_instance.xcom_pull(task_ids=self.source_task_id_list[task+1])
            if (source_df_1 is None or source_df_2 is None):
                pass
            else:
                source_df_1.columns = ['count']
                source_df_2.columns = ['count']
                src = PostgresHook(postgres_conn_id='postgres_default')
                src_conn = src.get_conn()
                cursor = src_conn.cursor()
                cursor.execute("delete FROM xcom where task_id='"+str(self.source_task_id_list[task])+"'")
                cursor.execute("delete FROM xcom where task_id='"+str(self.source_task_id_list[task+1])+"'")
                src_conn.commit()
                result_df = pd.DataFrame(result_dict)
                self.__NeuRedshiftLoad__(result_df, 'dev_core_common_Redshift', 'validation_result' )
                break
                
        return True