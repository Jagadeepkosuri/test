
from builtins import str
from typing import Iterable
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.utils.decorators import apply_defaults
import pandas as pd
import logging
#from airflow.contrib.hooks.snowflake_hook import SnowflakeHook


class NeuDataOpsMsSqlSensor(BaseSensorOperator):


    template_fields = ('sql',)
    template_ext = ('.hql', '.sql',)
    ui_color = '#7c7287'


    @apply_defaults
    def __init__(self, conn_id, snowflake_conn_id, database, table, sql, parameters=None, success=None, failure=None,
                 fail_on_empty=False, allow_null=True, *args, **kwargs):
        self.conn_id = conn_id
        self.snowflake_conn_id=snowflake_conn_id
        self.database=database
        self.table=table
        self.sql = sql
        self.parameters = parameters
        self.success = success
        self.failure = failure
        self.fail_on_empty = fail_on_empty
        self.allow_null = allow_null
        super(NeuDataOpsMsSqlSensor, self).__init__(*args, **kwargs)


    def poke(self, context):
        hook = OdbcHook(odbc_conn_id=self.conn_id, database=self.database)
        self.sql=self.sql.replace('{LATEST_VERSION}',str(self.get_latest_version()))
        self.log.info("sensor query:" + self.sql)
        df= hook.get_pandas_df(sql=self.sql, parameters=self.parameters)
        first_data = df.iloc[0,0]

        logging.info(df)
        if first_data =="":
            if self.fail_on_empty:
                raise AirflowException("No rows returned, raising as per fail_on_empty flag")
            else:
                return False

        logging.info(first_data)
        if first_data> 0 :
            first_cell=True
        else:
            first_cell=False
        logging.info(first_cell)

        if self.failure is not None:
            if callable(self.failure):
                if self.failure(first_cell):
                    raise AirflowException(
                        "Failure criteria met. self.failure({}) returned True".format(first_cell))
            else:
                raise AirflowException("self.failure is present, but not callable -> {}".format(self.success))
        if self.success is not None:
            if callable(self.success):
                return self.success(first_cell)
            else:
                raise AirflowException("self.success is present, but not callable -> {}".format(self.success))
        if self.allow_null:
            return first_cell is None or bool(first_cell)
        return bool(first_cell)
    def get_latest_version(self):
        sql="SELECT MAX(SYS_CHANGE_VERSION	) AS CHANGE_VERSION	 FROM "+ self.table.upper()
        hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)
        conn_config = hook._get_conn_params()
        engine=hook.get_sqlalchemy_engine(engine_kwargs={"connect_args":conn_config})
        connection = engine.connect()
        data=pd.read_sql_query(sql,engine)
        connection.close()
        engine.dispose()
        if data.change_version[0]==None:
            return 0
        elif data.empty == False:
            return int(data.change_version[0])+1
        else:
            return 0
