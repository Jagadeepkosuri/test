from typing import Iterable, Mapping, Optional, Union

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.utils.decorators import apply_defaults
import logging
import pandas as pd
from airflow.models import Variable
from airflow.utils.decorators import apply_defaults
import json

class NeuMsSqlScriptOperator(BaseOperator):
    """
    Executes sql code in a specific Microsoft SQL Server database

    :param sql: the sql code to be executed
    :type sql: str or string pointing to a template file with .sql
        extension. (templated)
    :param mssql_conn_id: reference to a specific mssql database
    :type mssql_conn_id: str
    :param parameters: (optional) the parameters to render the SQL query with.
    :type parameters: dict or iterable
    :param autocommit: if True, each command is automatically committed.
        (default value: False)
    :type autocommit: bool
    :param database: name of database which overwrite defined one in connection
    :type database: str
    :type autocommit: str
    :param incremental_type: name of the incremental field  which overwrite defined one in connection
    :type database: str
        :type autocommit: bool
    :param database: name of database which overwrite defined one in connection
    :type database: str
        :type autocommit: bool
    :param database: name of database which overwrite defined one in connection
    :type database: str
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
        self,
        sql: str,
        sqlscript_path: str,
        mssql_conn_id: str = 'mssql_default',
        parameters: Optional[Union[Mapping, Iterable]] = None,
        autocommit: bool = False,
        database: Optional[str] = None,
        incremental_type: str = None,
        incremental_field: str =None,
        watermark_task_id: str = None,
        *args, **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.mssql_conn_id = mssql_conn_id
        self.sqlscript_path = sqlscript_path
        self.sql = sql
        self.parameters = parameters
        self.autocommit = autocommit
        self.database = database
        self.incremental_type = incremental_type
        self.incremental_field = incremental_field
        self.watermark_task_id = watermark_task_id
        self._hook = None


    def __NeuMsSql__(self,data=None):
        """ This method is used to execute the sql scripts in snowflake."""
        with open(self.sqlscript_path, 'r') as f:
            file_data=f.read()
        self.sql = file_data
        self.log.info('Executing: %s', self.sql)
        """Retreiving the mssql connection from the airflow connections using OdbcHook."""
        hook = OdbcHook(odbc_conn_id=self.mssql_conn_id, database=self.database)
        if self.incremental_type == "ActualDate":
            logging.info("The incremental type is based on ActualDate.")
            if data.empty:
                logging.info("The watermark date field is empty doing inital load.")
                logging.info(data)
                WATERMARK_TIMESTAMP="1900-01-01 01:01:01.000"
                self.sql=self.sql.replace('{WATERMARK_TIMESTAMP}',str(WATERMARK_TIMESTAMP))
                logging.info(self.sql)
                logging.info("reached here")
                df= hook.get_pandas_df(sql=self.sql, parameters=self.parameters)
                logging.info(df)
                return df
            else:
                logging.info('INSIDE ELSE')
                LASTUPDATEDTS=data.iloc[0]['LASTUPDATEDTS']
                logging.info(LASTUPDATEDTS)
                if LASTUPDATEDTS == None:
                    LASTUPDATEDTS = '1900-01-01 01:01:01.000'
                # if 'ActualDate' in self.incremental_field.upper():
                #     WATERMARK_TIMESTAMP="to_timestamp_ntz('{}')".format(str(LASTUPDATEDTS))
                # else:
                #     WATERMARK_TIMESTAMP="(CONVERT_TIMEZONE('Etc/UCT','Australia/Sydney',TO_TIMESTAMP_NTZ('{}')))".format(str(LASTUPDATEDTS))
                WATERMARK_TIMESTAMP="CONVERT(datetime,'{}')".format(str(LASTUPDATEDTS))
                logging.info(WATERMARK_TIMESTAMP)
                self.sql=self.sql.replace('{WATERMARK_TIMESTAMP}',WATERMARK_TIMESTAMP)
                logging.info(self.sql)
                df= hook.get_pandas_df(sql=self.sql, parameters=self.parameters)
                return df
        else:
            df= hook.get_pandas_df(sql=self.sql, parameters=self.parameters)
            return df
    def execute(self,context):
        """
        This is the execute method for this class
        :param self: inidcates the instance of the class
        :param context: indicates the current context
        :return: return the transfromed data
        """
        task_instance = context['task_instance']
        if self.incremental_type == "ActualDate":

            data = task_instance.xcom_pull(task_ids=self.watermark_task_id)
            return self.__NeuMsSql__(data)

        else:
            return self.__NeuMsSql__()



