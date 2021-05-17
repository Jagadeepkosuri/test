from typing import Iterable, Mapping, Optional, Union
from airflow.plugins_manager import AirflowPlugin
from airflow.operators import BaseOperator
import logging
from codecs import open
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
from airflow.providers.odbc.hooks.odbc import OdbcHook


class NeuDataOpsRedshiftScriptOperator(BaseOperator):
    """
    This class is used to execute the sql script/scripts in Redshift.
    """

    def __init__(self, conn_id, sqlscript_path, *args, **kwargs):
        """
        This is a constructor method.

        :param conn_id: airflow connection for the snowflake.
        :param sqlscript_path: name of sql scripts to be executed in snowflake.    
        """

        super(NeuDataOpsRedshiftScriptOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = conn_id
        self.sqlscript_path = sqlscript_path
        self.parameters = None

    def __RedshiftScript__(self):
        """ This method is used to execute the sql scripts in Redshift."""
        """Retreiving the Redshift connection from the airflow connections using OdbcHook."""
        hook = OdbcHook(odbc_conn_id=self.redshift_conn_id, database='dev')

        with open(self.sqlscript_path, 'r') as f:
            sql_data = f.read()

        hook.run(sql=sql_data, autocommit=True, parameters=self.parameters)

    def execute(self, context):
        """
        This is the execute method for this class
        :param self: inidcates the instance of the class
        :param context: indicates the current context 
        :return: return the transfromed data
        """
        self.__RedshiftScript__()
