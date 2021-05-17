from typing import Iterable, Mapping, Optional, Union
from codecs import open

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine
from airflow.utils.decorators import apply_defaults
import logging
import pandas as pd
from airflow.models import Variable
from airflow.utils.decorators import apply_defaults
import json
from datetime import datetime, timedelta
import pandas.io.sql as psql
from contextlib import closing
import urllib
from airflow.contrib.hooks.wasb_hook import WasbHook
import os


class NeuMsSqlCTTargetOperator(BaseOperator):
    """
    This class is used to define the Neudesic change tracking extract and load operator NeuMsSqlCTRedshiftOperator.
    This operator is used to stage the data from  Application DB on MSSQL Server into enterprise DWH on Redshift.
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            sql=None,
            sql_script=None,
            mssql_conn_id='mssql_default',
            parameters=None,
            autocommit=False,
            database=None,
            destination_conn_id=None,
            schema_name=None,
            table_name=None,
            ct_key_column=None,
            destination_system=None,
            destination_database=None,
            destination_schema=None,
            destination_table=None,
            blob_conn_id=None,
            load_type=None,
            source_task_id=None,
            job_type=None,
            source_record_count=None,
            destination_record_count=None,
            logger_conn_id=None,
            *args, **kwargs
    ):
        '''

        :param sql: This contains the source SQL qery passed for extracting the data ,if any, other wise empty  string
        :param sql_script: This contains the source SQL Script path for extracting data ,if any, other wise empty path
        :param mssql_conn_id: This is the source MSQQL DB connection id defined on airflow connections
        :param parameters: DB param for MSSQL- optional
        :param autocommit: Auto commit flag from MSSQL -optional
        :param database: source data base name for MSsql SB
        :param destination_conn_id: The is the target  stagig DB connection ID
        :param schema_name: Source schema name
        :param table_name:  Source table  name
        :param ct_key_column: Source table PK / Change Tracking Key
        :param destination_table: This is the name of destination tbales
        :param load_type: Defines the traget load type (e.g: append)
        :param source_task_id: This is the previous tasks used perfrom xoom pull to get source schema definition from
        schema validator task
        :param job_type: This parameter indicates the type of jobs (e.g : onetime_load,incremental_load)
        :param source_record_count: This param is used to collect the source record count, used for logging
        :param destination_record_count:  This param is used to collect the destination record count, used for logging
        :param logger_conn_id:This param keep the logger tables target connection id , if it empty then logger
        functionality is switched off
        :param args:The special syntax *args in function definitions in python is used to pass a variable number of
        arguments to a function.
        :param kwargs:The special syntax **kwargs in function definitions in python is used to pass a keyworded,
         variable-length argument list.
        '''
        super(NeuMsSqlCTTargetOperator, self).__init__(*args, **kwargs)
        self.mssql_conn_id = mssql_conn_id
        self.sql = sql
        self.sql_script = sql_script
        self.parameters = parameters
        self.autocommit = autocommit
        self.database = database
        self._hook = None
        self.schema_name = schema_name
        self.table_name = table_name
        self.ct_key_column = ct_key_column
        # desintation side
        self.destination_system = destination_system
        self.destination_conn_id = destination_conn_id
        self.destination_database = destination_database
        self.destination_schema = destination_schema
        self.destination_table = destination_table
        self.blob_conn_id = blob_conn_id
        self.load_type = load_type
        self.source_task_id = source_task_id
        self.job_type = job_type
        self.source_record_count = source_record_count
        self.destination_record_count = destination_record_count
        self.logger_conn_id = logger_conn_id
        self.is_logged = False

    def __NeuMsSqlChangeTracking__(self, data=None, source_schema_df=None, task_instance=None, \
                                   job_instance=None):
        hook = OdbcHook(odbc_conn_id=self.mssql_conn_id, database=self.database)
        try:
            if self.sql != "":
                df = hook.get_pandas_df(sql=self.sql, parameters=self.parameters)
                self.source_record_count = len(df.index)
                if self.source_record_count > 0:
                    self.__NeuDataLoad__(data=df, task_instance=task_instance, job_instance=job_instance)
            elif self.sql_script != "":
                self.sql = self.read_sql_script()
                df = hook.get_pandas_df(sql=self.sql, parameters=self.parameters)
                self.source_record_count = len(df.index)
                if self.source_record_count > 0:
                    self.__NeuDataLoad__(data=df, task_instance=task_instance, job_instance=job_instance)
            else:
                ctsql = self.change_tracking_sql_generator(source_schema_df=source_schema_df)
                self.sql = ctsql
                logging.info("the sql fro getting data from source == ", self.sql)
                df = hook.get_pandas_df(sql=ctsql, parameters=self.parameters)
                logging.info("the data getting loaded == ", df)

                self.source_record_count = len(df.index)
                if self.source_record_count > 0:
                    self.__NeuDataLoad__(data=df, task_instance=task_instance, job_instance=job_instance)
        except AirflowException as err:
            self.__NeuLogCollector__(task_instance=task_instance, job_instance=job_instance, status='failed')
            raise AirflowException("Error during source data extraction.")

        return True

    def __NeuDataLoad__(self, data, task_instance=None, job_instance=None):
        """Retreiving the destination connection from the airflow connections."""
        try:

            conn = BaseHook.get_connection(self.destination_conn_id)
            
            if self.destination_system == "postgres":
                connection_string = 'postgresql://{}:{}@{}:{}/{}'.format(conn.login, conn.password, conn.host, conn.port, self.destination_database)
                logging.info("connection_string ==== ", connection_string)

                connection = create_engine(connection_string)

            if data.empty:
                strmsg = 'Source table dataframe is empty.Please check change tracking enabled on source table or not. ' \
                         'Unable to load the data into detination table.'
                raise AirflowException(strmsg)
            else:
                if self.destination_system == "synapse":
                    blob_conn = BaseHook.get_connection(self.blob_conn_id)
                    self.__NeuSynapseLoad__(data, blob_conn)
                elif self.destination_system == "postgres":
                    if self.load_type == 'append':
                        self.destination_record_count = len(data.index)
                        data.to_sql(self.destination_table, con=connection, index=False, if_exists='append',
                                schema=self.destination_schema,
                                index_label=None, chunksize=16000)
                    else:
                        self.destination_record_count = len(data.index)
                        data.to_sql(self.destination_table, con=connection, index=False, if_exists='replace',
                                    schema=self.destination_schema,
                                    index_label=None, chunksize=16000)
        except AirflowException as err:
            raise AirflowException("Error during data loading to destination.")
        return True

    def __NeuSynapseLoad__(self, data, blob_conn):
        if self.load_type == "replace":
            self.__NeuSynapseExecute__('DELETE FROM '+ self.destination_schema + '.' + self.destination_table)
            
        data = self.__NeuBlobUpload__(data, blob_conn)
        logging.info("data to be loaded in Temp table == ", data)

        create_statement = self.__NeuGenerateDDL__(data = data, table = self.destination_schema + ".TMP_" + self.destination_table, ddl_type = 'table')
        logging.info("create statement using the dataframe == ", create_statement)
        self.__NeuSynapseExecute__(create_statement)

        copy_statement = self.__CreateSynapseCopyQuery__(blob_conn, table_type = "temp")
        logging.info("copy statement using the dataframe == ", copy_statement)
        self.__NeuSynapseExecute__(copy_statement)
        
        copy_statement = self.__CreateSynapseCopyQuery__(blob_conn, table_type = "main")
        logging.info("copy statement using the dataframe == ", copy_statement)
        self.__NeuSynapseExecute__(copy_statement)
        
        #Deleting Temporary Table Generated in Destination
        self.__NeuSynapseExecute__('DROP TABLE '+ self.destination_schema + '.TMP_' + self.destination_table)

    def __NeuBlobUpload__(self, data, blob_conn):
        """
        To upload file in azure data lake storage
        :param data: dataframe to be uploaded
        :return: None
        """
        wasb_hook = WasbHook(wasb_conn_id=self.blob_conn_id)
        path = Variable.get("AIRFLOW_HOME") + "/parquet_files/"
        if not os.path.exists(path):
            os.mkdir(path)
        local_filename = path + self.destination_table + ".parquet"
        for i in data.columns:
            if (data[i].dtype == 'bool') or (data[i].dtype == 'datetime64[ns]'):
                data[i] = data[i].astype('str')
        data_tmp = data.astype(str)
        data_tmp.to_parquet(local_filename)
        wasb_hook.load_file(
            file_path=local_filename,
            container_name=blob_conn.schema,
            blob_name = self.destination_schema.lower().replace("_", "-") + "/" + self.destination_table + ".parquet")
        if os.path.exists(local_filename):
            os.remove(local_filename)
        return data_tmp

    def __CreateSynapseCopyQuery__(self, blob_conn, table_type):

        """
        To create dynamic copy command for loading into tstaging synapse
        :param filename: uploaded filename
        :param table_name: synapse table name
        :return: None
        """
        copy_query = "COPY INTO {} FROM '{}' " "WITH (FILE_TYPE = 'parquet', CREDENTIAL = (IDENTITY='Managed Identity'));"
        insert_query = "INSERT INTO {} SELECT * FROM {}"
        logging.info("Generated Copy Query == ", copy_query)
        host = blob_conn.host
        if 'https://' not in host:
            host = 'https://' + host
        if table_type == "temp":
            return copy_query.format(self.destination_schema + ".TMP_" + self.destination_table, 
            host + '/' + blob_conn.schema + '/' + self.destination_schema.lower().replace("_", "-") + "/" + self.destination_table + ".parquet")
        if table_type == "main":
            return insert_query.format(self.destination_schema + "." + self.destination_table, self.destination_schema + ".TMP_" + self.destination_table)

    
    def __NeuGenerateDDL__(self, data, ddl_type, table=None, schema=None):
        """
        To generate DDL statement using dataframe
        :param data: dataframe to be uploaded
        :param table: table name to be used in create statement
        :return: statement
        """
        create_statement = pd.io.sql.get_schema(data, table)
        create_statement = (
            create_statement.replace(" TEXT", " VARCHAR(8000)")
            .replace('"', '')
        )
        create_statement = create_statement[:-1] + ", etl_status varchar(50), etl_datetime datetime, hard_delete varchar(50)" + create_statement[-1:]
        create_statement = "IF OBJECT_ID('{}') IS NULL " + create_statement
        return create_statement
    
    def __NeuSynapseExecute__(self, sql_query):
        """
        To execute SQL statements in synapse
        :param create_statement: statement to be executed
        :return: None
        """
        hook = OdbcHook(odbc_conn_id=self.destination_conn_id, database=self.destination_database)
        hook.run(sql_query, autocommit=True)
        return True

    def execute(self, context):
        """
        This is the execute method for this class
        :param self: inidcates the instance of the class
        :param context: indicates the current context
        :return: return the transfromed data
        """
        task_instance = context['task_instance']
        job_instance = context['dag_run']
        key = self.schema_name + "_" + self.table_name + "_schema_df"
        source_schema_df = task_instance.xcom_pull(task_ids=self.source_task_id, key=key)
        result = self.__NeuMsSqlChangeTracking__(source_schema_df=source_schema_df, task_instance=task_instance,
                                                 job_instance=job_instance)
        return True

    def __check_change_tracking_enabled__(self):
        strctchecksql = "select * from sys.change_tracking_tables a  inner join sys.tables b on a.object_id=b.object_id inner join sys.schemas c on b.schema_id=c.schema_id where c.name='{}' and  b.name ='{}'".format(
            self.schema_name, self.table_name)
        logging.info(strctchecksql)
        mssqlhook = OdbcHook(odbc_conn_id=self.mssql_conn_id, database=self.database)
        ct_check_df = mssqlhook.get_pandas_df(sql=strctchecksql, parameters=None)
        if ct_check_df.empty == True:
            return False
        else:
            return True

    def change_tracking_sql_generator(self, source_schema_df):
        if self.__check_change_tracking_enabled__():
            if self.job_type == 'incremental_load':
                ctsql = "declare @last_synchronization_version bigint;SET @last_synchronization_version = CHANGE_TRACKING_CURRENT_VERSION()-1; " \
                        "SELECT {} c.SYS_CHANGE_VERSION,c.SYS_CHANGE_OPERATION,c.SYS_CHANGE_CONTEXT,{}  AS SYS_CHANGE_COLUMNS " \
                        "FROM {}.{} a RIGHT OUTER " \
                        "JOIN CHANGETABLE(CHANGES {}.{}, @last_synchronization_version) AS c ON a.{} = c.{};"

                strcolumn = "coalesce(a.{0},c.{0}) as {0},".format(self.ct_key_column)
                strctcolumn = "CASE WHEN CHANGE_TRACKING_IS_COLUMN_IN_MASK(COLUMNPROPERTY(OBJECT_ID('{}.{}'), " \
                              "'{}', 'ColumnId'),c.sys_change_columns)=1 THEN '{}' END ,"
                for i, row in source_schema_df.iterrows():
                    if row.column_name != self.ct_key_column:
                        if row.column_name == 'SCHEMA':
                            strcolumn = strcolumn + "a.[" + row.column_name + "], "
                        else:
                            strcolumn = strcolumn + "a." + row.column_name + ", "
                    if i == 0:
                        strchangecolumn = "REVERSE(STUFF(REVERSE(CONCAT(" + strctcolumn.format(
                            self.schema_name, self.table_name, row.column_name, row.column_name + ",")
                    else:
                        strchangecolumn = strchangecolumn + strctcolumn.format(
                            self.schema_name, self.table_name, row.column_name, row.column_name + ",")
                strchangecolumn = strchangecolumn[
                                  :-1] + ")), 1, 1, ''))"  # to avoid last comma in the concatenated list

                ctsql = ctsql.format(strcolumn, strchangecolumn, self.schema_name, self.table_name, self.schema_name,
                                     self.table_name, self.ct_key_column, self.ct_key_column)
            elif self.job_type == 'onetime_load':
                ctsql = "declare @last_synchronization_version bigint;SET @last_synchronization_version = CHANGE_TRACKING_CURRENT_VERSION()-1; " \
                        "SELECT {} c.SYS_CHANGE_VERSION,c.SYS_CHANGE_OPERATION,c.SYS_CHANGE_CONTEXT, CASE WHEN c.sys_change_columns != NULL THEN " \
                        "{} ELSE c.sys_change_columns END AS SYS_CHANGE_COLUMNS " \
                        "FROM {}.{} a LEFT OUTER " \
                        "JOIN CHANGETABLE(CHANGES {}.{}, @last_synchronization_version) AS c ON a.{} = c.{};"

                strcolumn = ""
                strctcolumn = "CASE WHEN CHANGE_TRACKING_IS_COLUMN_IN_MASK(COLUMNPROPERTY(OBJECT_ID('{}.{}'), " \
                              "'{}', 'ColumnId'),c.sys_change_columns)=1 THEN '{}' END ,"

                no_of_columns = len(source_schema_df.index)
                for i, row in source_schema_df.iterrows():
                    if row.column_name == 'SCHEMA':
                        strcolumn = strcolumn + "a.[" + row.column_name + "], "
                    else:
                        strcolumn = strcolumn + "a." + row.column_name + ", "
                    if no_of_columns <= 1:
                        strchangecolumn = strctcolumn.format(
                            self.schema_name, self.table_name, row.column_name, row.column_name + ",")
                    else:
                        if i == 0:
                            strchangecolumn = "CONCAT(" + strctcolumn.format(
                                self.schema_name, self.table_name, row.column_name, row.column_name + ",")

                        else:
                            strchangecolumn = strchangecolumn + strctcolumn.format(
                                self.schema_name, self.table_name, row.column_name, row.column_name)
                if no_of_columns <= 1:
                    strchangecolumn = strchangecolumn[:-1] + " "
                else:
                    strchangecolumn = strchangecolumn[:-1] + ")"
                ctsql = ctsql.format(strcolumn, strchangecolumn, self.schema_name, self.table_name, self.schema_name,
                                     self.table_name, self.ct_key_column, self.ct_key_column)
            return ctsql
        else:
            if self.job_type == 'onetime_load':
                ctsql = "SELECT {} NULL AS SYS_CHANGE_VERSION, NULL AS SYS_CHANGE_OPERATION,NULL AS SYS_CHANGE_CONTEXT," \
                        "NULL AS SYS_CHANGE_COLUMNS " \
                        "FROM {}.[{}] a ;"

                strcolumn = ""
                for i, row in source_schema_df.iterrows():
                    if row.column_name == 'SCHEMA':
                        strcolumn = strcolumn + "a.[" + row.column_name + "], "
                    else:
                        strcolumn = strcolumn + "a." + row.column_name + ", "

                ctsql = ctsql.format(strcolumn, self.schema_name, self.table_name)
                return ctsql

            else:
                raise AirflowException("Change tracking is not enabled on table:" + str(self.schema_name) + "." + str(
                    self.table_name) + ".")

    def read_sql_script(self):
        with open(self.sql_script, 'r') as f:
            file_data = f.read()
        sql = file_data
        logging.info(sql)
        return sql
