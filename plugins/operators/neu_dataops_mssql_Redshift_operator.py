from typing import Iterable, Mapping, Optional, Union

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, Variable
from airflow.providers.odbc.hooks.odbc import OdbcHook
import logging
import pandas as pd
from airflow.utils.decorators import apply_defaults
import json


class NeuDataOpsMsSqlRedshiftOperator(BaseOperator):
    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            sql: str,
            key_column: str=None,
            mssql_conn_id: str = 'mssql_default',
            parameters: Optional[Union[Mapping, Iterable]] = None,
            autocommit: bool = False,
            mssql_database: Optional[str] = None,
            mssql_schema_name:Optional[str] = None,
            mssql_tablename:Optional[str] = None,
            Redshift_conn_id: str = None,
            Redshift_database_name: str = None,
            Redshift_schema_name: str = None,
            Redshift_tablename: str = None,
            job_type: str=None,
            incremental_field: str=None,
            delta_job_name: str =None,
            *args, **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.mssql_conn_id = mssql_conn_id
        self.sql = sql
        self.key_column = key_column
        self.parameters = parameters
        self.autocommit = autocommit
        self.mssql_database = mssql_database
        self.mssql_schema_name =mssql_schema_name
        self.mssql_tablename=mssql_tablename
        self.Redshift_conn_id=Redshift_conn_id
        self.Redshift_database_name=Redshift_database_name
        self.Redshift_schema_name=Redshift_schema_name
        self.Redshift_tablename=Redshift_tablename
        self.job_type=job_type
        self.incremental_field=incremental_field
        self.delta_job_name=delta_job_name
        self._hook = None

    def __NeuDataOpsMsSql__(self):
        self.destination_hook = OdbcHook(odbc_conn_id=self.Redshift_conn_id, database = 'dev')
        self.source_hook = OdbcHook(odbc_conn_id=self.mssql_conn_id, database=self.mssql_database)
        self.__NeuDataOpsRedshiftDB__()

        '''
        dest_hook = OdbcHook(odbc_conn_id=self.Redshift_conn_id)
        self.sql = self.__NeuDataOpsRedshiftDB__(dest_hook)
        dest_hook.run(sql=self.sql,autocommit=True, parameters=self.parameters)
        return True
        '''

    def __NeuDataOpsRedshiftDB__(self):

        #Query to create database if does not exist
        # self.create_database_if_not_exists()

        # if we have multiple tables in same schema running tasks in parallel will cause trouble
        #Query to create schema if does not exist
        # self.create_schema_if_not_exists()
        
        #Query to create dataframe from the source table for the coulumn names and datatypes
        self.schema_df = self.get_schema_extraction_sql()

        #Query to create schema if does not exist
        self.create_table_if_not_exists()
        
        #Query to insert values in the destination table
        self.insert_data_into_destination_db()

        return True


    def create_database_if_not_exists(self):
        """
        Checks if a database exists in Redshift and creat the database if it doesn't exist already.
        """
        sql = "CREATE DATABASE " + self.Redshift_database_name + ";"
        try:
            self.destination_hook.run(sql=sql,autocommit=True, parameters=self.parameters)
            print("Database {} Created".format(self.Redshift_database_name))
        except Exception as e:
            print("Database Already exists: ", e)
        
        #Using the Database by updating the hook parameter
        self.destination_hook = OdbcHook(odbc_conn_id=self.Redshift_conn_id, database=self.Redshift_database_name)
        return True    


    def create_schema_if_not_exists(self):
        sql = "create schema if not exists "+ self.Redshift_schema_name +";"
        self.destination_hook.run(sql=sql,autocommit=True, parameters=self.parameters)
        return True


    def get_schema_extraction_sql(self):
        #sql = "select column_name, data_type, character_maximum_length " + \
        #      "from information_schema.columns where table_name='"+ self.mssql_tablename + \
        #      "' and table_schema='"+ self.mssql_schema_name +"'"
        

        sql = "SELECT UPPER(c.NAME) AS COLUMN_NAME , " + \
            "CASE WHEN UPPER(Y.NAME) IN('TINYINT','INT','BIGINT','DECIMAL','NUMERIC','MONEY','SMALLMONEY','FLOAT','REAL') THEN 'NUMERIC' " + \
            "WHEN UPPER(Y.NAME) IN('CHAR','VARCHAR','NCHAR','NVARCHAR','NTEXT' ) THEN 'TEXT' " + \
            "WHEN UPPER(Y.NAME) IN('DATE','DATETIME','DATETIME2','SMALLDATETIME','TIME') THEN 'TIMESTAMP' " + \
            "WHEN UPPER(Y.NAME) IN('BIT') THEN 'BOOLEAN' " + \
            "WHEN UPPER(Y.NAME) IN ('UNIQUEIDENTIFIER') THEN 'TEXT' ELSE UPPER(Y.NAME) END AS DATA_TYPE ," + \
            "CASE WHEN UPPER(Y.NAME) IN ('UNIQUEIDENTIFIER') THEN 36 ELSE c.MAX_LENGTH END AS MAX_LENGTH," + \
            "c.PRECISION,c.SCALE,CASE WHEN c.IS_NULLABLE=1 THEN 0 WHEN c.IS_NULLABLE=0 THEN 1 END AS IS_NULLABLE " + \
            "FROM sys.SCHEMAS s INNER JOIN sys.TABLES t ON s.SCHEMA_ID=t.SCHEMA_ID " + \
            "INNER JOIN sys.COLUMNS c ON t.OBJECT_ID=c.OBJECT_ID " + \
            "INNER JOIN sys.TYPES Y ON c.SYSTEM_TYPE_ID=Y.SYSTEM_TYPE_ID " + \
            "WHERE s.NAME ='{}' AND t.NAME ='{}' AND upper(Y.name) not in ('SYSNAME') ORDER BY c.column_id ".format( self.mssql_schema_name,self.mssql_tablename )

        logging.info("schema extraction from mssql=====", sql)


        schema_df = self.source_hook.get_pandas_df(sql=sql).fillna('NaN').to_dict(orient="records")
        return schema_df


    def create_table_if_not_exists(self):
        sql = "Create table if not exists " + self.Redshift_schema_name + "." + self.Redshift_tablename + "("

        for i in range(len(self.schema_df)):
            sql = sql + str(self.schema_df[i]['COLUMN_NAME']) + " " + str(self.schema_df[i]['DATA_TYPE'])
            if self.schema_df[i]['DATA_TYPE'] == 'NUMERIC':
                sql = sql + "(" + str(self.schema_df[i]['PRECISION']) + "," + str(self.schema_df[i]['SCALE'])    + ")"
            # if self.schema_df[i]['COLUMN_NAME'] == (self.key_column).upper():
            #     sql = sql + " PRIMARY KEY"
                
            sql = sql + ","
        # sql = sql[:-1] + "); "
        sql = sql + "ETL_STATUS TEXT DEFAULT 'N') "

        self.destination_hook.run(sql=sql, autocommit=True, parameters=self.parameters)
        return True


    def insert_data_into_destination_db(self):
        values_df = self.get_sql_from_source_table()
        
        for i in range(len(values_df)):
            sql = "insert into " + self.Redshift_schema_name + "." + self.Redshift_tablename + " values("
            row = ""
            for v in values_df[i].values():
                row = row + "'" + str(v) + "',"
            row = row[:-1] + "); "
            sql = sql + row
            self.destination_hook.run(sql=sql, autocommit=True, parameters=self.parameters)
        return True

    def __NeudesicRedshiftLoad__(self,data,task_instance=None, job_instance=None):
        """Retreiving the snowflake connection from the airflow connections using SnowflakeHook."""
        try:

            hook= OdbcHook(odbc_conn_id=self.Redshift_conn_id, database=self.Redshift_database_name)
            conn_config = hook._get_conn_params()
            engine=hook.get_sqlalchemy_engine(engine_kwargs={"connect_args":conn_config})
            connection = engine.connect()
            if data.empty:
                strmsg='Source table dataframe is empty.Please check change tracking enabled on source table or not. ' \
                       'Unable to load the data into detination table.'
                self.__NeuLogCollector__(task_instance=task_instance,job_instance=job_instance,status='failed')
                raise  AirflowException(strmsg)
            else:
                if self.load_type == 'append':
                    self.destination_record_count=len(data.index)
                    data.to_sql(self.destination_table,con=connection,index=False,if_exists='append',index_label=None,chunksize=16000)
                else:
                    self.destination_record_count=len(data.index)
                    data.to_sql(self.destination_table,con=connection,index=False,if_exists='replace',index_label=None,chunksize=16000)

            connection.close()
            engine.dispose()
        except AirflowException as err:
            self.__NeuLogCollector__(task_instance=task_instance,job_instance=job_instance,status='failed')
            raise  AirflowException("Error during data loading to destination." )
        return True


    def get_sql_from_source_table(self):
        #Creating data frame to store source table's schema
        sql = "select * from {}.{}".format(self.mssql_schema_name,self.mssql_tablename)

        if self.job_type=="incremental_load":
            latest_watermark_ts=self.get_latest_watermark_ts()
            where_sql=" WHERE {} > CONVERT(DATETIME,'{}',101) ".format(self.incremental_field,latest_watermark_ts)
            sql = sql + where_sql
        print(sql)
        values_df = self.source_hook.get_pandas_df(sql= sql).fillna(0).to_dict(orient="records")
        return values_df


    def get_latest_watermark_ts(self):
        source_task_name="read_"+self.mssql_tablename
        sql="SELECT lastupdatedts FROM common.watermark WHERE jobname ='{}' AND taskname='{}' AND datasetobjectname='{" \
            "}'".format(self.delta_job_name,source_task_name,self.mssql_tablename)
        data=self.source_hook.get_pandas_df(sql=sql, parameters=self.parameters)
        if data.empty == False:
            for i,row in data.iterrows():
                latest_watermark_ts=row.lastupdatedts
            latest_watermark_ts= str(latest_watermark_ts)
            return latest_watermark_ts[:-3]
        else:
            return ''


    def execute(self,context):
        """
        This is the execute method for this class
        :param self: inidcates the instance of the class
        :param context: indicates the current context
        :return: return the transfromed data
        """
        return self.__NeuDataOpsMsSql__()
