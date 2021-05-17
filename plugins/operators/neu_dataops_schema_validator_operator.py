from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
import logging
import pandas as pd
import numpy as np
from typing import Iterable, Mapping, Optional, Union

from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.base_hook import BaseHook

from airflow import AirflowException
from datetime import datetime,timedelta
from sqlalchemy import create_engine


class NeuSchemaValidatorOperator(BaseOperator):
    """
    This is a custom build schema validator and schema adjustment operator which does comparison between source and
    target schema and if required , based on source schema definition ,it automatically create DDL( create and Alter)
    scripts and get executed them on target environment .In case , if  the schema changes are not supported by the
    target environment , then the operator needs to raise an airflow exception as well.

    """
    def __init__(
            self,
            source_conn_id = None,
            source_system = None,
            source_database = None,
            source_schema_name = None,
            source_table_name  = None,
            target_conn_id  = None,
            target_system = None,
            target_schema_name = None,
            target_table_name  = None,
            job_type=  None,
            logger_conn_id = None,
            source_schema_sql = None,
            target_schema_sql = None,
            target_database=None,
            parameters: Optional[Union[Mapping, Iterable]] = None,
            *args, **kwargs ):
        '''
        :param source_conn_id: This indicated the MS Sql source db connection id
        :param source_system: This indicates the source system name (e.g: MsSQL)
        :param source_database:This indicates the source data base name (Ms SQL)
        :param source_schema_name:This is the source schema name
        :param source_table_name:This is the source table name
        :param target_conn_id:This is the target  DB connection id
        :param target_system: This is the target system name( e.g: Redshift)
        :param target_schema_name: This is target staging schema name prefixed with "_PAS2"
        :param target_table_name:This is the target table name
        :param job_type: Defines the traget load type (e.g: append)
        :param logger_conn_id: This param keep the logger tables target connection id , if it empty then logger
        functionality is switched off
        :param source_schema_sql: This holds source schema extraction sql.
        :param target_schema_sql: This holds target schema extraction sql
        :param args:The special syntax *args in function definitions in python is used to pass a variable number of
        arguments to a function.
        :param kwargs:The special syntax **kwargs in function definitions in python is used to pass a keyworded,
         variable-length argument list.
        '''
        super(NeuSchemaValidatorOperator, self).__init__(*args, **kwargs)
        self.source_conn_id = source_conn_id
        self.source_system = source_system
        self.source_database = source_database
        self.source_schema_name = source_schema_name
        self.source_table_name = source_table_name
        self.target_conn_id = target_conn_id
        self.target_system = target_system
        self.target_schema_name = target_schema_name
        self.target_table_name = target_table_name
        self.job_type=job_type
        self.logger_conn_id=logger_conn_id
        self.source_schema_sql=""
        self.target_schema_sql=""
        self.target_database=target_database
        self.is_logged=False
        self.parameters = parameters

    def __get_schema_extraction_sql__(self, system):
        """
            This function used to  generate source / target schema extraction query .
        :param system: indicate the DB system  from which schema info get extracting ( e.g: mssql ,Redshift)
        :return: sql - Schema extraction sql
        """
        if system == 'mssql' or system == 'synapse':
            logging.info("mssql")
            #tables="'"+"','".join(self.source_table_name) +"'"
            sql = "SELECT s.NAME  AS schema_name,t.NAME AS table_name, c.NAME AS column_name ," \
                  "case when Y.name = 'int' then 'integer' else Y.name end   AS   data_type ," \
                  "case when Y.name = 'datetime' then 0 " \
                  "     when Y.name in ('integer','int')  then 0 else c.MAX_LENGTH end as max_length," \
                  "case when Y.name = 'datetime' then 0 " \
                  " when Y.name in ('integer','int') then 32 else c.PRECISION end AS precision," \
                  "case when Y.name = 'datetime' then 0 else c.SCALE end  as scale," \
                  "case when c.is_nullable = 0 then 0 else 1 end AS is_nullable  " \
                  "FROM sys.SCHEMAS s " \
                  "INNER JOIN sys.TABLES t  ON s.SCHEMA_ID=t.SCHEMA_ID " \
                  "INNER JOIN sys.COLUMNS c  ON t.OBJECT_ID=c.OBJECT_ID " \
                  "INNER JOIN  sys.TYPES Y ON c.SYSTEM_TYPE_ID=Y.SYSTEM_TYPE_ID " \
                  "WHERE Y.NAME <> 'sysname' AND s.NAME ='{}' AND  t.NAME  ='{}'  " \
                  "ORDER BY c.COLUMN_ID".format(self.source_schema_name,self.source_table_name)

        elif system == 'redshift':
            logging.info(system)
            column_exclusion_list = "'sys_change_version','sys_change_operation','sys_change_columns'," \
                                    "'sys_change_context','etl_status','etl_datetime','hard_delete'"

            sql = "SELECT table_schema AS schema_name," \
                  "table_name AS table_name," \
                  "column_name AS column_name," \
                  "case when data_type = 'timestamp without time zone' then 'datetime' " \
                  "when data_type = 'character varying' then 'varchar' else data_type end AS data_type," \
                  "coalesce(CHARACTER_MAXIMUM_LENGTH,0)  AS max_length," \
                  "CASE WHEN NUMERIC_PRECISION IS NULL THEN 0 ELSE NUMERIC_PRECISION END AS precision," \
                  "CASE WHEN NUMERIC_SCALE IS NULL THEN 0 ELSE NUMERIC_SCALE END AS scale," \
                  "CASE WHEN IS_NULLABLE = 'YES' THEN 1 ELSE 0 END AS is_nullable " \
                  "FROM pg_catalog.svv_columns " \
                  "WHERE lower(table_schema) = LOWER('{}') AND   lower(table_name) = LOWER('{}') " \
                  "AND   COLUMN_NAME NOT IN ({}) " \
                  "ORDER BY TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME".format(self.target_schema_name, self.target_table_name, column_exclusion_list)

            # sql = "select table_schema as schema_name,table_name as table_name,column_name as column_name," \
            #       "data_type  as data_type ,CHARACTER_MAXIMUM_LENGTH AS max_length,case when NUMERIC_PRECISION is null" \
            #       " then 0 else NUMERIC_PRECISION end  AS precision," \
            #       " case when NUMERIC_SCALE is null then 0 else NUMERIC_SCALE end AS scale,CASE WHEN IS_NULLABLE='YES' THEN 0 ELSE 1 END AS is_nullable " \
            #       "from pg_catalog.svv_columns where table_schema =lower('{}') and  table_name = lower('{}') " \
            #       "AND COLUMN_NAME NOT IN ({}) ORDER BY TABLE_SCHEMA ,TABLE_NAME ,COLUMN_NAME".format(
            #       self.target_schema_name, self.target_table_name, column_exclusion_list)

            # sql=" SELECT  c.TABLE_SCHEMA  AS SCHEMA_NAME,c.TABLE_NAME,c.COLUMN_NAME,c.DATA_TYPE," \
            #     "c.CHARACTER_MAXIMUM_LENGTH AS MAX_LENGTH,c.NUMERIC_PRECISION AS PRECISION,c.NUMERIC_SCALE AS SCALE," \
            #     "CASE WHEN c.IS_NULLABLE='YES' THEN 0 ELSE 1 END AS IS_NULLABLE FROM  " \
            #     "INFORMATION_SCHEMA.COLUMNS c  WHERE  c.TABLE_SCHEMA='{}' AND " \
            #     "c.TABLE_NAME ='{}'  AND c.COLUMN_NAME NOT IN ({}) " \
            #     "ORDER BY c.TABLE_SCHEMA ,c.TABLE_NAME ,c.COLUMN_NAME".format(
            #     self.target_schema_name.upper(),self.target_table_name.upper(),column_exclusion_list.upper()
            # )
        else:
            sql = None
        return sql

    def __schema_compare__(self,source_tbl_df,target_tbl_df,task_instance,job_instance):
        """
        This is a table level comparison function

        :param source_tbl_df: This dataframe holds the source table definitions
        :param target_tbl_df: This dataframe holds the target table definitions
        :param task_instance: This  holds the current task instance
        :param job_instance: This holds the current job instance
        :return: Boolean : Comparison Result
        """
        logging.info("source_tbl_df == ", source_tbl_df, "target_tbl_df === ", target_tbl_df)
        if source_tbl_df.iloc[0,0]!=target_tbl_df.iloc[0,0]:
            strmsg="Source and Target Schemas are not matching.Please fix the pipeline configurations"
            # self.__PicklesLogCollector__(task_instance,job_instance,'failed',strmsg)
            raise AirflowException(strmsg)
        if source_tbl_df.iloc[0,1]!=target_tbl_df.iloc[0,1]:
            strmsg="Source Table and Target Table are not matching. They are : " + str(
                source_tbl_df.iloc[0, 1]) + ", " + str(
                target_tbl_df.iloc[0, 1]) + " respectively. Please fix the pipeline configurations"
            # self.__PicklesLogCollector__(task_instance,job_instance,'failed',strmsg)
            raise AirflowException(strmsg)
        if source_tbl_df["column_name"].equals(target_tbl_df["column_name"]):
            return True
        else:
            return False

    def __generate_redshift_create_table_script__(self,source_schema_df):
        """
        Used to generate Create Table statement-DDL
        :param self:
        :param source_schema_df: Hold the definitions of source schema
        :return: generated DDL
        """
        logging.info(source_schema_df)
        #Assumption: This fucntion assumed that schema is already existing on Target db
        strddl="CREATE TABLE "+ str(self.target_schema_name).strip().upper() +"."+str(self.target_table_name).strip().upper() +"( "
        for i,row in source_schema_df.iterrows():
            if row.data_type=='NUMBER':
                strdtype='NUMERIC('+ str(row.precision).strip()+','+ str(row.scale).strip() +')'
            elif (row.data_type=='TEXT') and (row.max_length == -1):
                strdtype='VARCHAR'
            elif (row.data_type=='TEXT' ) and (row.max_length>0):
                strdtype='VARCHAR('+str(row.max_length).strip()+')'
            elif (row.data_type=='TIMESTAMP_NTZ' ):
                strdtype='DATETIME'
            else:
                strdtype=row.data_type
            if row.is_nullable ==0:
                strnullable=" NOT NULL"
            else:
                strnullable=" NULL"

            strddl=strddl+ str(row.column_name) +" "+ strdtype + strnullable +","
            logging.info(strddl)

        strddl = strddl + " sys_change_version varchar(200), sys_change_operation varchar(200), sys_change_columns " \
                          "varchar, sys_change_context varchar, etl_status varchar(50), etl_datetime datetime default " \
                          "sysdate, hard_delete varchar(50) );"
        # logging.info(strddl)
        return strddl

    def __generate_synapse_create_table_script__(self,source_schema_df):
        """
        Used to generate Create Table statement-DDL
        :param self:
        :param source_schema_df: Hold the definitions of source schema
        :return: generated DDL
        """
        logging.info(source_schema_df)
        #Assumption: This fucntion assumed that schema is already existing on snowflake db
        strddl="CREATE TABLE "+ str(self.target_schema_name).strip().upper() +"."+str(self.target_table_name).strip().upper() +"( "
        for i,row in source_schema_df.iterrows():
            if row.data_type in ['decimal','tinyint','bigint','integer','numeric','money','smallmoney','float','real']:
                strdtype='NUMERIC('+ str(row.precision).strip()+','+ str(row.scale).strip() +')'
            elif (row.data_type=='text') and (row.max_length == -1):
                strdtype='VARCHAR'
            elif (row.data_type in ['char','varchar','nchar','nvarchar','ntext','text']) and (row.max_length>=0):
                strdtype='VARCHAR('+str(row.max_length).strip()+')'
            elif (row.data_type in ['date','datetime','datetime2','smalldatetime','time', 'timestamp']):
                strdtype='DATETIME2'
            else:
                strdtype=row.data_type
            if row.is_nullable == 0:
                strnullable=" NOT NULL"
            else:
                strnullable=" NULL"
            strddl=strddl+ str(row.column_name) +" "+ strdtype + strnullable +","
            logging.info(strddl)

        strddl = strddl + " sys_change_version varchar(200), sys_change_operation varchar(200), sys_change_columns " \
                          "varchar(200), sys_change_context varchar(200), etl_status varchar(50), etl_datetime datetime" \
                          ", hard_delete varchar(50) );"
        return strddl

    def __get_dropped_columns__(self,dropped_src_column,task_instance=None, job_instance=None):
        """
         used to get string with  droped columns on source table
        :param self:
        :param dropped_src_column: holds dataframe with  dropped columns list
        :return: string
        """
        dropped_column_str=""
        for j,jrow in dropped_src_column.iterrows():
            deleted_column_table=jrow.schema_name+"."+jrow.table_name
            if dropped_column_str=="":
                dropped_column_str=jrow.column_name
            else:
                dropped_column_str=dropped_column_str +", "+ jrow.column_name

            if self.target_system == 'synapse':
                self.__execute_ddl__("ALTER TABLE " + deleted_column_table + " DROP COLUMN " + jrow.column_name + ";", task_instance, job_instance)
        # logging.info("Following columns: "+ dropped_column_str + " of source table: " +
        #              dropped_column_str + " got dropped" )
        return dropped_column_str

    def __get_alter_column__(self,change_src_column_df,changetype,task_instance,job_instance):
        """
        This method is used to generate ALTER DDLs in case there is a valid source schema changes
        :param self:
        :param change_src_column_df: data frame with changed source columns  info
        :param changetype: Indicate  soruce column table change type, like adding columns/fields, data type change ,
        size change ,not null constraint change etc..
        :param task_instance: holds current task instance info
        :param job_instance: holds the current job instance info
        :return: DDL
        """
        strddl=""
        if self.target_system == 'postgres':
            if changetype=='add':
                for i,row in change_src_column_df.iterrows():
                    strddl=strddl+" ALTER TABLE " + str(self.target_schema_name).strip().upper() +"."+str(
                        self.target_table_name).strip().upper() +" ADD COLUMN "
                    if row.data_type=='NUMBER':
                        strdtype='NUMERIC('+ str(int(row.precision)).strip()+','+ str(int(row.scale)).strip() +')'
                    elif (row.data_type=='TEXT') and (row.max_length==-1) :
                        strdtype='VARCHAR'
                    elif (row.data_type=='TEXT' ) and (row.max_length>0):
                        strdtype='VARCHAR('+str(row.max_length).strip()+')'
                    else:
                        strdtype=row.data_type
                    if row.is_nullable ==1:
                        strnullable=" NOT NULL"
                    else:
                        strnullable=" NULL"
                    strddl=strddl+ str(row.column_name) +" "+ strdtype + strnullable +";"
            elif changetype=='modify':
                for i,row in change_src_column_df.iterrows():
                    if row.column_name==row.column_name and row.data_type_left != row.data_type_right:
                        strmsg="Source target data type mismatch for column: "+row.column_name+" of table: "+row.table_name + \
                            " . Unable proceed with data staging. Please fix the issue manually."
                        # self.__PicklesLogCollector__(task_instance,job_instance,'failed',strmsg)
                        raise AirflowException(strmsg)
                    elif row.column_name==row.column_name and row.data_type_left == 'NUMBER' and row.data_type_right \
                            =='NUMBER' and (int(row.precision) > int(row.precision) or int(row.scale)>int(row.scale)):
                        strddl=strddl+"ALTER TABLE "+ str(self.target_schema_name).strip().upper() +"."+str(
                            self.target_table_name).strip().upper() +  " MODIFY COLUMN " + row.column_name +' NUMBER('+ \
                            str(int(row.precision)).strip()+','+ str(int(row.scale)).strip() +');'
                    elif row.column_name==row.column_name and row.data_type == 'TEXT' and row.data_type \
                            =='TEXT' and int(row.max_length) > int(row.max_length) :
                        strddl=strddl+"ALTER TABLE "+ str(self.target_schema_name).strip().upper() +"."+str(
                            self.target_table_name).strip().upper() +  " MODIFY COLUMN " + row.column_name +' TEXT('+ \
                            str(int(row.max_length)).strip()+');'
                    elif row.column_name==row.column_name and row.data_type == 'NUMBER' and row.data_type \
                            =='NUMBER' and (int(row.precision) < int(row.precision) or int(row.scale)<int(row.scale)):
                        strmsg="Source  data type precision or scale decreased for column: " + row.column_name + " of table :" \
                            + row.table_name + " . Unable proceed with data staging. Please fix the issue manually."
                        # self.__PicklesLogCollector__(task_instance,job_instance,'failed',strmsg)
                        raise AirflowException(strmsg)
            elif changetype=='modifyisnull':
                #Redshift support altering not null to null . How ever altering null to not null will work only if the
                # data existing for that column in all the rows . so please check nad handel this manually.
                for i,row in change_src_column_df.iterrows():
                    if row.column_name==row.column_name and int(row.is_nullable) == 0 and int(row.is_nullable) == 1:
                        strddl=strddl+"ALTER TABLE "+ str(self.target_schema_name).strip().upper() +"."+str(
                            self.target_table_name).strip().upper() +  " MODIFY COLUMN " + row.column_name +" DROP NOT NULL;"
                    elif row.column_name==row.column_name and int(row.is_nullable) == 1 and int(row.is_nullable) == 0:
                        strmsg="Source target not null constrint mis match for column: "+row.column_name+" of table: "+ \
                            row.table_name+ " . Unable proceed with data staging. Please fix the issue manually."
                        # self.__PicklesLogCollector__(task_instance,job_instance,'failed',
                        #                              strmsg)
                        raise AirflowException(strmsg)

        elif self.target_system == 'synapse':
            numeric_list = ['decimal','tinyint','bigint','integer','numeric','money','smallmoney','float','real']
            varchar_list = ['char','varchar','nchar','nvarchar','ntext','text']
            date_list = ['date','datetime','datetime2','smalldatetime','time', 'timestamp']
            if changetype=='add':
                for i,row in change_src_column_df.iterrows():
                    strddl=strddl+" ALTER TABLE "+ str(self.target_schema_name).strip().upper() +"."+str(
                        self.target_table_name).strip().upper() +" ADD "
                    if row.data_type_left in numeric_list:
                        strdtype='NUMERIC('+ str(int(row.precision_left)).strip()+','+ str(int(row.scale_left)).strip() +')'
                    elif (row.data_type_left=='text') and (row.max_length_left == -1):
                        strdtype='VARCHAR'
                    elif (row.data_type_left in varchar_list) and (row.max_length_left>0):
                        strdtype='VARCHAR('+str(int(row.max_length_left)).strip()+')'
                    elif (row.data_type_left in date_list):
                        strdtype='DATETIME2'
                    else:
                        strdtype=row.data_type_left
                    if row.is_nullable_left == 0:
                        strnullable=" NOT NULL"
                    else:
                        strnullable=" NULL"
                    strddl=strddl+ str(row.column_name) +" "+ strdtype + strnullable +";"

            elif changetype=='modify':
                for i,row in change_src_column_df.iterrows():
                    if ((row.data_type_left in numeric_list and row.data_type_right =='numeric') \
                        and (int(row.precision_left) >= int(row.precision_right) or int(row.scale_left)>=int(row.scale_right)))\
                            or (row.data_type_left in numeric_list and row.data_type_right !='numeric'):
                        strddl=strddl+"ALTER TABLE "+ str(self.target_schema_name).strip().upper() +"."+str(
                            self.target_table_name).strip().upper() +  " ALTER COLUMN " + row.column_name +' NUMERIC('+ \
                            str(int(row.precision_left)).strip()+','+ str(int(row.scale_left)).strip() +')'

                    elif ((row.data_type_left in varchar_list and row.data_type_right =='varchar') \
                        and int(row.max_length_left) >= int(row.max_length_right)) \
                            or (row.data_type_left in varchar_list and row.data_type_right !='varchar'):
                        strddl=strddl+"ALTER TABLE "+ str(self.target_schema_name).strip().upper() +"."+ str(
                            self.target_table_name).strip().upper() +  " ALTER COLUMN " + row.column_name +' VARCHAR('+ \
                            str(int(row.max_length_left)).strip()+')'
                    
                    elif row.data_type_left in date_list:
                        strddl = strddl + "ALTER TABLE " + str(self.target_schema_name).strip().upper() + "." + str(
                            self.target_table_name).strip().upper() + " ALTER COLUMN " + row.column_name + " DATETIME2"

                    elif (row.data_type_left in numeric_list and row.data_type_right =='numeric' \
                        or row.data_type_left in varchar_list and row.data_type_right =='varchar') \
                            and (int(row.precision_left) < int(row.precision_right) or int(row.scale_left)<int(row.scale_right)):
                        strmsg="Source data type precision or scale decreased for column: " + row.column_name + " of table :" \
                            + row.table_name + " . Unable proceed with data staging. Please fix the issue manually."
                        raise AirflowException(strmsg)

                    else:
                        strmsg="Source target data type mismatch for column: "+row.column_name+" of table: "+row.table_name + \
                            " . Unable proceed with data staging. Please fix the issue manually."
                        raise AirflowException(strmsg)
                        
                    if int(row.is_nullable_right) == 1:
                        strddl = strddl + ' NULL;'
                    else:
                        strddl = strddl + ' NOT NULL;'

            elif changetype=='modifyisnull':
                # Synapse support altering not null to null. How ever altering null to not null will work only if the
                # data existing for that column in all the rows. so please check nad handel this manually.
                for i,row in change_src_column_df.iterrows():
                    if row.column_name==row.column_name and int(row.is_nullable_left) == 1 and int(row.is_nullable_right) == 0:
                        strddl=strddl+"ALTER TABLE "+ str(self.target_schema_name).strip().upper() +"."+str(
                            self.target_table_name).strip().upper() +  " ALTER COLUMN " + row.column_name + " "
                        if row.data_type_right == 'numeric':
                            strddl = strddl + row.data_type_right + '(' + str(row.precision_right).strip()+','+ str(row.scale_right).strip() +')' + ' NULL;'
                        elif row.data_type_right == 'varchar':
                            strddl = strddl + row.data_type_right + '(' + str(row.max_length_right).strip() + ')' + ' NULL;'
                        else:
                            strddl = strddl + row.data_type_right + ' NULL;'
                    elif row.column_name==row.column_name and int(row.is_nullable_left) == 0 and int(row.is_nullable_right) == 1:
                        strmsg="Source target not null constraint mis match for column: "+row.column_name+" of table: "+ \
                            row.table_name+ " . Unable proceed with data staging. Please fix the issue manually."
                        raise AirflowException(strmsg)
        return strddl

    def __execute_ddl__(self,strddl,task_instance,job_instance):
        """
        This is the execute DDL method
        :param self:
        :param strddl: Holds DDLs to be executed
        :param task_instance:  holds task instance info
        :param job_instance: holds job instance info
        :return: Boolean
        """
        self.destination_hook = OdbcHook(odbc_conn_id=self.target_conn_id, database=self.target_database)
        self.destination_hook.run(sql=strddl, autocommit=True, parameters=self.parameters)

        return True


    def __NeuSchemaValidator__(self, data=None,task_instance=None,job_instance=None):
        """
        This method used to compare and validate - source target schema . Also used to adjust the taget schema if needed
        :param self:
        :param data: Holds the data ,if any , here it is null
        :param task_instance: Holds the current task instance
        :param job_instance:  Holds the current job instance
        :return: Boolean
        """
        try:
            schemaloggerstr=""
            if self.source_system=='mssql':
                source_sql = self.__get_schema_extraction_sql__(system=self.source_system)
                logging.info("source sql === ",source_sql)
                self.source_schema_sql=source_sql
                logging.info(str(self.source_conn_id))
                logging.info(str(self.source_database))
                logging.info("The schema of source database is {}".format(self.source_schema_name))

                mssqlhook = OdbcHook(odbc_conn_id=self.source_conn_id, database=self.source_database)
                source_schema_df= mssqlhook.get_pandas_df(sql=source_sql, parameters=None)
                logging.info("source schema df ==== ", source_schema_df)
                if source_schema_df.empty == True :
                    strmsg="Either Source table is not existing or source schema query failed.Unable to execute the " \
                           "pipeline"
                    # self.__PicklesLogCollector__(task_instance,job_instance,'failed',strmsg)
                    raise AirflowException(strmsg)
                else:
                    if self.target_system=='redshift':
                        logging.info("target system ===", self.target_system)

                        target_sql = self.__get_schema_extraction_sql__(system=self.target_system)
                        logging.info("target_sql === ", target_sql)
                        self.target_schema_sql=target_sql

                        redshifthook = OdbcHook(odbc_conn_id=self.target_conn_id, database=self.target_database)
                        target_schema_df = redshifthook.get_pandas_df(sql=target_sql, parameters=None)
                        target_schema_df.info()

                        if target_schema_df.empty == True:
                            strddl=self.__generate_redshift_create_table_script__(source_schema_df)
                            if self.__execute_ddl__(strddl,task_instance,job_instance):
                                target_schema_df = redshifthook.get_pandas_df(sql=target_sql, parameters=None)
                                schemaloggerstr = "New stage table: " + self.target_schema_name.upper() + "." + str(
                                    self.target_table_name.upper()) + " created. The DDL is:\n " + strddl

                    elif self.target_system=='synapse':
                        logging.info("target system ===", self.target_system)

                        target_sql = self.__get_schema_extraction_sql__(system=self.target_system)
                        logging.info("target_sql === ", target_sql)
                        self.target_schema_sql=target_sql

                        synapsehook = OdbcHook(odbc_conn_id=self.target_conn_id, database=self.target_database)
                        target_schema_df = synapsehook.get_pandas_df(sql=target_sql, parameters=None)
                        target_schema_df.info()

                        if target_schema_df.empty == True :
                            strddl=self.__generate_synapse_create_table_script__(source_schema_df)
                            if self.__execute_ddl__(strddl,task_instance,job_instance):
                                target_schema_df = synapsehook.get_pandas_df(sql=target_sql, parameters=None)
                                schemaloggerstr = "New stage table: " + self.target_schema_name.upper() + "." + str(
                                    self.target_table_name.upper()) + " created. The DDL is:\n " + strddl

                    schema_match=True
                    if self.source_table_name !="":
                        # logging.info("Compairing table " + self.source_table_name)
                        source_tbl_df = source_schema_df[source_schema_df.table_name== self.source_table_name]
                        target_tbl_df = target_schema_df[target_schema_df.table_name== self.source_table_name]
                        schema_match = self.__schema_compare__(source_tbl_df=source_tbl_df, target_tbl_df=target_tbl_df,task_instance=task_instance,job_instance=job_instance)
                        logging.info("schema match === ", schema_match)
                        if schema_match == False:
                            outer_src_column_df=pd.merge(source_tbl_df,target_tbl_df,left_on=['schema_name','table_name',
                                                                                           'column_name'],
                                                        right_on=['schema_name','table_name','column_name'],suffixes=('_left','_right'),
                                                        how='outer', indicator=True)

                            dropped_src_column=outer_src_column_df[(outer_src_column_df['_merge']=='right_only') & (~outer_src_column_df.column_name.isin(['sys_change_version','sys_change_operation','sys_change_columns','sys_change_context','etl_status','etl_datetime','hard_delete']))]

                            if dropped_src_column.empty==False:
                                dropped_column_str=self.__get_dropped_columns__(dropped_src_column, task_instance, job_instance)
                                schemaloggerstr="\nFollowing columns: "+ dropped_column_str + " of source table: " + \
                                self.source_schema_name.upper()+ "." + self.source_table_name.upper() + " got dropped"
                            
                            missing_src_column=outer_src_column_df[outer_src_column_df['_merge']=='left_only']
                            logging.info("missing column header ===", missing_src_column.head())

                            if missing_src_column.empty==False:
                                strddl=self.__get_alter_column__(change_src_column_df=missing_src_column,changetype='add',
                                                        task_instance=task_instance,job_instance=job_instance)
                                if self.__execute_ddl__(strddl,task_instance,job_instance):
                                    schemaloggerstr="\nNew columns are added into target table. The alter DDL statment is as follows :\n " + strddl

                            remaining_src_column=outer_src_column_df[outer_src_column_df['_merge']=='both']
                            
                            if remaining_src_column.empty ==False :
                                logging.info("remaining src column == ",remaining_src_column.columns)
                                strmodifyddl=self.__get_alter_column__(change_src_column_df=remaining_src_column,
                                                                   changetype='modify',task_instance=task_instance,job_instance=job_instance)
                                if strmodifyddl !="":
                                   if self.__execute_ddl__(strmodifyddl,task_instance,job_instance):
                                    schemaloggerstr="\nModified columns of  target table. The alter DDL statment is as follows " \
                                                    ":\n " + strmodifyddl

                            #check not null constrains change on remaining columns
                            if remaining_src_column.empty ==False :
                                strmodifynotnullddl=self.__get_alter_column__(change_src_column_df=remaining_src_column,
                                                                   changetype='modifyisnull',task_instance=task_instance,job_instance=job_instance)
                                if strmodifynotnullddl !="":
                                    if self.__execute_ddl__(strmodifynotnullddl,task_instance,job_instance):
                                        schemaloggerstr="\nModified notnull columns of  target table. The alter DDL statment is " \
                                                        "as follows :\n " + strmodifynotnullddl
                        # logging.info(schemaloggerstr)
                        task_instance.xcom_push(self.source_schema_name+"_"+self.source_table_name+"_logger",schemaloggerstr)
                        task_instance.xcom_push(self.source_schema_name+"_"+self.source_table_name+"_schema_df",source_schema_df)
            # self.__PicklesLogCollector__(task_instance,job_instance,'sucess',schemaloggerstr)
        except AirflowException as err:
            # self.__PicklesLogCollector__(task_instance,job_instance,'failed',"Schema validation failed")
            raise AirflowException(err)
        return True

    def __NeuLogCollector__(self,task_instance,job_instance,status,schemaloggerstr):
        """
        This methods collect the job/taks instance execution status
        :param self:
        :param task_instance: Holds task instance details
        :param job_instance: Holds job instance details
        :param status:  Current execution status
        :param schemaloggerstr: System generted generic log message
        :return: Boolean
        """
        if self.logger_conn_id:
            logdata={
                'JOB_RUN_ID':job_instance.run_id,
                'JOB_NAME':job_instance.dag_id,
                'TASK_ID':task_instance.task_id,
                'START_DATETIME':job_instance.execution_date,
                'END_DATETIME':datetime.now(),
                'JOB_TYPE':self.job_type,
                'JOB_STATUS':status,
                'SOURCE_RECORD_COUNT':0,
                'DESTINATION_RECORD_COUNT':0,
                'JOB_MESSAGE':schemaloggerstr,
                'SOURCE_QUERY':self.source_schema_sql,
                'DESTINATION_QUERY':self.target_schema_sql}
            df = pd.DataFrame([logdata])
            if self.is_logged==False:
                # conn = BaseHook.get_connection(self.logger_conn_id)
                # pg_connection_string = 'postgresql://' + conn.password + ':' + str(self.logger_conn_id.password) + '@' \ 		+ str(self.logger_conn_id.host) + ':' + str(self.logger_conn_id.port).encode('utf-8') + '/' + str(self.logger_conn_id.schema)
                # hook = PostgresHook(postgres_conn_id=self.logger_conn_id)
                # conn_config = hook.get_conn()
                # engine = hook.get_sqlalchemy_engine(engine_kwargs={"connect_args": conn_config})
                # connection = engine.connect()
                conn = BaseHook.get_connection(self.target_conn_id)
                login = conn.login
                port = conn.port
                password = conn.password
                host = conn.host
                schema = conn.schema
                connection_string = 'postgresql://{}:{}@{}:{}/{}'.format(login, password, host, port, self.target_database)
                logging.info("connection_string ==== ", connection_string )

                connection = create_engine(connection_string)



                df.to_sql('df_test2', connection, index=False, if_exists='append', schema=schema)



            self.is_logged=True
        return True

    def execute(self, context):
        """
            This is the execute method for this class
            :param self: inidcates the instance of the class
            :param context: indicates the current context
            :return: return the transfromed data
        """
        task_instance = context['task_instance']
        job_instance=context['dag_run']
        logging.info("source systems and target ===", self.source_system, self.target_system)
        return self.__NeuSchemaValidator__(None, task_instance,job_instance)

