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
from azure.cosmos.partition_key import PartitionKey


class NeuDataOpsMsSqlUpsertOperator(BaseOperator):
    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            sql: str,
            mssql_conn_id: str = 'mssql_default',
            parameters: Optional[Union[Mapping, Iterable]] = None,
            autocommit: bool = False,
            mssql_database: Optional[str] = None,
            mssql_schema_name:Optional[str] = None,
            mssql_tablename:Optional[str] = None,
            incremental_field: Optional[str] = None,
            source_job_name:Optional[str] = None,
            source_task_name:Optional[str] = None,
            *args, **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.mssql_conn_id = mssql_conn_id
        self.sql = sql
        self.parameters = parameters
        self.autocommit = autocommit
        self.mssql_database = mssql_database
        self.mssql_schema_name =mssql_schema_name
        self.mssql_tablename=mssql_tablename
        self.source_job_name=source_job_name
        self.source_task_name=source_task_name
        self.incremental_field = incremental_field

    def __NeuDataOpsMsSqlUpsert__(self):
        hook = OdbcHook(odbc_conn_id=self.mssql_conn_id, database=self.mssql_database)
        watermark_ts=self.__NeuDataOpsGetLastModifiedDate(hook)
        self.sql=self.__prepare_watermark_merge_sql(watermark_ts)
        hook.run(sql=self.sql,autocommit=True, parameters=self.parameters)
        return True

    def __NeuDataOpsGetLastModifiedDate(self, hook):
        sql = "SELECT MAX({}) as modifieddate FROM {}.{}".\
             format(self.incremental_field, self.mssql_schema_name, self.mssql_tablename)
        df = hook.get_pandas_df(sql=sql, parameters=self.parameters)
        for i, row in df.iterrows():
           latest_modified_date=row.modifieddate
        return latest_modified_date

    def __prepare_watermark_merge_sql(self, watermark_ts):
            watermark_ts=str(watermark_ts)
            watermark_ts=watermark_ts[:-3]
            sql="MERGE common.watermark w USING (SELECT '{}' AS jobname, '{}' AS taskname ,'{}' AS datasetobjectname, " \
                "'modifieddate' AS  watermarktsfield, CONVERT(DATETIME,'{}',101) AS lastupdatedts , " \
                "'' AS watermarkoffsetvalue,1 AS isactive," \
                "GETDATE() AS createddate,	" \
                "GETDATE() AS modifieddate,'{}' AS createdby,'{}' AS modifiedby) s".format(self.source_job_name,
                                                                                       self.source_task_name,
                                                                                       self.mssql_tablename,
                                                                                       watermark_ts,
                                                                                       self.source_job_name,
                                                                                       self.source_job_name)
            sql=sql+ " ON w.jobname=s.jobname AND w.taskname=s.taskname AND w.datasetobjectname=s.datasetobjectname " \
                     "WHEN  MATCHED THEN UPDATE SET w.lastupdatedts=s.lastupdatedts , w.modifieddate=GETDATE() " \
                     "WHEN NOT MATCHED THEN INSERT (jobname, taskname, datasetobjectname, watermarktsfield, lastupdatedts, " \
                     "watermarkoffsetvalue, isactive, createddate, modifieddate, createdby, modifiedby)" \
                     " VALUES ( s.jobname, s.taskname, s.datasetobjectname, s.watermarktsfield,s.lastupdatedts," \
                     "s.watermarkoffsetvalue,s.isactive, s.createddate, s.modifieddate, s.createdby, s.modifiedby);"
            logging.info("MERGER: "+sql)
            return sql

    def execute(self,context):
        """
        This is the execute method for this class
        :param self: inidcates the instance of the class
        :param context: indicates the current context
        :return: return the transfromed data
        """
        return self.__NeuDataOpsMsSqlUpsert__()
