import os
from pathlib import Path
import json
from datetime import datetime
# from datetime import timezone
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from operators.neu_dataops_mssql_Redshift_operator import NeuDataOpsMsSqlRedshiftOperator
from operators import NeuDataOpsMsSqlUpsertOperator
from operators.neu_dataops_redshift_operator import NeuDataOpsRedshiftScriptOperator
from operators.neu_dataops_schema_validator_operator import NeuSchemaValidatorOperator
from operators.neu_mssql_ct_to_target_operator import NeuMsSqlCTTargetOperator
from transformation.neu_dataops_get_mssql_table_list import get_mssql_table_list
#from sensors.neu_dataops_mssql_sql_sensor import NeuDataOpsMsSqlSensor
from hooks.neu_dataops_slack_alert import neu_dataops_slack_fail_alert
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.postgres_operator import PostgresOperator



class NeuDataOpsCreateDagOperator:
    """
        Creating common dag for etl workflow on Airflow environment.

    """

    @staticmethod
    def get_config_dictionary(config_path, conf_file):
        """
            This Method reads the job and task config files.
        """
        with open(os.path.join(config_path, conf_file), 'r') as json_file:
            config_dict = json.load(json_file)
            if str(config_dict).strip() == "":
                config_dict = {}
            if not config_dict:
                config_dict = {}
            return config_dict

    @staticmethod
    def create_batch_ingestion_dag(dag_id, default_dag_args):
        """
            This is to create a dag for mini batch etl integration with NeuDataOps application data base using
            configuration files.
          Params
          dag_id - Name of the DAG
          default_dag_args- The default set of arguments need to pass into the DAG

        """
        """
            Assigning Airflow varilable for AIRFLOW_HOME parameter.
        """

        AIRFLOW_HOME = Variable.get("AIRFLOW_HOME")
        AIRFLOW_ENV = Variable.get("AIRFLOW_ENV")

        """
            Bulding JOB_PATH and CONFIG_PATH for reading the job and task configuration's.
        """
        JOB_PATH = os.path.join(AIRFLOW_HOME, "dags", dag_id)
        CONFIG_PATH = os.path.join(JOB_PATH, "config")
        JOB_RESOURCE_PATH = os.path.join(JOB_PATH, "resource")
        AIRFLOW_HOME_PARENT_PATH = Path(AIRFLOW_HOME).parent
        job_dict = NeuDataOpsCreateDagOperator.get_config_dictionary(CONFIG_PATH, "jobconfig.json")
        task_dict = NeuDataOpsCreateDagOperator.get_config_dictionary(CONFIG_PATH, "taskconfig.json")
        """

            Updating the default arguments as per job config.
        """
        job_dict_update = job_dict.copy()
        job_start_date = datetime.strptime(job_dict_update["start_date"], "%Y-%m-%d")
        job_dict_update["start_date"] = job_start_date
        job_dict_update["retries"] = int(job_dict_update["retries"])
        del job_dict_update['job_name']
        del job_dict_update['job_type']
        job_dict_update["on_failure_callback"] = neu_dataops_slack_fail_alert
        # job_dict_update["on_success_callback"] = neu_dataops_slack_sucess_alert

        default_dag_args.update(job_dict_update)

        """
            defining the dag with Deafult arguments and Dag Id.
        """
        if default_dag_args["schedule_interval"].upper() == ("" or "NONE"):
            del default_dag_args["schedule_interval"]
            dag = DAG(dag_id, schedule_interval=None, default_args=default_dag_args)
        else:
            del default_dag_args["schedule_interval"]
            dag = DAG(dag_id, schedule_interval=job_dict["schedule_interval"], default_args=default_dag_args)

        """
            Definition of task nodes starts from here.
        """
        if job_dict['job_type'] == "onetime_load" or job_dict['job_type'] == "incremental_load":
            """
            This part of the code defines the MS SQL change tracking Sensor pipelines
            """

            task_dict_update = task_dict.copy()
            for keys in task_dict_update:
                for i_index, i in enumerate(task_dict_update[keys]):
                    for key in i:
                        if 'conn' in key:
                            i[key] = AIRFLOW_ENV + '_' + i[key]

            task_dict = task_dict_update.copy()

            if not task_dict["source"][0]["source_table"]:
                source_conn_id = task_dict["source"][0]["source_conn_id"]
                source_database = task_dict["source"][0]["source_database"]
                table_list = get_mssql_table_list(source_conn_id, source_database)
                source_table_list = []
                source_pk_list = []
                target_table_list = []
                source_schema = task_dict["source"][0]["source_schema"] if task_dict["source"][0]["source_schema"] else table_list['schema_name'].value_counts().idxmax()
                exclude_tables_list = ['sysdiagrams', 'test']
                for ind in table_list.index:
                    if table_list['schema_name'][ind] == source_schema and \
                            table_list['table_name'][ind] not in exclude_tables_list:
                        source_table_list.append(table_list['table_name'][ind])
                        source_pk_list.append(table_list['pk'][ind])
                        target_table_list.append(table_list['table_name'][ind].upper())

                task_dict["source"][0]["source_table"] = source_table_list
                task_dict["source"][0]["source_key_column"] = source_pk_list
                task_dict["source"][0]["source_schema"] = source_schema

            start_task = DummyOperator(task_id='start', dag=dag)


            wf_validatortask = {}
            wf_srctask = {}
            i = 0

            for tbl in task_dict["source"][0]["source_table"]:
                ct_key_column = task_dict["source"][0]["source_key_column"][i]

                wf_validatortask[tbl] = NeuSchemaValidatorOperator(
                    task_id="schema_validator_" + str(tbl),
                    source_conn_id=task_dict["source"][0]["source_conn_id"],
                    source_system=task_dict["source"][0]["source_system"],
                    source_database=task_dict["source"][0]["source_database"],
                    source_schema_name=task_dict["source"][0]["source_schema"],
                    source_table_name=str(tbl),
                    target_conn_id=task_dict["destination"][0]["target_conn_id"],
                    target_system=task_dict["destination"][0]["target_system"],
                    target_database=task_dict["destination"][0]["target_database"],
                    target_schema_name=task_dict["destination"][0]["target_schema"],
                    target_table_name=str(tbl),
                    job_type=job_dict['job_type'],
                    # logger_conn_id=task_dict["destination"][0]["logger_conn_id"],
                    dag=dag
                    )
                if task_dict["source"][0]["script"] != "":
                    sql_script = JOB_RESOURCE_PATH + "/" + task_dict["source"][0]["script"][i]
                else:
                    sql_script = ""
                wf_srctask[tbl] = NeuMsSqlCTTargetOperator(
                    task_id=task_dict["source"][0]["task_name"] + "_" + task_dict["destination"][0][
                        "task_name"] + "_" +
                            task_dict["destination"][0]["target_schema"].lower() + "." + str(tbl),
                    mssql_conn_id=task_dict["source"][0]["source_conn_id"],
                    sql=task_dict["source"][0]["source_query"],
                    sql_script=sql_script,
                    database=task_dict["source"][0]["source_database"],
                    schema_name=task_dict["source"][0]["source_schema"],
                    table_name=str(tbl),
                    ct_key_column=str(ct_key_column).upper(),
                    destination_system=task_dict["destination"][0]["target_system"],
                    destination_conn_id=task_dict["destination"][0]["target_conn_id"],
                    destination_database=task_dict["destination"][0]["target_database"],
                    destination_schema=task_dict["destination"][0]["target_schema"],
                    destination_table=tbl.lower(),
                    blob_conn_id = task_dict["destination"][0]["blob_conn_id"],
                    load_type=task_dict["destination"][0]["load_type"],
                    source_task_id="schema_validator_" + str(tbl),
                    job_type=job_dict['job_type'],
                    # logger_conn_id=task_dict["destination"][0]["logger_conn_id"],
                    dag=dag)
                i = i + 1

            end_task = DummyOperator(task_id='end', dag=dag)

            if task_dict["destination_trigger"][0]["target_action"] == "trigger":
                wf_trigger_job_task = TriggerDagRunOperator(task_id=task_dict["destination_trigger"][0]["task_name"],
                                                            trigger_dag_id=task_dict["destination_trigger"][0][
                                                                "target_job"], dag=dag)
            """
                Defining the Flow of the different tasks.
            """

            if task_dict["destination_trigger"][0]["target_action"] == "trigger":

                for tbl in task_dict["source"][0]["source_table"]:
                    start_task >> wf_validatortask[tbl] >> wf_trigger_job_task >> end_task
            else:
                for tbl in task_dict["source"][0]["source_table"]:
                    start_task >> wf_validatortask[tbl] >> wf_srctask[tbl] >> end_task

        if job_dict['job_type'] == 'onetime_transformation':
            """
            This section of the code defines the SQL based onetime transformation which supports both watermark 
            as well ETL status based onetime transformation and handling of HARD delete functionality.
            """

            """
                Definition of task nodes starts from here.
            """
            task_dict_update = task_dict.copy()

            for keys in task_dict_update:
                for i_index, i in enumerate(task_dict_update[keys]):
                    for key in i:
                        if 'conn' in key:
                            i[key] = AIRFLOW_ENV + '_' + i[key]
            task_dict.clear()

            wf_transformtask = {}

            """ 
                Definition of task start node.
            """
            start_task = DummyOperator(task_id='start', dag=dag)
            sqlscript = task_dict_update["destination"][0]["script"]
            updateetlstatussqlscript = task_dict_update["destination"][1]["script"]

            wf_transformtask[sqlscript] = NeuDataOpsRedshiftScriptOperator(
                task_id=task_dict_update["destination"][0]["destination_system"] + "_" +
                        task_dict_update["destination"][0]["destination_action"] + "_" + sqlscript,
                conn_id=task_dict_update["destination"][0]["conn_id"],
                sqlscript_path=JOB_RESOURCE_PATH + "/" + task_dict_update["destination"][0]["script"], dag=dag)

            wf_transformtask[updateetlstatussqlscript] = NeuDataOpsRedshiftScriptOperator(
                task_id=task_dict_update["destination"][1]["destination_system"] + "_" +
                        task_dict_update["destination"][1]["destination_action"] + "_" + updateetlstatussqlscript,
                conn_id=task_dict_update["destination"][1]["conn_id"],
                sqlscript_path=JOB_RESOURCE_PATH + "/" + task_dict_update["destination"][1]["script"], dag=dag)

            end_task = DummyOperator(task_id='end', dag=dag)

            start_task >> wf_transformtask[sqlscript] >> wf_transformtask[updateetlstatussqlscript] >> end_task
        '''
        if job_dict['job_type'] == "mssql_ct_sensor":
            """
            This part of the code defines the MS SQL change tracking Sensor pipelines
            """
            task_dict_update = task_dict.copy()
            for i in range(len(task_dict_update['source'])):
                task_dict_update['source'][i]['source_conn_id'] = AIRFLOW_ENV + '_' + task_dict_update['source'][i][
                    'source_conn_id']

            for i in range(len(task_dict_update['destination'])):
                task_dict_update['destination'][i]['target_conn_id'] = AIRFLOW_ENV + '_' + \
                                                                       task_dict_update['destination'][i][
                                                                           'target_conn_id']

            task_dict = task_dict_update.copy()

            start_task = DummyOperator(task_id='start', dag=dag)
            sqlsensortask = task_dict["source"][0]["task_name"]
            sqlsensorquery = task_dict["source"][0]["source_query"]
            sqlsensorconnid = task_dict["source"][0]["source_conn_id"]
            sqldatabase = task_dict["source"][0]["source_database"]
            wf_sourcetask = NeuDataOpsMsSqlSensor(
                task_id=sqlsensortask,
                conn_id=sqlsensorconnid,
                target_conn_id=task_dict["destination"][0]["target_conn_id"],
                database=sqldatabase,
                table=task_dict["source"][0]["source_table"][0],
                sql=sqlsensorquery,
                dag=dag
            )
            if task_dict["destination_trigger"][0]["target_action"] == "trigger":
                wf_trigger_job_task = TriggerDagRunOperator(task_id=task_dict["destination_trigger"][0]["task_name"],
                                                            trigger_dag_id=task_dict["destination_trigger"][0][
                                                                "target_job"], dag=dag)
            # end_task = DummyOperator(task_id='end', dag=dag)
            end_task = PostgresOperator(task_id='end', postgres_conn_id='postgres_default',
                                        sql="delete from xcom where dag_id= '" + dag.dag_id + "' and date(execution_date)=date('{{ ds }}')",
                                        dag=dag)

            start_task >> wf_sourcetask >> wf_trigger_job_task >> end_task
        '''
        return dag
