from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
SLACK_CONN_ID = 'slack'
def neu_dataops_slack_fail_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
    
            :heavy_exclamation_mark: *Task Failed*\n*Task*: {task}\n*Dag*: {dag}\n*Execution Time*: {exec_date}\n*Log Url*: <{log_url}|*Airflow Log*>
            
            """.format(
            task=context.get('task_instance').task_id,
            dag=context.get('task_instance').dag_id,
            ti=context.get('task_instance'),
            exec_date=context.get('execution_date'),
            log_url=context.get('task_instance').log_url,
        )
    failed_alert = SlackWebhookOperator(
        task_id='slack_alert',
        http_conn_id='slack',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='Airflow')
    return failed_alert.execute(context=context)


def neu_dataops_slack_sucess_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """

            :heavy_exclamation_mark: *Pipeline Successful*\n*Task*: {task}\n*Dag*: {dag}\n*Execution Time*: {exec_date}

            """.format(
        task=context.get('task_instance').task_id,
        dag=context.get('task_instance').dag_id,
        ti=context.get('task_instance'),
        exec_date=context.get('execution_date'),
    )
    success_alert = SlackWebhookOperator(
        task_id='slack_alert',
        http_conn_id='slack',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='Airflow')
    return success_alert.execute(context=context)


