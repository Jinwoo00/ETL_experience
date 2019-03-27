import airflow
from airflow import DAG
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.operators.slack_operator import SlackAPIPostOperator
from datetime import datetime, timedelta
from pytz import timezone

def current_time():
    return datetime.now(timezone('Asia/Seoul')).strftime('%Y-%m-%d %H:%M')

SLACK_CHANNEL = ""
SLACK_TOKEN = ""

DEFAULT_ARGS = {
    'owner':'',
    'depends_on_past': False,
    'start_date':datetime(2019, 3, 11),
}

SPARK_STEPS = [
        {
            "Name":"",
            "ActionOnFailure":"TERMINATE_CLUSTER",
            "HadoopJarStep":{
                "Jar":"command-runner.jar",
                "Args":[
                    "spark-submit",
                    "--master", "yarn",
                    "--deploy-mode", "cluster",
                    "--num-executors", "47",
                    "--executor-cores", "1",
                    "--executor-memory", "6G",
                    "spark_application1.py"
                ]
            }
        },
        {
            "Name":"",
            "ActionOnFailure":"TERMINATE_CLUSTER",
            "HadoopJarStep":{
                "Jar":"command-runner.jar",
                "Args":[
                    "spark-submit",
                    "--master", "yarn",
                    "--deploy-mode", "cluster",
                    "--num-executors", "47",           
                    "--executor-cores", "1",           
                    "--executor-memory", "6G",
                    "spark_application2.py"
                ]
            }
        }
]

dag = DAG(
    '',
    catchup=False,
    default_args=DEFAULT_ARGS,
    schedule_interval='0 13 * * *'
)

######################## START POINT ###############################
cluster_creator = EmrCreateJobFlowOperator(
    task_id='create_job_flow',
    aws_conn_id='aws_default',
    emr_conn_id='',
    dag=dag
)

####################################################################
cluster_success_message = SlackAPIPostOperator(
    task_id='cluster_success_message',
    channel=SLACK_CHANNEL,
    token=SLACK_TOKEN,
    text=current_time()+" EMR cluster start",
    dag=dag
)

cluster_fail_message = SlackAPIPostOperator(
    task_id='cluster_fail_message',
    channel=SLACK_CHANNEL,
    token=SLACK_TOKEN,
    text=current_time()+" EMR cluster fail",
    trigger_rule='all_failed',
    dag=dag
)

step_adder = EmrAddStepsOperator(
    task_id='add_steps',
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=SPARK_STEPS,
    dag=dag
)

####################################################################

adder_success_message = SlackAPIPostOperator(
    task_id='adder_success_message',
    channel=SLACK_CHANNEL,
    token=SLACK_TOKEN,
    text=current_time()+" EMR step add",
    dag=dag
)

adder_fail_message = SlackAPIPostOperator(
    task_id='adder_fail_message',
    channel=SLACK_CHANNEL,
    token=SLACK_TOKEN,
    text = current_time()+" EMR step add fail",
    trigger_rule='all_failed',
    dag=dag
)

#STEP - 1################################################################

step_checker_1 = EmrStepSensor(
    task_id='watch_step_1',
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value')}}",
    step_id="{{ task_instance.xcom_pull('add_steps', key='return_value')[0]}}",
    aws_conn_id='aws_default',
    dag=dag
)

step1_success_message = SlackAPIPostOperator(
    task_id='step1_success_message',
    channel=SLACK_CHANNEL,
    token=SLACK_TOKEN,
    text = current_time()+"  step 1. success",
    dag=dag
)

step1_fail_message = SlackAPIPostOperator(
    task_id='step1_fail_message',
    channel=SLACK_CHANNEL,
    token=SLACK_TOKEN,
    text = current_time()+" step 1. fail",
    trigger_rule='all_failed',
    dag=dag
)

#STEP - 2##################################################################


step_checker_2 = EmrStepSensor(
    task_id='watch_step_2',
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value')}}",
    step_id="{{ task_instance.xcom_pull('add_steps', key='return_value')[1]}}",
    aws_conn_id='aws_default',
    dag=dag
)

step2_success_message = SlackAPIPostOperator(
    task_id='step2_success_message',
    channel=SLACK_CHANNEL,
    token=SLACK_TOKEN,
    text = current_time()+" step2. success",
    dag=dag
)

step2_fail_message = SlackAPIPostOperator(
    task_id='step2_fail_message',
    channel=SLACK_CHANNEL,
    token=SLACK_TOKEN,
    text = current_time()+" step2. fail",
    trigger_rule='all_failed',
    dag=dag
)

#######################################################################

cluster_creator >> step_adder
cluster_creator >> cluster_success_message
cluster_creator >> cluster_fail_message

step_adder >> step_checker_1
step_adder >> adder_success_message
step_adder >> adder_fail_message

step_checker_1 >> step1_success_message
step_checker_1 >> step1_fail_message

step1_success_message >> step_checker_2

step_checker_2 >> step2_success_message
step_checker_2 >> step2_fail_message
