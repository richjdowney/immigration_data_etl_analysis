import sys
sys.path.append("/home/ubuntu/immigration_code/")
from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators.emr_create_job_flow_operator import (
    EmrCreateJobFlowOperator,
)
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import (
    EmrTerminateJobFlowOperator,
)
from airflow.operators.python_operator import PythonOperator
from src.copy_code_to_s3.copy_main import copy_app_to_s3
from src.test_module1.emr_step_config import add_step_to_emr
from utils.load_config import load_yaml

# Load the config file
config = load_yaml("/home/ubuntu/immigration_code/utils/config.yaml")
secrets_config = load_yaml("/home/ubuntu/immigration_code/utils/secrets.yaml")

DEFAULT_ARGS = {
    "owner": config["airflow"]["owner"],
    "start_date": config["airflow"]["start_date"],
    "end_date": config["airflow"]["start_date"],
    "depends_on_past": config["airflow"]["depends_on_past"],
    "retries": config["airflow"]["retries"],
    "retry_delay": timedelta(minutes=config["airflow"]["retry_delay"]),
    "catchup": config["airflow"]["catchup"],
    "email": [config["airflow"]["email"]],
    "email_on_failure": config["airflow"]["email_on_failure"],
    "email_on_retry": config["airflow"]["email_on_retry"],
}

JOB_FLOW_OVERRIDES = {
    "Instances": {
        "Ec2KeyName": secrets_config["aws_secrets"]["Ec2KeyName"],
        "InstanceGroups": [
            {
                "InstanceCount": config["aws"]["MasterInstanceCount"],
                "InstanceRole": "MASTER",
                "InstanceType": config["aws"]["MasterInstanceType"],
                "Name": "Master node",
            },
            {
                "Name": "Slave nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": config["aws"]["SlaveInstanceType"],
                "InstanceCount": config["aws"]["SlaveInstanceCounts"],
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": config["aws"]["TerminateAfterComplete"],
        "TerminationProtected": config["aws"]["TerminateProtect"],
    },
    "JobFlowRole": config["aws"]["JobFlowRole"],
    "Name": config["aws"]["ClusterName"],
    "LogUri": config["aws"]["LogUri"],
    "ReleaseLabel": config["aws"]["ReleaseLabel"],
    "ServiceRole": config["aws"]["ServiceRole"],
}

with DAG(
    dag_id="test_emr_cluster_creation",
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    schedule_interval="@once",
) as dag:

    # Create egg file and copy application files to s3
    upload_code = PythonOperator(
        task_id='upload_app_to_s3',
        python_callable=copy_app_to_s3
    )

    # Start the cluster
    cluster_creator = EmrCreateJobFlowOperator(
        task_id="create_job_flow",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id="aws_default",
        emr_conn_id="emr_default",
    )

    # Step to run the test submit
    SPARK_TEST_STEP = add_step_to_emr(task_id="add_steps1",
                                      egg=config["runner_files"]["egg"],
                                      runner=config["runner_files"]["runner"])

    step_adder = EmrAddStepsOperator(
        task_id="add_steps1",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id="aws_default",
        steps=SPARK_TEST_STEP,
    )

    step_checker = EmrStepSensor(
        task_id="watch_step",
        job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps1', key='return_value')[0] }}",
        aws_conn_id="aws_default",
    )

    cluster_remover = EmrTerminateJobFlowOperator(
        task_id="remove_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id="aws_default",
    )

    upload_code >> cluster_creator >> step_adder >> step_checker >> cluster_remover
