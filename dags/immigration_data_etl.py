import sys
sys.path.append("/home/ubuntu/immigration_code/")
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
from utils.load_config import load_yaml
from utils.aws_utils import add_step_to_emr
from utils.logging_framework import log
from utils.send_email import notify_email

config_path = "/home/ubuntu/immigration_code/utils/config.yaml"

# Load the config file
log.info("Importing config file from {}".format(config_path))
config = load_yaml(config_path)
log.info("Succesfully imported the config file from {}".format(config_path))

with DAG(**config['dag']) as dag:

    # Create egg file and copy application files to s3
    upload_code = PythonOperator(
        task_id="upload_app_to_s3", python_callable=copy_app_to_s3
    )

    # Start the cluster
    log.info("Creating emr cluster")
    cluster_creator = EmrCreateJobFlowOperator(
        task_id="create_job_flow",
        job_flow_overrides=config["emr"],
        aws_conn_id="aws_default",
        emr_conn_id="emr_default",
        on_failure_callback=notify_email,
    )
    log.info("emr cluster succesfully created")

    log.info("Adding task add_steps1 to Spark cluster")
    # Step to run the test submit
    SPARK_STEP = add_step_to_emr(
       task_id="add_steps1",
       egg=config["s3"]["egg"],
       runner=config["s3"]["runner"],
    )

    step_adder = EmrAddStepsOperator(
        task_id="add_steps1",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id="aws_default",
        steps=SPARK_STEP,
        on_failure_callback=notify_email,
    )
    log.info("Task add_steps1 succesfully added to Spark cluster")

    step_checker = EmrStepSensor(
        task_id="watch_step",
        job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps1', key='return_value')[0] }}",
        aws_conn_id="aws_default",
        on_failure_callback=notify_email,
    )

    cluster_remover = EmrTerminateJobFlowOperator(
        task_id="remove_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id="aws_default",
        on_failure_callback=notify_email,
    )
    log.info("Cluster succesfully terminated")

    upload_code >> cluster_creator >> step_adder >> step_checker >> cluster_remover
