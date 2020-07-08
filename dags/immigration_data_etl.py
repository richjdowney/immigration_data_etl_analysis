from airflow import DAG
from airflow.contrib.operators.emr_create_job_flow_operator import (
    EmrCreateJobFlowOperator,
)
from airflow.contrib.operators.emr_terminate_job_flow_operator import (
    EmrTerminateJobFlowOperator,
)
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from src.copy_code_to_s3.copy_main import copy_app_to_s3
from utils.load_config import load_yaml
from utils.send_email import notify_email
from utils.run_spark_jobs import add_spark_step
from utils import constants

# Load the config file
config = load_yaml(constants.config_path)


with DAG(**config["dag"]) as dag:

    # Create egg file
    create_egg = BashOperator(
        task_id="create_app_egg",
        bash_command="python /home/ubuntu/immigration_code/setup.py bdist_egg",
    )

    # Copy application files to s3
    upload_code = PythonOperator(
        task_id="upload_app_to_s3", python_callable=copy_app_to_s3, op_args=[config]
    )

    # Start the cluster
    cluster_creator = EmrCreateJobFlowOperator(
        task_id="create_job_flow",
        job_flow_overrides=config["emr"],
        aws_conn_id="aws_default",
        emr_conn_id="emr_default",
        on_failure_callback=notify_email,
    )

    # Stage the immigration data
    task = "stage_immigration"

    stage_immigration, immigration_step_sensor = add_spark_step(
        task=task, path_to_egg=config["s3"]["egg"], runner=config["s3"]["runner"], config=config
    )

    # Remove the cluster
    cluster_remover = EmrTerminateJobFlowOperator(
        task_id="remove_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id="aws_default",
        on_failure_callback=notify_email,
    )

    create_egg >> upload_code >> cluster_creator >> stage_immigration >> immigration_step_sensor >> cluster_remover
