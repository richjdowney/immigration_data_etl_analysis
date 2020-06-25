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


DEFAULT_ARGS = {
    "owner": "Rich",
    "start_date": datetime(2020, 6, 20),
    "end_date": datetime(2020, 6, 20),
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
    "email": ["downey2k@hotmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
}

JOB_FLOW_OVERRIDES = {
    "Instances": {
        "Ec2KeyName": "spark-cluster",
        "InstanceGroups": [
            {
                "InstanceCount": 1,
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "Name": "Master node",
            },
            {
                "Name": "Slave nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "Name": "test_cluster",
    "LogUri": "s3://aws-logs-800613416076-us-west-2",
    "ReleaseLabel": "emr-5.28.0",
    "ServiceRole": "EMR_DefaultRole",
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
    SPARK_TEST_STEP = [
        {
            'Name': "Run spark step",
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                "Args": [
                    "spark-submit",
                    "--deploy-mode",
                    "cluster",
                    "--py-files",
                    "s3://immigration-data-etl/test_submit-0.1-py3.7.egg",
                    "s3://immigration-data-etl/spark_runner.py",
                    "module 1"
                ]
            }
        }
    ]

    step_adder = EmrAddStepsOperator(
        task_id="add_steps",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id="aws_default",
        steps=SPARK_TEST_STEP,
    )

    step_checker = EmrStepSensor(
        task_id="watch_step",
        job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
        aws_conn_id="aws_default",
    )

    cluster_remover = EmrTerminateJobFlowOperator(
        task_id="remove_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id="aws_default",
    )

    upload_code >> cluster_creator >> step_adder >> step_checker >> cluster_remover
