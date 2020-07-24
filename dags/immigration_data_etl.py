from airflow import DAG
from airflow.contrib.operators.emr_create_job_flow_operator import (
    EmrCreateJobFlowOperator,
)
from airflow.contrib.operators.emr_terminate_job_flow_operator import (
    EmrTerminateJobFlowOperator,
)
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from config.load_etl_config import load_yaml
from config import constants
from config.load_etl_config import Config
from utils.send_email import notify_email
from utils.run_spark_jobs import add_spark_step
from utils.logging_framework import log
from utils.copy_app_to_s3 import copy_app_to_s3


# Load the config file
config = load_yaml(constants.config_path)

# Check the config types
try:
    Config(**config)
except TypeError as error:
    log.error(error)

with DAG(**config["dag"]) as dag:

    # Create egg file
    create_egg = BashOperator(
        task_id="create_app_egg",
        bash_command="cd /home/ubuntu/immigration_code && python /home/ubuntu/immigration_code/setup.py bdist_egg",
        run_as_user='airflow'
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
        task=task,
        path_to_egg=config["s3"]["egg"],
        runner=config["s3"]["runner"],
        input_path=config["input"]["InputPath"],
        input_file=config["input"]["ImmigrationInput"],
        staging_path=config["staging"]["ImmigrationStaging"],
    )

    # Stage the airport codes data
    task = "stage_airport_codes"

    stage_airport_codes, airport_codes_step_sensor = add_spark_step(
        task=task,
        path_to_egg=config["s3"]["egg"],
        runner=config["s3"]["runner"],
        input_path=config["input"]["InputPath"],
        input_file=config["input"]["AirportCodesInput"],
        staging_path=config["staging"]["AirportStaging"],
    )

    task = "stage_cities"

    stage_cities, cities_step_sensor = add_spark_step(
        task=task,
        path_to_egg=config["s3"]["egg"],
        runner=config["s3"]["runner"],
        input_path=config["input"]["InputPath"],
        input_file=config["input"]["CitiesInput"],
        staging_path=config["staging"]["CitiesStaging"],
    )

    task = "stage_cit_res_map"

    stage_cit_res_map, cit_res_map_step_sensor = add_spark_step(
        task=task,
        path_to_egg=config["s3"]["egg"],
        runner=config["s3"]["runner"],
        input_path=config["input"]["InputPath"],
        input_file=config["input"]["CitResMapInput"],
        staging_path=config["staging"]["CitResMapStaging"],
    )

    task = "stage_mode_map"

    stage_mode_map, mode_map_step_sensor = add_spark_step(
        task=task,
        path_to_egg=config["s3"]["egg"],
        runner=config["s3"]["runner"],
        input_path=config["input"]["InputPath"],
        input_file=config["input"]["ModeMapInput"],
        staging_path=config["staging"]["ModeMapStaging"],
    )

    task = "stage_state_map"

    stage_state_map, state_map_step_sensor = add_spark_step(
        task=task,
        path_to_egg=config["s3"]["egg"],
        runner=config["s3"]["runner"],
        input_path=config["input"]["InputPath"],
        input_file=config["input"]["StateMapInput"],
        staging_path=config["staging"]["StateMapStaging"],
    )

    task = "stage_visa_map"

    stage_visa_map, visa_map_step_sensor = add_spark_step(
        task=task,
        path_to_egg=config["s3"]["egg"],
        runner=config["s3"]["runner"],
        input_path=config["input"]["InputPath"],
        input_file=config["input"]["VisaMapInput"],
        staging_path=config["staging"]["VisaMapStaging"],
    )

    task = "stage_port_map"

    stage_port_map, port_map_step_sensor = add_spark_step(
        task=task,
        path_to_egg=config["s3"]["egg"],
        runner=config["s3"]["runner"],
        input_path=config["input"]["InputPath"],
        input_file=config["input"]["PortMapInput"],
        staging_path=config["staging"]["PortMapStaging"],
    )

    # Remove the cluster
    cluster_remover = EmrTerminateJobFlowOperator(
        task_id="remove_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id="aws_default",
        on_failure_callback=notify_email,
    )

    create_egg >> upload_code >> cluster_creator >> stage_immigration >> immigration_step_sensor >> cluster_remover
    cluster_creator >> stage_airport_codes >> airport_codes_step_sensor >> cluster_remover
    cluster_creator >> stage_cities >> cities_step_sensor >> cluster_remover
    cluster_creator >> stage_cit_res_map >> cit_res_map_step_sensor >> cluster_remover
    cluster_creator >> stage_mode_map >> mode_map_step_sensor >> cluster_remover
    cluster_creator >> stage_state_map >> state_map_step_sensor >> cluster_remover
    cluster_creator >> stage_visa_map >> visa_map_step_sensor >> cluster_remover
    cluster_creator >> stage_port_map >> port_map_step_sensor >> cluster_remover
