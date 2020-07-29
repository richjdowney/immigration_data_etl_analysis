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
        run_as_user="airflow",
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
        staging_path=config["staging"]["ImmigrationStaging"],
        input_path=config["input"]["InputPath"],
        input_file=config["input"]["ImmigrationInput"],
    )

    # Stage the airport codes data
    task = "stage_airport_codes"

    stage_airport_codes, airport_codes_step_sensor = add_spark_step(
        task=task,
        path_to_egg=config["s3"]["egg"],
        runner=config["s3"]["runner"],
        staging_path=config["staging"]["AirportStaging"],
        input_path=config["input"]["InputPath"],
        input_file=config["input"]["AirportCodesInput"],
    )

    task = "stage_cities"

    stage_cities, cities_step_sensor = add_spark_step(
        task=task,
        path_to_egg=config["s3"]["egg"],
        runner=config["s3"]["runner"],
        staging_path=config["staging"]["CitiesStaging"],
        input_path=config["input"]["InputPath"],
        input_file=config["input"]["CitiesInput"],
    )

    task = "stage_cit_res_map"

    stage_cit_res_map, cit_res_map_step_sensor = add_spark_step(
        task=task,
        path_to_egg=config["s3"]["egg"],
        runner=config["s3"]["runner"],
        staging_path=config["staging"]["CitResMapStaging"],
        input_path=config["input"]["InputPath"],
        input_file=config["input"]["CitResMapInput"],
    )

    task = "stage_mode_map"

    stage_mode_map, mode_map_step_sensor = add_spark_step(
        task=task,
        path_to_egg=config["s3"]["egg"],
        runner=config["s3"]["runner"],
        staging_path=config["staging"]["ModeMapStaging"],
        input_path=config["input"]["InputPath"],
        input_file=config["input"]["ModeMapInput"],
    )

    task = "stage_state_map"

    stage_state_map, state_map_step_sensor = add_spark_step(
        task=task,
        path_to_egg=config["s3"]["egg"],
        runner=config["s3"]["runner"],
        staging_path=config["staging"]["StateMapStaging"],
        input_path=config["input"]["InputPath"],
        input_file=config["input"]["StateMapInput"],
    )

    task = "stage_visa_map"

    stage_visa_map, visa_map_step_sensor = add_spark_step(
        task=task,
        path_to_egg=config["s3"]["egg"],
        runner=config["s3"]["runner"],
        staging_path=config["staging"]["VisaMapStaging"],
        input_path=config["input"]["InputPath"],
        input_file=config["input"]["VisaMapInput"],
    )

    task = "stage_port_map"

    stage_port_map, port_map_step_sensor = add_spark_step(
        task=task,
        path_to_egg=config["s3"]["egg"],
        runner=config["s3"]["runner"],
        staging_path=config["staging"]["PortMapStaging"],
        input_path=config["input"]["InputPath"],
        input_file=config["input"]["PortMapInput"],
    )

    task = "create_fact_table"

    create_fact_table, create_fact_table_sensor = add_spark_step(
        task=task,
        path_to_egg=config["s3"]["egg"],
        runner=config["s3"]["runner"],
        staging_path=config["staging"]["ImmigrationStaging"],
        adm_path=config["adm"]["Immigration"],
    )

    task = "create_dim_airport_codes"

    create_dim_airport_codes, create_dim_airport_codes_sensor = add_spark_step(
        task=task,
        path_to_egg=config["s3"]["egg"],
        runner=config["s3"]["runner"],
        staging_path=config["staging"]["AirportStaging"],
        adm_path=config["adm"]["Airport"],
    )

    task = "create_dim_city_demos"

    create_dim_city_demos, create_dim_city_demos_sensor = add_spark_step(
        task=task,
        path_to_egg=config["s3"]["egg"],
        runner=config["s3"]["runner"],
        staging_path=config["staging"]["CitiesStaging"],
        adm_path=config["adm"]["Cities"],
    )

    task = "create_dim_city_res"

    create_dim_city_res, create_dim_city_res_sensor = add_spark_step(
        task=task,
        path_to_egg=config["s3"]["egg"],
        runner=config["s3"]["runner"],
        staging_path=config["staging"]["CitResMapStaging"],
        adm_path=config["adm"]["CitResMap"],
    )

    task = "create_mode_dim"

    create_dim_mode, create_dim_mode_sensor = add_spark_step(
        task=task,
        path_to_egg=config["s3"]["egg"],
        runner=config["s3"]["runner"],
        staging_path=config["staging"]["ModeMapStaging"],
        adm_path=config["adm"]["ModeMap"],
    )

    task = "create_state_dim"

    create_dim_state, create_dim_state_sensor = add_spark_step(
        task=task,
        path_to_egg=config["s3"]["egg"],
        runner=config["s3"]["runner"],
        staging_path=config["staging"]["StateMapStaging"],
        adm_path=config["adm"]["StateMap"],
    )

    task = "create_visa_dim"

    create_dim_visa, create_dim_visa_sensor = add_spark_step(
        task=task,
        path_to_egg=config["s3"]["egg"],
        runner=config["s3"]["runner"],
        staging_path=config["staging"]["VisaMapStaging"],
        adm_path=config["adm"]["VisaMap"],
    )

    task = "create_port_dim"

    create_dim_port, create_dim_port_sensor = add_spark_step(
        task=task,
        path_to_egg=config["s3"]["egg"],
        runner=config["s3"]["runner"],
        staging_path=config["staging"]["PortMapStaging"],
        adm_path=config["adm"]["PortMap"],
    )

    task = "fact_immigration_qc"

    fact_immigration_qc, fact_immigration_qc_sensor = add_spark_step(
        task=task,
        path_to_egg=config["s3"]["egg"],
        runner=config["s3"]["runner"],
        staging_path=config["staging"]["ImmigrationStaging"],
        adm_path=config["adm"]["Immigration"],
    )

    task = "dim_airport_codes_qc"

    dim_airport_codes_qc, dim_airport_codes_qc_sensor = add_spark_step(
        task=task,
        path_to_egg=config["s3"]["egg"],
        runner=config["s3"]["runner"],
        staging_path=config["staging"]["AirportStaging"],
        adm_path=config["adm"]["Airport"],
    )

    task = "dim_city_demos_qc"

    dim_city_qc, dim_city_qc_sensor = add_spark_step(
        task=task,
        path_to_egg=config["s3"]["egg"],
        runner=config["s3"]["runner"],
        staging_path=config["staging"]["CitiesStaging"],
        adm_path=config["adm"]["Cities"],
    )

    task = "dim_cit_res_qc"

    dim_cit_res_qc, dim_cit_res_qc_sensor = add_spark_step(
        task=task,
        path_to_egg=config["s3"]["egg"],
        runner=config["s3"]["runner"],
        staging_path=config["staging"]["CitResMapStaging"],
        adm_path=config["adm"]["CitResMap"],
    )

    task = "dim_mode_qc"

    dim_mode_qc, dim_mode_qc_sensor = add_spark_step(
        task=task,
        path_to_egg=config["s3"]["egg"],
        runner=config["s3"]["runner"],
        staging_path=config["staging"]["ModeMapStaging"],
        adm_path=config["adm"]["ModeMap"],
    )

    task = "dim_state_qc"

    dim_state_qc, dim_state_qc_sensor = add_spark_step(
        task=task,
        path_to_egg=config["s3"]["egg"],
        runner=config["s3"]["runner"],
        staging_path=config["staging"]["StateMapStaging"],
        adm_path=config["adm"]["StateMap"],
    )

    task = "dim_visa_qc"

    dim_visa_qc, dim_visa_qc_sensor = add_spark_step(
        task=task,
        path_to_egg=config["s3"]["egg"],
        runner=config["s3"]["runner"],
        staging_path=config["staging"]["VisaMapStaging"],
        adm_path=config["adm"]["VisaMap"],
    )

    task = "dim_port_qc"

    dim_port_qc, dim_port_qc_sensor = add_spark_step(
        task=task,
        path_to_egg=config["s3"]["egg"],
        runner=config["s3"]["runner"],
        staging_path=config["staging"]["PortMapStaging"],
        adm_path=config["adm"]["PortMap"],
    )

    # Remove the cluster
    cluster_remover = EmrTerminateJobFlowOperator(
        task_id="remove_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id="aws_default",
        on_failure_callback=notify_email,
    )

    create_egg >> upload_code >> cluster_creator

    cluster_creator >> stage_immigration >> immigration_step_sensor >> create_fact_table >> \
        create_fact_table_sensor >> fact_immigration_qc >> fact_immigration_qc_sensor \
        >> cluster_remover

    cluster_creator >> stage_airport_codes >> airport_codes_step_sensor >> create_dim_airport_codes >> \
        create_dim_airport_codes_sensor >> dim_airport_codes_qc >> dim_airport_codes_qc_sensor  \
        >> cluster_remover

    cluster_creator >> stage_cities >> cities_step_sensor >> create_dim_city_demos >> \
        create_dim_city_demos_sensor >> dim_city_qc >> dim_city_qc_sensor \
        >> cluster_remover

    cluster_creator >> stage_cit_res_map >> cit_res_map_step_sensor >> create_dim_city_res >> \
        create_dim_city_res_sensor >> dim_cit_res_qc >> dim_cit_res_qc_sensor \
        >> cluster_remover

    cluster_creator >> stage_mode_map >> mode_map_step_sensor >> create_dim_mode >> \
        create_dim_mode_sensor >> dim_mode_qc >> dim_mode_qc_sensor \
        >> cluster_remover

    cluster_creator >> stage_state_map >> state_map_step_sensor >> create_dim_state >> \
        create_dim_state_sensor >> dim_state_qc >> dim_state_qc_sensor \
        >> cluster_remover

    cluster_creator >> stage_visa_map >> visa_map_step_sensor >> create_dim_visa >> \
        create_dim_visa_sensor >> dim_visa_qc >> dim_visa_qc_sensor \
        >> cluster_remover

    cluster_creator >> stage_port_map >> port_map_step_sensor >> create_dim_port >> \
        create_dim_port_sensor >> dim_port_qc >> dim_port_qc_sensor >> \
        cluster_remover
