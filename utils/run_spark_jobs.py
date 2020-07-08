from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from utils.aws_utils import add_step_to_emr
from utils.send_email import notify_email
from utils.logging_framework import log


def add_spark_step(task, path_to_egg, runner, config):

    """ Function to add a Spark step to emr

    Parameters
    ----------
    task : str
        Name of task to execute
    path_to_egg : str
        Path to the egg file containing the main Spark application
    runner : str
        Name of the main runner file
    config : dict
        dictionary with config

    """

    # Add the Spark step
    spark_step = add_step_to_emr(
        task_id="{}".format(task),
        egg=path_to_egg,
        runner=runner,
        input_data_path=config["input"]["InputPath"],
        input_file_name=config["input"]["ImmigrationInput"],
    )

    step_adder = EmrAddStepsOperator(
        task_id="add_step",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id="aws_default",
        steps=spark_step,
        on_failure_callback=notify_email,
    )

    step_checker = EmrStepSensor(
        task_id="watch_{}".format(task),
        job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_step', key='return_value')[0] }}",
        aws_conn_id="aws_default",
        on_failure_callback=notify_email,
    )

    log.info("Step sensor added for task {}".format(task))
    log.info("Step added for task {}".format(task))

    return step_adder, step_checker
