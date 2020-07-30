import sys
from src.stage_tables.staging_helpers import read_input_data, profile_data
from utils.data_processing import *
from pyspark.sql import SparkSession
from utils.logging_framework import log

if __name__ == "__main__":

    spark = SparkSession.builder.appName("Staging_Tables").getOrCreate()

    task = sys.argv[1]
    input_path = sys.argv[2]
    input_file = sys.argv[3]
    staging_path = sys.argv[4]
    execution_date = sys.argv[6]

    log.info("Running Spark job for task {}".format(task))
    log.info("Path to input files for task {} is {}".format(task, input_path))
    log.info("Input file name for task {} is {}".format(task, input_file))
    log.info("Staging tables to {}".format(staging_path))
    log.info("Executing DAG on {}".format(execution_date))

    if task == "stage_immigration":

        staging_immigration_data = read_input_data(
            spark=spark, input_data_path=input_path, source=input_file, task=task
        )

        profile_data(staging_immigration_data, "Immigration Data")

        write_to_s3(
            df=staging_immigration_data,
            partition_col="dtadfile",
            num_partitions=20,
            out_path=staging_path,
        )

    elif task == "stage_airport_codes":

        staging_airport_codes = read_input_data(
            spark=spark, input_data_path=input_path, source=input_file, task=task
        )

        profile_data(staging_airport_codes, "Airport Codes")

        write_to_s3(
            df=staging_airport_codes,
            partition_col="ident",
            num_partitions=20,
            out_path=staging_path,
        )

    elif task == "stage_cities":

        staging_cities = read_input_data(
            spark=spark, input_data_path=input_path, source=input_file, task=task
        )

        profile_data(staging_cities, "Cities")

        write_to_s3(
            df=staging_cities,
            partition_col="City",
            num_partitions=20,
            out_path=staging_path,
        )

    elif task == "stage_cit_res_map":

        staging_cit_res_map = read_input_data(
            spark=spark, input_data_path=input_path, source=input_file, task=task
        )

        profile_data(staging_cit_res_map, "City and res map")

        write_to_s3(
            df=staging_cit_res_map,
            partition_col="value",
            num_partitions=1,
            out_path=staging_path,
        )

    elif task == "stage_mode_map":

        staging_mode_map = read_input_data(
            spark=spark, input_data_path=input_path, source=input_file, task=task
        )

        profile_data(staging_mode_map, "Mode map")

        write_to_s3(
            df=staging_mode_map,
            partition_col="value",
            num_partitions=1,
            out_path=staging_path,
        )

    elif task == "stage_state_map":

        staging_state_map = read_input_data(
            spark=spark, input_data_path=input_path, source=input_file, task=task
        )

        profile_data(staging_state_map, "State map")

        write_to_s3(
            df=staging_state_map,
            partition_col="value",
            num_partitions=1,
            out_path=staging_path,
        )

    elif task == "stage_visa_map":

        staging_visa_map = read_input_data(
            spark=spark, input_data_path=input_path, source=input_file, task=task
        )

        profile_data(staging_visa_map, "Visa map")

        write_to_s3(
            df=staging_visa_map,
            partition_col="value",
            num_partitions=1,
            out_path=staging_path,
        )

    elif task == "stage_port_map":

        staging_port_map = read_input_data(
            spark=spark, input_data_path=input_path, source=input_file, task=task
        )

        profile_data(staging_port_map, "Port map")

        write_to_s3(
            df=staging_port_map,
            partition_col="port",
            num_partitions=1,
            out_path=staging_path,
        )

    spark.stop()
