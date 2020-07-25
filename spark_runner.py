import sys
from src.stage_tables.staging_helpers import (
    read_input_data,
    profile_data,
    clean_data,
    write_to_staging,
)
from pyspark.sql import SparkSession
from utils.logging_framework import log

if __name__ == "__main__":

    spark = SparkSession.builder.appName("Immigration_ETL").getOrCreate()

    task = sys.argv[1]
    input_path = sys.argv[2]
    input_file = sys.argv[3]
    output_path = sys.argv[4]

    log.info("Running Spark job for task {}".format(task))
    log.info("Path to input files for task {} is {}".format(task, input_path))
    log.info("Input file name for task {} is {}".format(task, input_file))

    if task == "stage_immigration":

        staging_immigration_data = read_input_data(
            spark=spark, input_data_path=input_path, source=input_file, task=task
        )

        profile_data(staging_immigration_data, "Immigration Data")

        immigration_clean = clean_data(df=staging_immigration_data, task=task)

        write_to_staging(
            df=immigration_clean,
            partition_col="date_file_added",
            num_partitions=20,
            out_path=output_path,
        )

    elif task == "stage_airport_codes":

        staging_airport_codes = read_input_data(
            spark=spark, input_data_path=input_path, source=input_file, task=task
        )

        profile_data(staging_airport_codes, "Airport Codes")

        airport_codes_clean = clean_data(df=staging_airport_codes, task=task)

        write_to_staging(
            df=airport_codes_clean,
            partition_col="ident",
            num_partitions=20,
            out_path=output_path,
        )

    elif task == "stage_cities":

        staging_cities = read_input_data(
            spark=spark, input_data_path=input_path, source=input_file, task=task
        )

        profile_data(staging_cities, "Cities")

        cities_clean = clean_data(df=staging_cities, task=task)

        write_to_staging(
            df=cities_clean,
            partition_col="city",
            num_partitions=20,
            out_path=output_path,
        )

    elif task == "stage_cit_res_map":

        staging_cit_res_map = read_input_data(
            spark=spark, input_data_path=input_path, source=input_file, task=task
        )

        profile_data(staging_cit_res_map, "City and res map")

        cit_res_map_clean = clean_data(df=staging_cit_res_map, task=task)

        write_to_staging(
            df=cit_res_map_clean,
            partition_col="lookup_value",
            num_partitions=1,
            out_path=output_path,
        )

    elif task == "stage_mode_map":

        staging_mode_map = read_input_data(
            spark=spark, input_data_path=input_path, source=input_file, task=task
        )

        profile_data(staging_mode_map, "Mode map")

        mode_map_clean = clean_data(df=staging_mode_map, task=task)

        write_to_staging(
            df=mode_map_clean,
            partition_col="lookup_value",
            num_partitions=1,
            out_path=output_path,
        )

    elif task == "stage_state_map":

        staging_state_map = read_input_data(
            spark=spark, input_data_path=input_path, source=input_file, task=task
        )

        profile_data(staging_state_map, "State map")

        state_map_clean = clean_data(df=staging_state_map, task=task)

        write_to_staging(
            df=state_map_clean,
            partition_col="lookup_value",
            num_partitions=1,
            out_path=output_path,
        )

    elif task == "stage_visa_map":

        staging_visa_map = read_input_data(
            spark=spark, input_data_path=input_path, source=input_file, task=task
        )

        profile_data(staging_visa_map, "Visa map")

        visa_map_clean = clean_data(df=staging_visa_map, task=task)

        write_to_staging(
            df=visa_map_clean,
            partition_col="lookup_value",
            num_partitions=1,
            out_path=output_path,
        )

    elif task == "stage_port_map":

        staging_port_map = read_input_data(
            spark=spark, input_data_path=input_path, source=input_file, task=task
        )

        profile_data(staging_port_map, "Visa map")

        port_map_clean = clean_data(df=staging_port_map, task=task)

        write_to_staging(
            df=port_map_clean,
            partition_col="port",
            num_partitions=1,
            out_path=output_path,
        )

    spark.stop()
