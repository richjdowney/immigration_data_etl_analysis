import sys
from src.stage_tables.staging_helpers import read_input_data, profile_data
from src.create_facts.create_facts_helpers import *
from src.create_dims.create_dims_helpers import *
from utils.data_processing import *
from pyspark.sql import SparkSession
from utils.logging_framework import log

if __name__ == "__main__":

    spark = SparkSession.builder.appName("Immigration_ETL").getOrCreate()

    task = sys.argv[1]
    input_path = sys.argv[2]
    input_file = sys.argv[3]
    staging_path = sys.argv[4]
    adm_path = sys.argv[4]

    log.info("Running Spark job for task {}".format(task))
    log.info("Path to input files for task {} is {}".format(task, input_path))
    log.info("Input file name for task {} is {}".format(task, input_file))
    log.info("ADM path passed for task {} is {}".format(task, adm_path))

    if task == "stage_immigration":

        staging_immigration_data = read_input_data(
            spark=spark, input_data_path=input_path, source=input_file, task=task
        )

        profile_data(staging_immigration_data, "Immigration Data")

        write_to_s3(
            df=staging_immigration_data,
            partition_col="date_file_added",
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
            partition_col="city",
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
            partition_col="lookup_value",
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
            partition_col="lookup_value",
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
            partition_col="lookup_value",
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
            partition_col="lookup_value",
            num_partitions=1,
            out_path=staging_path,
        )

    elif task == "stage_port_map":

        staging_port_map = read_input_data(
            spark=spark, input_data_path=input_path, source=input_file, task=task
        )

        profile_data(staging_port_map, "Visa map")

        write_to_s3(
            df=staging_port_map,
            partition_col="port",
            num_partitions=1,
            out_path=staging_path,
        )

    elif task == "create_fact_table":

        immigration_fact = read_staging_data(spark=spark, path=staging_path)

        immigration_fact = clean_immigration(immigration_fact)

        write_to_s3(
            df=immigration_fact,
            partition_col="date_file_added",
            num_partitions=20,
            out_path=adm_path,
        )

    elif task == "create_dim_airport_codes":

        airport_codes_dim = read_staging_data(spark=spark, path=staging_path)

        airport_codes_dim = clean_airport_codes_dim(airport_codes_dim)

        write_to_s3(
            df=airport_codes_dim,
            partition_col="ident",
            num_partitions=20,
            out_path=adm_path,
        )

    elif task == "create_dim_city_demos":

        city_demos_dim = read_staging_data(spark=spark, path=staging_path)

        city_demos_dim = clean_cities_dim(city_demos_dim)

        write_to_s3(
            df=city_demos_dim,
            partition_col="city",
            num_partitions=20,
            out_path=adm_path,
        )

    elif task == "create_dim_city_res":

        city_res_dim = read_staging_data(spark=spark, path=staging_path)

        city_demos_dim = clean_other_dims(city_res_dim)

        write_to_s3(
            df=city_res_dim,
            partition_col="lookup_value",
            num_partitions=1,
            out_path=adm_path,
        )

    elif task == "create_mode_dim":

        mode_dim = read_staging_data(spark=spark, path=staging_path)

        mode_dim = clean_other_dims(mode_dim)

        write_to_s3(
            df=mode_dim,
            partition_col="lookup_value",
            num_partitions=1,
            out_path=adm_path,
        )

    elif task == "create_state_dim":

        state_dim = read_staging_data(spark=spark, path=staging_path)

        state_dim = clean_other_dims(state_dim)

        write_to_s3(
            df=state_dim,
            partition_col="lookup_value",
            num_partitions=1,
            out_path=adm_path,
        )

    elif task == "create_visa_dim":

        visa_dim = read_staging_data(spark=spark, path=staging_path)

        visa_dim = clean_other_dims(visa_dim)

        write_to_s3(
            df=visa_dim,
            partition_col="lookup_value",
            num_partitions=1,
            out_path=adm_path,
        )

    elif task == "create_port_dim":

        port_dim = read_staging_data(spark=spark, path=staging_path)

        port_dim = clean_port_map_dim(port_dim)

        write_to_s3(
            df=port_dim, partition_col="port", num_partitions=1, out_path=adm_path
        )

    spark.stop()
