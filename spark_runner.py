import sys
from src.stage_tables.staging_helpers import read_input_data, profile_data
from src.create_facts.create_facts_helpers import *
from src.data_quality_checks.data_quality_check_helpers import *
from src.create_dims.create_dims_helpers import *
from utils.data_processing import *
from pyspark.sql import SparkSession
from utils.logging_framework import log

if __name__ == "__main__":

    spark = SparkSession.builder.appName("Immigration_ETL").getOrCreate()

    log.info("{}".format(sys.argv[1]))
    log.info("{}".format(sys.argv[2]))
    log.info("{}".format(sys.argv[3]))
    log.info("{}".format(sys.argv[4]))
    log.info("{}".format(sys.argv[5]))

    task = sys.argv[1]
    input_path = sys.argv[2]
    input_file = sys.argv[3]
    staging_path = sys.argv[4]
    adm_path = sys.argv[5]

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

    elif task == "create_fact_table":

        immigration_fact = load_parquet(spark=spark, path=staging_path)

        immigration_fact = clean_immigration(immigration_fact)

        write_to_s3(
            df=immigration_fact,
            partition_col="date_file_added",
            num_partitions=20,
            out_path=adm_path,
        )

    elif task == "create_dim_airport_codes":

        airport_codes_dim = load_parquet(spark=spark, path=staging_path)

        airport_codes_dim = clean_airport_codes_dim(airport_codes_dim)

        write_to_s3(
            df=airport_codes_dim,
            partition_col="ident",
            num_partitions=20,
            out_path=adm_path,
        )

    elif task == "create_dim_city_demos":

        city_demos_dim = load_parquet(spark=spark, path=staging_path)

        city_demos_dim = clean_cities_dim(city_demos_dim)

        write_to_s3(
            df=city_demos_dim,
            partition_col="city",
            num_partitions=20,
            out_path=adm_path,
        )

    elif task == "create_dim_city_res":

        city_res_dim = load_parquet(spark=spark, path=staging_path)

        city_res_dim = clean_other_dims(city_res_dim)
        city_res_dim = city_res_dim.withColumnRenamed(
            "lookup_value", "city_res"
        ).withColumnRenamed("map", "city_res_desc")

        write_to_s3(
            df=city_res_dim,
            partition_col="city_res",
            num_partitions=1,
            out_path=adm_path,
        )

    elif task == "create_mode_dim":

        mode_dim = load_parquet(spark=spark, path=staging_path)

        mode_dim = clean_other_dims(mode_dim)
        mode_dim = mode_dim.withColumnRenamed(
            "lookup_value", "arrival_mode"
        ).withColumnRenamed("map", "arrival_mode_desc")

        write_to_s3(
            df=mode_dim,
            partition_col="arrival_mode",
            num_partitions=1,
            out_path=adm_path,
        )

    elif task == "create_state_dim":

        state_dim = load_parquet(spark=spark, path=staging_path)

        state_dim = clean_other_dims(state_dim)
        state_dim = state_dim.withColumnRenamed(
            "lookup_value", "us_address_state"
        ).withColumnRenamed("map", "state_desc")

        write_to_s3(
            df=state_dim,
            partition_col="us_address_state",
            num_partitions=1,
            out_path=adm_path,
        )

    elif task == "create_visa_dim":

        visa_dim = load_parquet(spark=spark, path=staging_path)

        visa_dim = clean_other_dims(visa_dim)
        visa_dim = visa_dim.withColumnRenamed(
            "lookup_value", "visa_code"
        ).withColumnRenamed("map", "visa_desc")

        write_to_s3(
            df=visa_dim, partition_col="visa_code", num_partitions=1, out_path=adm_path
        )

    elif task == "create_port_dim":

        port_dim = load_parquet(spark=spark, path=staging_path)

        port_dim = clean_port_map_dim(port_dim)

        write_to_s3(
            df=port_dim, partition_col="port", num_partitions=1, out_path=adm_path
        )

    elif task == "fact_immigration_qc":

        # Get data from ADM
        fact_immigration = load_parquet(spark=spark, path=adm_path)

        # Check for missing values
        data_quality_check_missing(fact_immigration, "fact_immigration")

        # Check number of columns
        data_quality_check_num_cols(fact_immigration, "fact_immigration", 24)

        # Check data types
        expected_dtype_fact_immigration = dict(
            zip(
                [
                    "ID",
                    "year",
                    "month",
                    "city",
                    "res",
                    "port",
                    "arrival_date",
                    "arrival_mode",
                    "us_address_state",
                    "departure_date",
                    "age",
                    "visa_code",
                    "date_file_added",
                    "loc_visa_issued",
                    "arrival_flag",
                    "departure_flag",
                    "match_flag",
                    "birth_year",
                    "date_admitted_to",
                    "admission_number",
                    "flight_number",
                    "visa_type",
                    "gender",
                    "airline",
                ],
                [
                    "IntegerType",
                    "IntegerType",
                    "IntegerType",
                    "IntegerType",
                    "IntegerType",
                    "StringType",
                    "DateType",
                    "IntegerType",
                    "StringType",
                    "StringType",
                    "IntegerType",
                    "IntegerType",
                    "StringType",
                    "StringType",
                    "StringType",
                    "StringType",
                    "StringType",
                    "IntegerType",
                    "StringType",
                    "IntegerType",
                    "StringType",
                    "StringType",
                    "StringType",
                    "StringType",
                ],
            )
        )

        data_quality_check_dtypes(
            fact_immigration, "fact_immigration", expected_dtype_fact_immigration
        )

    elif task == "dim_visa_qc":

        # Get data from ADM
        dim_visa = load_parquet(spark=spark, path=adm_path)

        # Check for missing values
        data_quality_check_missing(dim_visa, "dim_visa")

        # Check number of columns
        data_quality_check_num_cols(dim_visa, "dim_visa", 2)

        # Check data types
        expected_dtype_dim_visa = dict(
            zip(["visa_code", "visa_desc"], ["StringType", "StringType"])
        )

        data_quality_check_dtypes(dim_visa, "dim_visa", expected_dtype_dim_visa)

    elif task == "dim_state_qc":

        # Get data from ADM
        dim_state = load_parquet(spark=spark, path=adm_path)

        # Check for missing values
        data_quality_check_missing(dim_state, "dim_state")

        # Check number of columns
        data_quality_check_num_cols(dim_state, "dim_state", 2)

        # Check data types
        expected_dtype_dim_state = dict(
            zip(["us_address_state", "state_desc"], ["StringType", "StringType"])
        )

        data_quality_check_dtypes(dim_state, "dim_state", expected_dtype_dim_state)

    elif task == "dim_cit_res_qc":

        # Get data from ADM
        dim_cit_res = load_parquet(spark=spark, path=adm_path)

        # Check for missing values
        data_quality_check_missing(dim_cit_res, "dim_cit_res")

        # Check number of columns
        data_quality_check_num_cols(dim_cit_res, "dim_cit_res", 2)

        # Check data types
        expected_dtype_dim_cit_res = dict(
            zip(["city_res", "city_res_desc"], ["StringType", "StringType"])
        )

        data_quality_check_dtypes(
            dim_cit_res, "dim_cit_res", expected_dtype_dim_cit_res
        )

    elif task == "dim_airport_codes_qc":

        # Get data from ADM
        dim_airport_codes = load_parquet(spark=spark, path=adm_path)

        # Check for missing values
        data_quality_check_missing(dim_airport_codes, "dim_airport_codes")

        # Check number of columns
        data_quality_check_num_cols(dim_airport_codes, "dim_airport_codes", 13)

        # Check data types
        expected_dtype_dim_airport_codes = dict(
            zip(
                [
                    "ident",
                    "type",
                    "name",
                    "elevation_ft",
                    "continent",
                    "iso_country",
                    "iso_region",
                    "municipality",
                    "gps_code",
                    "iata_code",
                    "local_code",
                    "lon",
                    "lat",
                ],
                [
                    "StringType",
                    "StringType",
                    "StringType",
                    "IntegerType",
                    "StringType",
                    "StringType",
                    "StringType",
                    "StringType",
                    "StringType",
                    "StringType",
                    "StringType",
                    "StringType",
                    "StringType",
                ],
            )
        )

        data_quality_check_dtypes(
            dim_airport_codes, "dim_airport_codes", expected_dtype_dim_airport_codes
        )

    elif task == "dim_port_qc":

        # Get data from ADM
        dim_port = load_parquet(spark=spark, path=adm_path)

        # Check for missing values
        data_quality_check_missing(dim_port, "dim_port")

        # Check number of columns
        data_quality_check_num_cols(dim_port, "dim_port", 3)

        # Check data types
        expected_dtype_dim_port = dict(
            zip(["port", "city", "state"], ["StringType", "StringType", "StringType"])
        )

        data_quality_check_dtypes(dim_port, "dim_port", expected_dtype_dim_port)

    elif task == "dim_city_demos_qc":

        # Get data from ADM
        dim_city_demos = load_parquet(spark=spark, path=adm_path)

        # Check for missing values
        data_quality_check_missing(dim_city_demos, "dim_city_demos")

        # Check number of columns
        data_quality_check_num_cols(dim_city_demos, "dim_city_demos", 11)

        # Check data types
        expected_dtype_dim_city_demos = dict(
            zip(
                [
                    "city",
                    "state",
                    "median_age",
                    "male_population",
                    "female_population",
                    "total_population",
                    "num_veterans",
                    "num_foreign_born",
                    "avg_hhd_size",
                    "state_code",
                    "race",
                ],
                [
                    "StringType",
                    "StringType",
                    "FloatType",
                    "IntegerType",
                    "IntegerType",
                    "IntegerType",
                    "IntegerType",
                    "IntegerType",
                    "FloatType",
                    "StringType",
                    "StringType",
                ],
            )
        )

        data_quality_check_dtypes(
            dim_city_demos, "dim_city_demos", expected_dtype_dim_city_demos
        )

    elif task == "dim_mode_qc":

        # Get data from ADM
        dim_mode = load_parquet(spark=spark, path=adm_path)

        # Check for missing values
        data_quality_check_missing(dim_mode, "dim_mode")

        # Check number of columns
        data_quality_check_num_cols(dim_mode, "dim_mode", 2)

        # Check data types
        expected_dtype_dim_mode = dict(
            zip(["arrival_mode", "arrival_mode_desc"], ["StringType", "StringType"])
        )

        data_quality_check_dtypes(dim_mode, "dim_port", expected_dtype_dim_mode)

    spark.stop()
