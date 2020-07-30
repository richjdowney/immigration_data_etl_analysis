import sys
from src.data_quality_checks.data_quality_check_helpers import *
from utils.data_processing import *
from pyspark.sql import SparkSession
from utils.logging_framework import log

if __name__ == "__main__":

    spark = SparkSession.builder.appName("Immigration_ETL").getOrCreate()

    task = sys.argv[1]
    adm_path = sys.argv[5]
    execution_date = sys.argv[6]

    log.info("Running Spark job for task {}".format(task))
    log.info("Reading data for task {} from {}".format(task, adm_path))
    log.info("Executing DAG on {}".format(execution_date))

    if task == "fact_immigration_qc":

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
                    "StringType",
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
                    "StringType",
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
