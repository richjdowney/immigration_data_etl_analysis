import sys
from src.create_fact_dims.create_facts_helpers import *
from src.create_fact_dims.create_dims_helpers import *
from utils.data_processing import *
from pyspark.sql import SparkSession
from utils.logging_framework import log

if __name__ == "__main__":

    spark = SparkSession.builder.appName("Immigration_ETL").getOrCreate()

    task = sys.argv[1]
    staging_path = sys.argv[4]
    adm_path = sys.argv[5]
    execution_date = sys.argv[6]

    log.info("Running Spark job for task {}".format(task))
    log.info("Reading staging tables from {}".format(staging_path))
    log.info("ADM path passed for task {} is {}".format(task, adm_path))
    log.info("Executing DAG on {}".format(execution_date))

    if task == "create_fact_table":

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

        city_res_dim = clean_other_dims(city_res_dim, "city_res_dim")
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

        mode_dim = clean_other_dims(mode_dim, "mode_dim")
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

        state_dim = clean_other_dims(state_dim, "state_dim")
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

        visa_dim = clean_other_dims(visa_dim, "visa_dim")
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

    spark.stop()
