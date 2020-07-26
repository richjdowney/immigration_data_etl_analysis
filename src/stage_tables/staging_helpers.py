from utils.logging_framework import log
from pyspark.sql.types import StringType, IntegerType, StructField, StructType
from utils.data_processing import *
import boto3


def _create_immigration_df(spark):
    """Function to specify the schema for immigration data and
       return an empty DataFrame
    """

    # Create the table for holding the immigration data
    field = [StructField("cicid", IntegerType(), True),
             StructField("i94yr", IntegerType(), True),
             StructField("i94mon", IntegerType(), True),
             StructField("i94city", IntegerType(), True),
             StructField("i94res", IntegerType(), True),
             StructField("i94port", StringType(), True),
             StructField("arrdate", IntegerType(), True),
             StructField("i94mode", IntegerType(), True),
             StructField("i94addr", StringType(), True),
             StructField("depdate", StringType(), True),
             StructField("i94bir", StringType(), True),
             StructField("i94visa", StringType(), True),
             StructField("count", StringType(), True),
             StructField("dtadfile", IntegerType(), True),
             StructField("visapost", StringType(), True),
             StructField("occup", StringType(), True),
             StructField("entdepa", StringType(), True),
             StructField("entdepd", StringType(), True),
             StructField("entdepu", StringType(), True),
             StructField("matflag", StringType(), True),
             StructField("biryear", IntegerType(), True),
             StructField("dtaddto", IntegerType(), True),
             StructField("gender", StringType(), True),
             StructField("insnum", IntegerType(), True),
             StructField("airline", StringType(), True),
             StructField("admnum", IntegerType(), True),
             StructField("fltno", IntegerType(), True),
             StructField("visatype", StringType(), True)
             ]

    schema = StructType(field)
    df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema=schema)

    return df


def read_input_data(spark, input_data_path, source, task):
    """Function to read the input data into Spark

    Parameters
    ----------
    spark : SparkSession
        current Spark session
    input_data_path : str
        path to the input data on s3
    source : str
        name of the source file
    task : str
        name of the task

    """
    log.info("Reading source data from {}{}".format(input_data_path, source))

    if task == "stage_immigration":
        log.info("Reading immigration data")

        df = _create_immigration_df(spark)

        # List folders in the immigration_data folder on s3 that contain the parquets
        s3_client = boto3.client("s3")

        def list_folders(s3_client, bucket_name):
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=source, Delimiter='/')
            for content in response.get('CommonPrefixes', ["immigration_data"]):
                yield content.get('Prefix')

        folder_list = list_folders(s3_client, "immigration-data-etl")

        # Read the parquets
        for folder in folder_list:
            print('Folder found: %s' % folder)

            df_in = spark.read.parquet("{}{}".format(input_data_path, folder))

            # NOTE:  The June table has extra columns that need to be dropped so it
            # has to be treated separately
            if folder == "data-inputs/immigration_data/immigration_data_jun16/":
                df_in = df_in.drop('validres', 'delete_days', 'delete_mexl', 'delete_dup', 'delete_visa',
                                   'delete_recdup')

            df = df.unionAll(df_in)

    elif task == "stage_airport_codes":
        log.info("Reading airport codes data")
        df = spark.read.csv("{}{}".format(input_data_path, source), header=True)

    elif task == "stage_cities":
        log.info("Reading cities data")
        df = spark.read.load(
            "{}{}".format(input_data_path, source), header=True, format="csv", sep=";"
        )

    elif task == "stage_cit_res_map":
        df = spark.read.load(
            "{}{}".format(input_data_path, source), header=True, format="csv", sep=","
        )

    elif task == "stage_mode_map":
        df = spark.read.load(
            "{}{}".format(input_data_path, source), header=False, format="csv", sep=","
        )
        df = df.withColumnRenamed('_c0', 'value')

    elif task == "stage_state_map":
        df = spark.read.load(
            "{}{}".format(input_data_path, source), header=False, format="csv", sep=","
        )
        df = df.withColumnRenamed('_c0', 'value')

    elif task == "stage_visa_map":
        df = spark.read.load(
            "{}{}".format(input_data_path, source), header=False, format="csv", sep=","
        )
        df = df.withColumnRenamed('_c0', 'value')

    elif task == "stage_port_map":
        df = spark.read.load(
            "{}{}".format(input_data_path, source), header=True, format="csv", sep="\t"
        )

    return df


def profile_data(df, df_desc):
    """Function to profile a Spark DataFrame printing the following to the output:

        *  Top 10 records
        *  DataFrame schema
        *  Count of rows and columns
        *  Count of duplicates across the entire DataFrame
        *  % of missing values per column
        *  Top 20 unique values per string column
        *  Min, Max, Mean, Standard Deviation for all numeric columns

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Spark DataFrame to profile
    df_desc : str
        Description of the DataFrame to print to the output

    """

    print("########## Profiling DataFrame {} ##########".format(df_desc))

    # Show the top 10 records
    print("Top 10 records from DataFrame {}".format(df_desc))
    df.show(10)

    # Show the schema
    print("Schema for DataFrame {}".format(df_desc))
    df.printSchema()

    # Get count of rows and columns
    print(
        "DataFrame {} has {} number of columns and {} rows".format(
            df_desc, len(df.columns), df.count()
        )
    )

    # Check the entire DataFrame for duplicates across all rows
    print("Count of rows in DataFrame {}: {}".format(df_desc, df.count()))
    print("Count of distinct rows in DataFrame {}: {}".format(df_desc, df.count()))

    # Get the percentage of missing values per column
    print("% of missing values per column in DataFrame {}".format(df_desc))
    df.select(
        [
            (F.count(F.when(F.isnan(c) | F.col(c).isNull(), c)) / F.count("*")).alias(c)
            for c in df.columns
        ]
    ).show()

    # Show top 20 unique values for each string variable
    print("Top 20 unique values for string variables in DataFrame {}".format(df_desc))
    str_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for col in str_cols:
        df.groupBy(col).count().orderBy(F.col("count").desc()).show(20)

    # Mean, min, max and standard deviation
    print(
        "Mean, min, max and standard deviation for variables in DataFrame {}".format(
            df_desc
        )
    )
    num_cols = [
        f.name for f in df.schema.fields if isinstance(f.dataType, StringType) == False
    ]
    for col in num_cols:
        df.agg(
            F.mean(col).alias("mean_" + col),
            F.max(col).alias("max_" + col),
            F.min(col).alias("min_" + col),
            F.stddev(col).alias("std_" + col),
        ).show()


