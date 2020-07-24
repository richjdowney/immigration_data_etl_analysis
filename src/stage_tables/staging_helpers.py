from utils.logging_framework import log
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, StructField, StructType
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
    df = spark.sqlContext.createDataFrame(spark.emptyRDD(), schema=schema)

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

    elif task == "stage_cit_res_mapping":
        df = spark.read.load(
            "{}{}".format(input_data_path, source), header=True, format="csv", sep=","
        )

    elif task == "stage_mode_mapping":
        df = spark.read.load(
            "{}{}".format(input_data_path, source), header=False, format="csv", sep=","
        )
        df = df.withColumnRenamed('_c0', 'value')

    elif task == "stage_state_mapping":
        df = spark.read.load(
            "{}{}".format(input_data_path, source), header=False, format="csv", sep=","
        )
        df = df.withColumnRenamed('_c0', 'value')

    elif task == "stage_visa_mapping":
        df = spark.read.load(
            "{}{}".format(input_data_path, source), header=False, format="csv", sep=","
        )
        df = df.withColumnRenamed('_c0', 'value')

    elif task == "stage_port_mapping":
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


def _clean_mapping(df):
    """Function to split the column by "=" and remove single quotes.  Common cleaning
    steps required across the mapping files

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Spark DataFrame to clean

    Returns
    ----------
    df : pyspark.sql.DataFrame
        Spark DataFrame with cleaned mapping file

    """

    split_col = F.split(df['value'], '=')
    df = df.withColumn('lookup_value', split_col.getItem(0)) \
        .withColumn('lookup_value', F.regexp_replace('lookup_value', "'", '')) \
        .withColumn('map', split_col.getItem(1)) \
        .withColumn('map', F.regexp_replace('map', "'", '')) \
        .drop('value')

    return df


def _rename_cols(df, mapping):
    """Function to rename the multiple columns in a DataFrame:

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Spark DataFrame to clean

    Returns
    ----------
    df : pyspark.sql.DataFrame
        Spark DataFrame with renamed columns
    mapping : dict
        Dictionary mapping of old column to new column

    """

    def getlist(dict):
        """ Function gets list of columns to rename from the mapping file"""
        list = []
        for key in dict.keys():
            list.append(key)

        return list

    cols_to_rename = getlist(mapping)
    other_cols = [i for i in df.columns if i not in cols_to_rename]

    df = df.select([F.col(c).alias(mapping.get(c, c)) for c in cols_to_rename] + other_cols)

    return df


def _immigration_treat_missing(df):
    """Function to treat missing data in the immigration data:

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Spark DataFrame to clean

    Returns
    ----------
    df : pyspark.sql.DataFrame
        Spark DataFrame with missing values treated

    """

    # Remove records where 'arrival_mode' is null
    df = df.where(F.col("arrival_mode").isNotNull())

    # Replace 'us_address_state' missing values with XX
    df = df.withColumn(
        "us_address_state",
        F.when(F.col("us_address_state").isNull(), "XX").otherwise(
            F.col("us_address_state")
        ),
    )

    # Replace missing 'departure_date' with '01-01-1901'
    df = df.withColumn(
        "departure_date",
        F.when(F.col("departure_date").isNull(), "01-01-1901").otherwise(
            F.col("departure_date")
        ),
    )

    # Replace missing 'age' with 0
    df = df.withColumn(
        "age",
        F.when(F.col("age").isNull(), 0).otherwise(
            F.col("age")
        ),
    )

    # Replace missing 'date_file_added' with '01-01-1901'
    df = df.withColumn(
        "date_file_added",
        F.when(F.col("date_file_added").isNull(), "01-01-1901").otherwise(
            F.col("date_file_added")
        ),
    )

    # Replace missing 'loc_visa_issued' with 'XXX'
    df = df.withColumn(
        "loc_visa_issued",
        F.when(F.col("loc_visa_issued").isNull(), "XXX").otherwise(
            F.col("loc_visa_issued")
        ),
    )

    # Replace missing 'arrival_flag' with 'X'
    df = df.withColumn(
        "arrival_flag",
        F.when(F.col("arrival_flag").isNull(), "X").otherwise(
            F.col("arrival_flag")
        ),
    )

    # Replace missing 'departure_flag' with 'X'
    df = df.withColumn(
        "departure_flag",
        F.when(F.col("departure_flag").isNull(), "X").otherwise(
            F.col("departure_flag")
        ),
    )

    # Replace missing 'match_flag' with 'X'
    df = df.withColumn(
        "match_flag",
        F.when(F.col("match_flag").isNull(), "X").otherwise(
            F.col("match_flag")
        ),
    )

    # Replace missing 'date_admitted_to' with '01-01-1901'
    df = df.withColumn(
        "date_admitted_to",
        F.when(F.col("date_admitted_to").isNull(), "01-01-1901").otherwise(
            F.col("date_admitted_to")
        ),
    )

    # Replace missing, 'X', 'U' in 'gender' with 'X'
    df = df.withColumn(
        "gender",
        F.when((F.col("gender").isNull()) | (F.col("gender").isin("X", "U")), "X").otherwise(
            F.col("gender")
        ),
    )

    # Replace missing 'airline' with 'XX'
    df = df.withColumn(
        "airline",
        F.when(F.col("airline").isNull(), "XX").otherwise(
            F.col("airline")
        ),
    )

    # Replace missing 'flight_number' with '00000'
    df = df.withColumn(
        "flight_number",
        F.when(F.col("flight_number").isNull(), "00000").otherwise(
            F.col("flight_number")
        ),
    )

    return df


def _airport_treat_missing(df):
    """Function to tret missing data in the airport data:

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Spark DataFrame to clean

    Returns
    ----------
    df : pyspark.sql.DataFrame
        Spark DataFrame with missing values treated

    """

    df = df.withColumn("elevation_ft", F.when(F.col("elevation_ft").isNull(), F.lit(0))
                       .otherwise(F.col("elevation_ft")))

    df = df.withColumn("municipality", F.when(F.col("municipality").isNull(), "Unknown")
                       .otherwise(F.col("municipality")))

    df = df.withColumn("gps_code", F.when(F.col("gps_code").isNull(), "XXXX")
                       .otherwise(F.col("gps_code")))

    df = df.withColumn("iata_code", F.when((F.col("iata_code").isNull()) | (F.col("iata_code") == "0"), "XXX")
                       .otherwise(F.col("iata_code")))

    df = df.withColumn("local_code", F.when(F.col("local_code").isNull(), "XXX")
                       .otherwise(F.col("local_code")))

    return df


def _convert_sas_dates(df, cols):
    """Function to convert SAS date columns to date:

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Spark DataFrame to clean
    cols : list
        Columns to convert

    Returns
    ----------
    df : pyspark.sql.DataFrame
        Spark DataFrame with converted columns

    """

    for c in cols:
        df = df.withColumn("{}".format(c), F.expr("date_add(to_date('1960-01-01'), {})".format(c)))

    return df


def _cast_vars(df, cols, dtype):
    """Function to cast columns to a new data type:

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Spark DataFrame to clean
    cols : list
        List of columns to cast

    Returns
    ----------
    df : pyspark.sql.DataFrame
        Spark DataFrame with cast columns

    """

    for c in cols:
        df = df.withColumn(c, F.col(c).cast("{}".format(dtype)))

    return df


def _convert_dates(df, col, dt_format):
    """Function to cast columns to int:

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Spark DataFrame to clean
    col : str
        name of the column to convert
    dt_format : str
        date format to convert from e.g. 'yyyyMMdd'

    Returns
    ----------
    df : pyspark.sql.DataFrame
        Spark DataFrame with cast columns

    """

    df = df.withColumn(col, F.to_date(col, dt_format))

    return df


def clean_data(df, task):
    """Function to clean the data, specifically:

        *  Rename columns
        *  Cast columns to appropriate data types
        *  Treat missing value
        *  Drop columns not required

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Spark DataFrame to clean
    task : str
        task to run

    Returns
    ----------
    df_clean : pyspark.sql.DataFrame
        Spark DataFrame cleaned

    """

    if task == "stage_immigration":
        # Drop the following columns:
        # occup - it is 99% missing
        # count - not clear what this is "used for summary statistics"
        # entdepu - is 99% missing
        # insnum - is 99% missing

        df = df.drop("count", "occup", "count", "entdepu", "insnum")

        # Rename columns
        mapping = dict(zip(['cicid', "i94yr", "i94mon", "i94city", "i94res",
                            "i94port", "arrdate", "i94mode", "i94addr", "depdate",
                            "i94bir", "i94visa", "dtadfile", "visapost", "entdepa",
                            "entdepd", "matflag", "biryear", "dtaddto", "admnum", "fltno", "visatype"],
                           ["ID", "year", "month", "city", 'res', 'port', 'arrival_date',
                            'arrival_mode', 'us_address_state', 'departure_date', 'age',
                            'visa_code', 'date_file_added', 'loc_visa_issued', 'arrival_flag',
                            'departure_flag', 'match_flag', 'birth_year', 'date_admitted_to',
                            'admission_number', 'flight_number', 'visa_type']))

        df = _rename_cols(df, mapping)

        # Cast columns to apropriate values
        cols_to_cast_int = [
            "ID",
            "year",
            "month",
            "cit",
            "res",
            "arrival_mode",
            "age",
            "visa_code",
            "birth_year",
            "admission_number",
        ]

        df = _cast_vars(df, cols_to_cast_int, "int")

        # Convert SAS dates
        df = _convert_sas_dates(df, ["arrival_date", "departure_date"])

        # Convert other integer dates
        df = _convert_dates(df, "date_file_added", "yyyyMMdd")
        df = _convert_dates(df, "date_admitted_to", "ddMMyyyy")

        # Treat missing values
        df = _immigration_treat_missing(df)

    if task == "stage_cities":

        # Drop the count variable
        df = df.drop('count')

        # Rename columns
        mapping = dict(zip(['City', 'State', 'Median Age', 'Male Population', 'Female Population', 'Total Population',
                            'Number of Veterans', 'Foreign-born', 'Average Household Size', 'State Code', 'Race'],
                           ['city', 'state', 'median_age', 'male_population', 'female_population', 'total_population',
                            'num_veterans', 'num_foreign_born', 'avg_hhd_size', 'state_code', 'race']))

        df = _rename_cols(df, mapping)

        # Cast columns to apropriate values
        df = _cast_vars(df, ['median_age', 'avg_hhd_size'], "float")

        cols_to_cast_int = [
            "male_population",
            "female_population",
            "total_population",
            "num_veterans",
            "num_foreign_born"
        ]

        df = _cast_vars(df, cols_to_cast_int, "int")

        # Replace missing - all numeric missing replace with 0
        cols_to_zero_replace = ["male_population",
                                "female_population",
                                "num_foreign_born",
                                "avg_hhd_size"
                                ]

        for c in cols_to_zero_replace:
            df = df.withColumn(c, F.when(F.col(c).isNull(), F.lit(0)).otherwise(F.col(c)))

    if task == "stage_airport_codes":
        # Cast columns to appropriate values
        df = _cast_vars(df, ["elevation_ft"], "int")

        # Replace missing
        df = _airport_treat_missing(df)

        # Split the 'coordinates' column into lat and lon
        split_col = F.split(F.col("coordinates"), ',')
        df = df.withColumn("lon", split_col.getItem(0))
        df = df.withColumn("lat", split_col.getItem(1))

        df = df.drop('coordinates')

    if task in ["stage_cit_res_map", 'stage_mode_map', 'stage_state_map', 'stage_visa_map']:
        df = _clean_mapping(df)

    if task == 'stage_port_map':

        # Remove quotes
        df = df.withColumn('port', F.regexp_replace('port', "'", '')) \
            .withColumn('city_state', F.regexp_replace('city_state', "'", ''))

        # Split city and state
        split_col = F.split(df['city_state'], ',')
        df = df.withColumn('city', split_col.getItem(0)) \
            .withColumn('state', split_col.getItem(1)) \
            .withColumn('state', F.trim(F.col('state')))

        # Drop unwanted columns
        df = df.drop('_c2', 'city_state')

    return df


def write_to_staging(df, partition_col, num_partitions, out_path):
    """Function to write cleaned DataFrames to staging location on s3

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Spark DataFrame to write
    partition_col : str
        Name of the column to partition by
    num_partitions : int
        Number of partitions
    out_path : str
        Path to the location for staging tables
    """

    df.repartition(num_partitions, partition_col)
    df.write.parquet(out_path, mode='overwrite')
