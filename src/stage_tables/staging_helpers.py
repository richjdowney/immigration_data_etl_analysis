from utils.logging_framework import log
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


def read_input_data(spark, input_data_path, source):
    """Function to read the input data into Spark

    Parameters
    ----------
    spark : SparkSession
        current Spark session
    input_data_path : str
        path to the input data on s3
    source : str
        name of the source file

    """

    if source == "sas_data":
        log.info("Reading immigration data")
        df = spark.read.parquet("{}{}".format(input_data_path, source))

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
        Descrription of the DataFrame to print to the output

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
        f.name
        for f in df.schema.fields
        if isinstance(f.dataType, StringType) == False
    ]
    for col in num_cols:
        df.agg(
            F.mean(col).alias("mean_" + col),
            F.max(col).alias("max_" + col),
            F.min(col).alias("min_" + col),
            F.stddev(col).alias("std_" + col),
        ).show()
