from pyspark.sql import functions as F
from utils.logging_framework import log
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession


def clean_mapping(df: SparkDataFrame) -> SparkDataFrame:
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

    split_col = F.split(df["value"], "=")
    df = (
        df.withColumn("lookup_value", split_col.getItem(0))
        .withColumn("lookup_value", F.regexp_replace("lookup_value", "'", ""))
        .withColumn("map", split_col.getItem(1))
        .withColumn("map", F.regexp_replace("map", "'", ""))
        .drop("value")
    )

    return df


def rename_cols(df: SparkDataFrame, mapping: dict) -> SparkDataFrame:
    """Function to rename the multiple columns in a DataFrame:

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Spark DataFrame to clean
    mapping: dict
        Dictionary containing the rename mapping

    Returns
    ----------
    df : pyspark.sql.DataFrame
        Spark DataFrame with renamed columns
    mapping : dict
        Dictionary mapping of old column to new column

    """

    def getlist(dict: dict) -> SparkDataFrame:
        """ Function gets list of columns to rename from the mapping file

        Parameters
        ----------
        dict : dict
            dictionary mapping of columns to rename

        """
        list = []
        for key in dict.keys():
            list.append(key)

        return list

    cols_to_rename = getlist(mapping)
    other_cols = [i for i in df.columns if i not in cols_to_rename]

    df = df.select(
        [F.col(c).alias(mapping.get(c, c)) for c in cols_to_rename] + other_cols
    )

    return df


def immigration_treat_missing(df: SparkDataFrame) -> SparkDataFrame:
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
    df = df.withColumn("age", F.when(F.col("age").isNull(), 0).otherwise(F.col("age")))

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
        F.when(F.col("arrival_flag").isNull(), "X").otherwise(F.col("arrival_flag")),
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
        F.when(F.col("match_flag").isNull(), "X").otherwise(F.col("match_flag")),
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
        F.when(
            (F.col("gender").isNull()) | (F.col("gender").isin("X", "U")), "X"
        ).otherwise(F.col("gender")),
    )

    # Replace missing 'airline' with 'XX'
    df = df.withColumn(
        "airline", F.when(F.col("airline").isNull(), "XX").otherwise(F.col("airline"))
    )

    # Replace missing 'flight_number' with '00000'
    df = df.withColumn(
        "flight_number",
        F.when(F.col("flight_number").isNull(), "00000").otherwise(
            F.col("flight_number")
        ),
    )

    # Replace missing city with X
    df = df.withColumn(
        "city",
        F.when(F.col("city").isNull(), "X").otherwise(
            F.col("city")
        ),
    )

    # Replace missing 'birth_year' with 9999
    df = df.withColumn(
        "birth_year",
        F.when(F.col("birth_year").isNull(), "9999").otherwise(
            F.col("birth_year")
        ),
    )

    return df


def airport_treat_missing(df: SparkDataFrame) -> SparkDataFrame:
    """Function to treat missing data in the airport data:

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Spark DataFrame to clean

    Returns
    ----------
    df : pyspark.sql.DataFrame
        Spark DataFrame with missing values treated

    """

    df = df.withColumn(
        "elevation_ft",
        F.when(F.col("elevation_ft").isNull(), F.lit(0)).otherwise(
            F.col("elevation_ft")
        ),
    )

    df = df.withColumn(
        "municipality",
        F.when(F.col("municipality").isNull(), "Unknown").otherwise(
            F.col("municipality")
        ),
    )

    df = df.withColumn(
        "gps_code",
        F.when(F.col("gps_code").isNull(), "XXXX").otherwise(F.col("gps_code")),
    )

    df = df.withColumn(
        "iata_code",
        F.when(
            (F.col("iata_code").isNull()) | (F.col("iata_code") == "0"), "XXX"
        ).otherwise(F.col("iata_code")),
    )

    df = df.withColumn(
        "local_code",
        F.when(F.col("local_code").isNull(), "XXX").otherwise(F.col("local_code")),
    )

    return df


def convert_sas_dates(df: SparkDataFrame, cols: list) -> SparkDataFrame:
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
        df = df.withColumn(
            "{}".format(c), F.expr("date_add(to_date('1960-01-01'), {})".format(c))
        )

    return df


def cast_vars(df: SparkDataFrame, cols: list, dtype: str) -> SparkDataFrame:
    """Function to cast columns to a new data type:

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Spark DataFrame to clean
    cols : list
        List of columns to cast
    dtype : str
        String containing the data type to cast to

    Returns
    ----------
    df : pyspark.sql.DataFrame
        Spark DataFrame with cast columns

    """

    for c in cols:
        df = df.withColumn(c, F.col(c).cast("{}".format(dtype)))

    return df


def convert_dates(df: SparkDataFrame, col: str, dt_format: str) -> SparkDataFrame:
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


def write_to_s3(df: SparkDataFrame, partition_col: str, num_partitions: int, out_path: str) -> None:
    """Function to write DataFrames to location on s3

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

    log.info("Writing data to {}".format(out_path))
    df.repartition(num_partitions, partition_col)
    df.write.parquet(out_path, mode="overwrite")


def read_staging_data(spark: SparkSession, path: str) -> SparkDataFrame:
    """
    Function to read data from staging

    Parameters
    ----------
    spark : SparkSession
        Current Spark session
    path : str
        Path to staging data on s3

    Returns
    --------
    df : pyspark.sql.DataFrame
        Spark DataFrame containing the staging data

    """

    log.info("Reading staging data from {}".format(path))
    df = spark.read.parquet(path)

    return df


def load_parquet(spark: SparkSession, path: str) -> SparkDataFrame:
    """
    Function to load parquet

    Parameters
    ----------
    spark : SparkSession
        Current Spark session
    path : str
        Path to staging data on s3

    Returns
    --------
    df : pyspark.sql.DataFrame
        Spark DataFrame containing the staging data

    """

    log.info("Reading parquet from {}".format(path))
    df = spark.read.parquet(path)

    return df
