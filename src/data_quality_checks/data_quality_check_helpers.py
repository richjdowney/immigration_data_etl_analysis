from pyspark.sql import functions as F
from pyspark.sql import DataFrame as SparkDataFrame
from utils.logging_framework import log


def data_quality_check_missing(df: SparkDataFrame, df_name: str) -> None:
    """Function to check DataFrame columns for missing values

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Spark DataFrame to write
    df_name : str
        Name of the DataFrame being checked

    Raises
    ------
    ValueError
        Raises Value Error if any columns have missing values
    """

    log.info("Checking for missing values in DataFrame {}".format(df_name))

    # Check file for missing records
    null_missing_counts = df.select(
        [F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]
    )
    null_missing_counts.show()

    for col in null_missing_counts.columns:
        if null_missing_counts.select(col).collect()[0][0] > 0:
            raise ValueError(
                "Data quality check failed, column {} in DataFrame {} "
                "has missing values".format(col, df_name)
            )

    log.info("No missing values found in DataFrame {}".format(df_name))


def data_quality_check_empty_frame(df: SparkDataFrame, df_name: str) -> None:
    """Function to check if DataFrame is empty

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Spark DataFrame to write
    df_name : str
        Name of the DataFrame being checked

    Raises
    ------
    ValueError
        Raises Value Error if DataFrame is empty
    """

    log.info("Checking for records in DataFrame {}".format(df_name))

    if df.count() == 0:
        raise ValueError("DataFrame {} is empty".format(df_name))

    log.info("DataFrame {} contains {} rows and {} columns".format(df_name, df.count(), len(df.columns)))


def data_quality_check_num_cols(df, df_name, exp_value):
    """Function to compare the number of DataFrame columns to the expected

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Spark DataFrame to write
    df_name : str
        Name of the DataFrame being checked
    exp_value : int
        Number of expected columns

    Raises
    ------
    ValueError
        Raises Value Error number of DataFrame columns does not match expected
    """

    log.info("Checking DataFrame {} contains {} columns".format(df_name, exp_value))

    num_cols = len(df.columns)
    if num_cols != exp_value:
        raise ValueError(
            "Data quality check failed, DataFrame {} has {} columns expected {}".format(
                df_name, num_cols, exp_value
            )
        )

    log.info("DataFrame {} contains {} columns as expected".format(df_name, exp_value))


def data_quality_check_dtypes(df: SparkDataFrame, df_name: str, expected_type_map: dict):
    """Function to compare the number of DataFrame columns to the expected

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Spark DataFrame to write
    df_name : str
        Name of the DataFrame being checked
    expected_type_map : dict
        Dictionary containing column names and expected types

    Raises
    ------
    ValueError
        Raises Value Error if data types do not match expected
    """
    log.info("Checking data types match expected types for DataFrame {} ".format(df_name))

    for col in df.columns:
        actual_dtype = str(df.schema[col].dataType)
        expected_dtype = expected_type_map[col]

        if expected_dtype != actual_dtype:
            raise ValueError(
                "Data quality check failed, DataFrame {} and column {} has data type {} expecting {}".format(
                    df_name, col, actual_dtype, expected_dtype
                )
            )

    log.info("Data types in DataFrame {} match expected types".format(df_name))
