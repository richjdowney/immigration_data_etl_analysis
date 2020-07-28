from utils.data_processing import *
from pyspark.sql import functions as F
from pyspark.sql import DataFrame as SparkDataFrame


def clean_cities_dim(df: SparkDataFrame) -> SparkDataFrame:
    """Function to clean the cities dimension table, specifically:

        *  Rename columns
        *  Cast columns to appropriate data types
        *  Treat missing value
        *  Drop columns not required

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Spark DataFrame to clean

    Returns
    ----------
    df : pyspark.sql.DataFrame
        Spark DataFrame cleaned
    """

    # Drop the count variable
    df = df.drop("count")

    # Rename columns
    mapping = dict(
        zip(
            [
                "City",
                "State",
                "Median_Age",
                "Male_Population",
                "Female_Population",
                "Total_Population",
                "Number_of_Veterans",
                "Foreign-born",
                "Average_Household_Size",
                "State_Code",
                "Race",
            ],
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
        )
    )

    df = rename_cols(df, mapping)

    # Cast columns to apropriate values
    df = cast_vars(df, ["median_age", "avg_hhd_size"], "float")

    cols_to_cast_int = [
        "male_population",
        "female_population",
        "total_population",
        "num_veterans",
        "num_foreign_born",
    ]

    df = cast_vars(df, cols_to_cast_int, "int")

    # Replace missing - all numeric missing replace with 0
    cols_to_zero_replace = [
        "male_population",
        "female_population",
        "num_foreign_born",
        "avg_hhd_size",
        "num_veterans"
    ]

    for c in cols_to_zero_replace:
        df = df.withColumn(c, F.when(F.col(c).isNull(), F.lit(0)).otherwise(F.col(c)))

    return df


def clean_airport_codes_dim(df: SparkDataFrame) -> SparkDataFrame:
    """Function to clean the airport codes dimension table, specifically:

        *  Cast columns to appropriate data types
        *  Treat missing values
        *  Split the co-ordinates column into lattitude and longitude

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Spark DataFrame to clean

    Returns
    ----------
    df : pyspark.sql.DataFrame
        Spark DataFrame cleaned
    """

    # Cast columns to appropriate values
    df = cast_vars(df, ["elevation_ft"], "int")

    # Replace missing
    df = airport_treat_missing(df)

    # Split the 'coordinates' column into lat and lon
    split_col = F.split(F.col("coordinates"), ",")
    df = df.withColumn("lon", split_col.getItem(0))
    df = df.withColumn("lat", split_col.getItem(1))

    df = df.drop("coordinates")

    return df


def clean_port_map_dim(df: SparkDataFrame) -> SparkDataFrame:
    """Function to clean the port mapping dimension table, specifically:

        *  Remove quotes from string columns
        *  Split the city and state column into 2 separate columns

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Spark DataFrame to clean

    Returns
    ----------
    df : pyspark.sql.DataFrame
        Spark DataFrame cleaned
    """

    # Remove quotes
    df = df.withColumn("port", F.regexp_replace("port", "'", "")).withColumn(
        "city_state", F.regexp_replace("city_state", "'", "")
    )

    # Split city and state
    split_col = F.split(df["city_state"], ",")
    df = (
        df.withColumn("city", split_col.getItem(0))
        .withColumn("state", split_col.getItem(1))
        .withColumn("state", F.trim(F.col("state")))
    )

    # Drop unwanted columns
    df = df.drop("_c2", "city_state")

    df = df.withColumn('state', F.when(F.col('state').isNull(), 'XX').otherwise(F.col('state')))

    return df


def clean_other_dims(df: SparkDataFrame) -> SparkDataFrame:
    """Function to clean the 'other' dimension tables that have common cleaning steps
    (city_res mapping, mode mapping, state mapping and visa mapping, specifically:

        *  Split into separate columns
        *  Remove quotes from  strings

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Spark DataFrame to clean

    Returns
    ----------
    df : pyspark.sql.DataFrame
        Spark DataFrame cleaned

    """

    # Common cleaning tasks across mapping files
    df = clean_mapping(df)

    return df
