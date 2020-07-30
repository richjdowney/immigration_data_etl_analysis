from utils.data_processing import *
from pyspark.sql import DataFrame as SparkDataFrame
from utils.logging_framework import log


def clean_immigration(df: SparkDataFrame) -> SparkDataFrame:
    """Function to clean the immigration data, specifically:

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

    # Drop the following columns:
    # occup - it is 99% missing
    # count - not clear what this is "used for summary statistics"
    # entdepu - is 99% missing
    # insnum - is 99% missing

    log.info("Checking the immigration fact table")

    df = df.drop("count", "occup", "count", "entdepu", "insnum")

    # Rename columns
    mapping = dict(
        zip(
            [
                "cicid",
                "i94yr",
                "i94mon",
                "i94city",
                "i94res",
                "i94port",
                "arrdate",
                "i94mode",
                "i94addr",
                "depdate",
                "i94bir",
                "i94visa",
                "dtadfile",
                "visapost",
                "entdepa",
                "entdepd",
                "matflag",
                "biryear",
                "dtaddto",
                "admnum",
                "fltno",
                "visatype",
            ],
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
            ],
        )
    )

    df = rename_cols(df, mapping)

    # Cast columns to apropriate values
    cols_to_cast_int = [
        "ID",
        "year",
        "month",
        "city",
        "res",
        "arrival_mode",
        "age",
        "visa_code",
        "birth_year",
        "admission_number",
    ]

    df = cast_vars(df, cols_to_cast_int, "int")

    # Convert SAS dates
    df = convert_sas_dates(df, ["arrival_date", "departure_date"])

    # Convert other integer dates
    df = convert_dates(df, "date_file_added", "yyyyMMdd")
    df = convert_dates(df, "date_admitted_to", "ddMMyyyy")

    # Treat missing values
    df = immigration_treat_missing(df)

    return df
