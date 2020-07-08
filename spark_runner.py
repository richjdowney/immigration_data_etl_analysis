import sys
from src.stage_tables.staging_helpers import read_input_data, profile_data
from pyspark.sql import SparkSession

if __name__ == "__main__":

    spark = SparkSession.builder.appName("Immigration_ETL").getOrCreate()

    task = sys.argv[1]
    input_path = sys.argv[2]
    input_file = sys.argv[3]

    if task == "stage_immigration":

        staging_immigration_data = read_input_data(spark=spark,
                                                   input_data_path=input_path,
                                                   source=input_file)

        profile_data(staging_immigration_data, "Immigration Data")

    spark.stop()
