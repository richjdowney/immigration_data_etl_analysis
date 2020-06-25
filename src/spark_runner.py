import sys
from pyspark.sql import SparkSession
# NOTE:  The import statement has to be this way to work when used in the spark submit call
# as the first level in the egg file is not the same root source as the repo - it will
# show as errors in the IDE but it is OK
from test_module1.test_func import print_success1
from test_module2.test_func import print_success2


if __name__ == "__main__":

    spark = SparkSession\
            .builder\
            .appName("Immigration data ETL")\
            .getOrCreate()

    task = sys.argv[1]

    if task == "module 1":
        print_success1()

    if task == "module 2":
        print_success2()

    spark.stop()
