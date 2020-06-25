import sys
sys.path.append("/home/ubuntu/immigration_code/")
from utils.aws_utils import load_file_to_s3
from utils.general_utils import create_egg


def copy_app_to_s3():
    """Runner to create egg file and copy application files to s3"""

    # Create egg file to package latest code
    create_egg("/home/ubuntu/immigration_code/src")

    # Upload egg file to s3
    load_file_to_s3(
        file_name="/home/ubuntu/immigration_code/src/dist/test_submit-0.1-py3.7.egg",
        bucket="immigration-data-etl",
        aws_credentials_id="aws_default",
        object_name="test_submit-0.1-py3.7.egg",
    )

    # Upload main class file to s3
    load_file_to_s3(
        file_name="/home/ubuntu/immigration_code/src/spark_runner.py",
        bucket="immigration-data-etl",
        aws_credentials_id="aws_default",
        object_name="spark_runner.py",
    )

    success = "Uploaded application to s3 successfully"

    return success


copy_app_to_s3()
