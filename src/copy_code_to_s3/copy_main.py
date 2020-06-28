import sys
sys.path.append("/home/ubuntu/immigration_code/")
from utils.aws_utils import load_file_to_s3
from utils.general_utils import create_egg
from utils.load_config import load_yaml
from utils.logging_framework import log

# Make config available
config = load_yaml("/home/ubuntu/immigration_code/utils/config.yaml")


def copy_app_to_s3() -> str:
    """Runner to create egg file and copy application files to s3"""

    # Create egg file to package latest code
    create_egg(config["app"]["SrcPath"])

    # Upload egg file to s3
    load_file_to_s3(
        file_name="{}{}".format(config["app"]["PathToEgg"], config["app"]["EggObject"]),
        bucket=config["s3"]["Bucket"],
        aws_credentials_id=config["airflow"]["AwsCredentials"],
        object_name=config["app"]["EggObject"],
    )

    # Upload main class file to s3
    load_file_to_s3(
        file_name="{}{}".format(config["app"]["SrcPath"], config["app"]["RunnerObject"]),
        bucket=config["s3"]["Bucket"],
        aws_credentials_id=config["airflow"]["AwsCredentials"],
        object_name=config["app"]["RunnerObject"],
    )

    return log.info("Uploaded application to s3 succesfully!")


copy_app_to_s3()
