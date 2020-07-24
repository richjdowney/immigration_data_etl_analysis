from utils.aws_utils import load_file_to_s3
from utils.logging_framework import log


def copy_app_to_s3(*op_args):
    """Runner to copy application files to s3

    Parameters
    ----------
    op_args : dict
        config dictionary

    """
    config = op_args[0]

    # Upload egg file to s3
    load_file_to_s3(
        file_name="{}{}".format(config["app"]["PathToEgg"], config["app"]["EggObject"]),
        bucket=config["s3"]["Bucket"],
        aws_credentials_id=config["airflow"]["AwsCredentials"],
        object_name="application/{}".format(config["app"]["EggObject"]),
    )

    # Upload main class file to s3
    load_file_to_s3(
        file_name="{}{}".format(config["app"]["RootPath"], config["app"]["RunnerObject"]),
        bucket=config["s3"]["Bucket"],
        aws_credentials_id=config["airflow"]["AwsCredentials"],
        object_name="application/{}".format(config["app"]["RunnerObject"]),
    )

    # Upload requirements
    load_file_to_s3(
        file_name="{}{}".format(config["app"]["RootPath"], config["app"]["Requirements"]),
        bucket=config["s3"]["Bucket"],
        aws_credentials_id=config["airflow"]["AwsCredentials"],
        object_name="bootstrap/{}".format(config["app"]["Requirements"]),
    )

    # Upload bootstrap shell script for dependencies
    load_file_to_s3(
        file_name="{}{}".format(config["app"]["PathToBin"], config["app"]["DependenciesShell"]),
        bucket=config["s3"]["Bucket"],
        aws_credentials_id=config["airflow"]["AwsCredentials"],
        object_name="bootstrap/{}".format(config["app"]["DependenciesShell"]),
    )

    return log.info("Uploaded application to s3 succesfully!")
