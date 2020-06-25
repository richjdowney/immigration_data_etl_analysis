import boto3
from airflow.contrib.hooks.aws_hook import AwsHook


def load_file_to_s3(file_name, bucket, aws_credentials_id, object_name=None):
    """Function to upload files to s3 using Boto

    Parameters
    ----------
    file_name : str
        string containing path to file
    bucket : str
        string containing name of the s3 bucket
    aws_credentials_id : str
        name of the Airflow connection holding the AWS credentials
    object_name : str
        name of the object to upload
    """

    aws_hook = AwsHook(aws_credentials_id)
    credentials = aws_hook.get_credentials()

    s3 = boto3.resource(
        "s3",
        aws_access_key_id=credentials.access_key,
        aws_secret_access_key=credentials.secret_key,
    )

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    s3.Bucket(bucket).Object(object_name).upload_file(file_name)
