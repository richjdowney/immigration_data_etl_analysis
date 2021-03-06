import yaml
from typing import Any, Dict
from utils.logging_framework import log
import pydantic


class ConfigDefaultArgs(pydantic.BaseModel):
    """Configuration for the default args when setting up the DAG"""

    owner: str
    start_date: str
    end_date: str
    depends_on_past: bool
    retries: int
    catchup: bool
    email: str
    email_on_failure: bool
    email_on_retry: bool


class ConfigDag(pydantic.BaseModel):
    """Configuration for the DAG runs"""

    # Name for the DAG run
    dag_id: str

    # Default args for DAG run e.g. owner, start_date, end_date
    default_args: ConfigDefaultArgs

    # DAG schedule interval
    schedule_interval: str


class ConfigEmr(pydantic.BaseModel):
    """Configuration for EMR clusters"""

    Instances: Dict[str, Any]

    # EMR ec2 role
    JobFlowRole: str

    # EMR role
    ServiceRole: str

    # Cluster name
    Name: str

    # Path to save logs
    LogUri: str

    # EMR version
    ReleaseLabel: str

    # Cluster configurations

    Configurations: Dict[str, Any]

    # Path to dependencies shell script on s3
    BootstrapActions: Dict[str, Any]

    # Number of steps EMR can run concurrently
    StepConcurrencyLevel: int


class ConfigS3(pydantic.BaseModel):
    """Configuration for s3"""

    # Name of s3 bucket
    Bucket: str

    # Paths to runner files on s3(egg app, runners)
    egg: str
    StageRunner: str
    FactDimRunner: str
    DataQualityRunner: str


class ConfigApp(pydantic.BaseModel):
    """Configuration for application paths"""

    # Path to the root directory on EC2
    RootPath: str

    # Path to the runner files
    PathToRunners: str

    # Path to the bin directory on EC2
    PathToBin: str

    # Path to the egg file on EC2
    PathToEgg: str

    # Path to the utils directory on EC2
    PathToUtils: str

    # Name of the main application egg object
    EggObject: str

    # Name of Spark runner to stage tables
    StageRunner: str

    # Name of Spark runner to create fact and dim tables
    FactDimRunner: str

    # Name of Spark runner for data quality checks
    DataQualityRunner: str

    # Name of the shell script for bootstrapping
    DependenciesShell: str

    # Name of the package requirements
    Requirements: str


class ConfigAirflow(pydantic.BaseModel):
    """ Configuration for credentials used within Airflow"""

    # Name of the variable used to hold the AWS credentials
    AwsCredentials: str


class ConfigInputs(pydantic.BaseModel):
    """Configuration for the input data paths and file names"""

    # Config for input path and filenames
    InputPath: str
    ImmigrationInput: str
    AirportCodesInput: str
    CitiesInput: str


class ConfigStaging(pydantic.BaseModel):

    # Config for staging paths
    ImmigrationStaging: str
    AirportStaging: str
    CitiesStaging: str


class Config(pydantic.BaseModel):
    """Main configuration"""

    dag: ConfigDag
    emr: ConfigEmr
    s3: ConfigS3
    app: ConfigApp
    airflow: ConfigAirflow
    input: ConfigInputs
    staging: ConfigStaging


class ConfigException(Exception):
    pass


def load_yaml(config_path):

    """Function to load yaml file from path

    Parameters
    ----------
    config_path : str
        string containing path to yaml

    Returns
    ----------
    config : dict
        dictionary containing config

    """
    log.info("Importing config file from {}".format(config_path))

    if config_path is not None:
        with open(config_path, 'r') as stream:
            config = yaml.safe_load(stream)

        log.info("Succesfully imported the config file from {}".format(config_path))

    if config_path is None:
        raise ConfigException("Must supply path to the config file")

    return config
