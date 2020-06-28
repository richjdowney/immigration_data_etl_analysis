import yaml
from typing import Any, Dict


class ConfigException(Exception):
    pass


def load_yaml(config_path) -> Dict[str, Any]:
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

    if config_path is not None:
        with open(config_path, 'r') as stream:
            config = yaml.safe_load(stream)

    if config_path is None:
        raise ConfigException("Must supply path to the config file")

    return config
