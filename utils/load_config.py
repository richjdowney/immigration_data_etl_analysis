import yaml


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

    with open(config_path, 'r') as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)

    return config
