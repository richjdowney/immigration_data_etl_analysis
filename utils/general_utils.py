import subprocess
import os


def create_egg(path):
    """Function to create an egg file

    Parameters
    ----------
    path : str
        string containing path setup.py
    """

    os.chdir(path)
    bash_command = "python setup.py bdist_egg"
    process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
    process.communicate()
