# External imports
from os import environ
from os.path import join, dirname, exists
import configparser


def get_config():
    config = configparser.ConfigParser(
        interpolation=configparser.ExtendedInterpolation()
    )
    def_pth = join(dirname(__file__), "model.cfg")
    config_pth = environ.get("MODEL_CONFIG", def_pth)
    if exists(config_pth):
        config.read(config_pth)
        return config
    else:
        raise FileNotFoundError("No Config path found")
