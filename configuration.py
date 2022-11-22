import os
from dotenv import dotenv_values
from configparser import ConfigParser


class Config:

    __conf = None
    basedir = os.path.abspath(os.path.dirname(__file__))
    nrt_token = dotenv_values(os.path.join(basedir, ".env"))["nrt_token"]

    @staticmethod
    def config():
        if Config.__conf is None:  # Read only once, lazy.
            config_path = os.path.join(Config.basedir, "config.ini")
            Config.__conf = ConfigParser()
            Config.__conf.read(config_path)
        return Config.__conf
