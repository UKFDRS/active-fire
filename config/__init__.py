# __init__.py

import pathlib
import tomllib
import dotenv

base_dir = pathlib.Path(__file__).parent.parent
nrt_token = dotenv.dotenv_values()['nrt_token']

path = pathlib.Path(__file__).parent / "configuration.toml"
with path.open(mode="rb") as fp:
    config_dict = tomllib.load(fp)

config_dict["nrt_token"] = nrt_token
config_dict["base_dir"] = base_dir
