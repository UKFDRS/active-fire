# __init__.py

import pathlib
import tomli
import dotenv

base_dir = pathlib.Path(__file__).parent.parent
nrt_token = dotenv.dotenv_values()

path = pathlib.Path(__file__).parent / "configuration.toml"
with path.open(mode="rb") as fp:
    config_dict = tomli.load(fp)

config_dict["nrt_token"] = nrt_token
config_dict["base_dir"] = base_dir
