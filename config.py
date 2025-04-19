import json

from typing import TypedDict

class Config(TypedDict):
    datapath: str
    datapath_1G: str
    datapath_10G: str
    datapath_30G: str


def load_config(config_path="./config.json") -> Config:
    with open(config_path) as config_file:
        config = json.load(config_file)
    return config
