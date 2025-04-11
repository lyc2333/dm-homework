import json

from typing import TypedDict

class Config(TypedDict):
    datapath: str


def load_config(config_path="./config.json") -> Config:
    with open(config_path) as config_file:
        config = json.load(config_file)
    return config
