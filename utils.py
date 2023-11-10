#!/usr/bin/python3.11

__created__ = "10.11.2023"
__last_update__ = "10.11.2023"
__author__ = "https://github.com/pyautoml"

import os
import sys
import json

def absolute_path(path: str = None) -> [str | None]:
    if not path:
        return os.path.abspath(os.path.join(os.path.dirname(__file__)))
    else:
        if not isinstance(path, str):
            # logger.exception(f"Path must be str type, not {type(path)}.")
            return None
        if not path:
            # logger.exception("Path cannot be empty.")
            return None
        return os.path.abspath(os.path.join(os.path.dirname(__file__), path))
    
def get_configuration_path() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "configuration"))


def load_json_data(file_path: str) -> json:
    if not os.path.exists(file_path):
        new_path = absolute_path(file_path)
        if not os.path.exists(file_path):
            # logger.error(f"Path {path} does not exist.")
            return None
        else:
            return new_path
    try:
        with open(file_path, "r") as settings:
            data = json.load(settings)
    except Exception as e:
        # logger.exception(f"e")
        return None
    return data
