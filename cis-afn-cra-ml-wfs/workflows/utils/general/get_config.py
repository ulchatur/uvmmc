import os
import json

def get_config(config_name):
    f = open(os.path.join("config", config_name))
    res = json.load(f)
    return res
