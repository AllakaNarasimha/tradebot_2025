import sys
import os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(project_root)  # Add project root to path
sys.path.append(os.path.join(project_root, "utils"))  # Add utils directory

import json
from logger import setup_logger
from brokers.fyers_broker import FyersBroker # type: ignore
from strategy import run_strategy
from data_processor import *
from sql.sql_util import SQLUtil

  # type: ignore

def get_broker(name, config):
    log_path = "log"
    if name == "fyers":
        return FyersBroker(config, log_path=log_path)
    else:
        raise ValueError(f"Unsupported broker: {name}")

if __name__ == "__main__":
    config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "private", "config.json"))
    config_path1 = os.path.abspath(os.path.join(os.path.dirname(__file__), "config.json"))
    with open(config_path) as f:
        config = json.load(f)

    logger = setup_logger()
    
    broker = get_broker(config["broker"], config[config["broker"]])
    broker.wait_for_authentication()

    sql_util = SQLUtil(datetime.now())

    data_processor = DataProcessor(broker, sql_util)
    data_processor.initialize()
