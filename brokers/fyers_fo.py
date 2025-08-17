import os
from brokers.instrument_reader import * 
from brokers.fyers_util import * 

class FyersFO:
    fyers = None
    io_folder = os.path.join(os.pardir)
    fyers_log_folder = os.path.join(io_folder, "log")
    data_folder = os.path.join(io_folder, "data", "market_data")
    contract_folder = os.path.join(io_folder, "contracts")
    symbol = "BANKNIFTY"    

    def __init__(self, fyers, start_month, back_range_month, config_folder, db_folder, contract_folder):
        self.fyers = fyers
        self.data_folder = db_folder
        self.contract_folder = contract_folder
        self.start_index = start_month
        self.range_index = back_range_month        
        self.fyers_util = FyersUtil(self.fyers)
        self.fyers_util.prepare_downloader(config_folder, db_folder, contract_folder)
        self.ins_reader = InstrumentReader(self.contract_folder)

    def initialize(self):
        if not os.path.exists(self.contract_folder):
            os.makedirs(self.contract_folder)
    
    
