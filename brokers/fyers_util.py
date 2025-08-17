import os
from file_utils import FileUtils


class FyersUtil:
    fyers = None
    io_folder = os.path.join(os.pardir)
    config_file_path = os.path.join(io_folder, "private", "config.csv")
    fyers_log_folder = os.path.join(io_folder, "log")
    data_folder = os.path.join(io_folder, "Data", "market_data")
    contract_folder = os.path.join(io_folder, "Contracts")
    file_utils = FileUtils()

    def __init__(self, fyers):
        self.fyers = fyers
        self.file_utils = FileUtils()
        self.prepare_options("config", "market_data", "contracts")

    def prepare_downloader(self, config_folder, _data_folder, _contract_folder):
        self.data_folder = _data_folder
        self.contract_folder = _contract_folder
        self.file_utils.delete_directory(os.path.join(self.data_folder, _contract_folder))
        #self.config_file_path = os.path.join(self.io_folder, config_folder, "config.csv")
        self.fyers_log_folder = os.path.join(self.io_folder, "log")
        if not os.path.exists(self.fyers_log_folder):
            os.makedirs(self.fyers_log_folder)
        #self.contract_folder = os.path.join(self.contract_folder,  _contract_folder)       
        if not os.path.exists(self.contract_folder):
            os.makedirs(self.contract_folder)
        #self.data_folder = os.path.join(self.io_folder, "data", _data_folder)
        self.data_folder = _data_folder
        if not os.path.exists(self.data_folder):
            os.makedirs(self.data_folder)
        self.download_status_log = os.path.join(self.data_folder, "log")


    def prepare_options(self, config_folder, _data_folder, _contract_folder):
        self._data_folder = _data_folder
        self.file_utils.delete_directory(os.path.join(self.data_folder, _contract_folder))
        self.config_file_path = os.path.join(self.io_folder, config_folder, "config.csv")
        self.fyers_log_folder = os.path.join(self.io_folder, "log")
        if not os.path.exists(self.fyers_log_folder):
            os.makedirs(self.fyers_log_folder)
        self.contract_folder = os.path.join(self.contract_folder,  _contract_folder)
        if not os.path.exists(self.contract_folder):
            os.makedirs(self.contract_folder)
        self.trades_log = os.path.join(self.data_folder, "log")