import os
import logging
import inspect
from datetime import datetime

class Logger:
    log_folder = 'log'
    log_file = 'output.txt'
    status_log_file = datetime.now().strftime("STATUS_%d%m%Y_%H%M%S.CSV")
    status_log_nodata_file = datetime.now().strftime("NODATA_%d%m%Y_%H%M%S.CSV")
    status_log_error_file = datetime.now().strftime("ERROR_%d%m%Y_%H%M%S.CSV")
    
    def __init__(self, _log_folder):
        self.log_folder = _log_folder
        if not os.path.exists(self.log_folder):
            os.makedirs(self.log_folder)
            
        self.log_path = os.path.join(self.log_folder, self.log_file)
        self.logger1 = logging.getLogger('logger1')
        self.logger1.level = logging.INFO
        handler1 = logging.FileHandler(self.log_path)
        formatter1 = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler1.setFormatter(formatter1)
        self.logger1.addHandler(handler1)

        self.status_log_path = os.path.join(self.log_folder, self.status_log_file)
        self.logger2 = logging.getLogger('logger2')
        self.logger2.level = logging.INFO
        handler2 = logging.FileHandler(self.status_log_path, mode='a')  # Use mode='a' for append
        formatter2 = logging.Formatter('%(asctime)s,%(levelname)s,%(message)s')
        handler2.setFormatter(formatter2)
        self.logger2.addHandler(handler2)
        
        self.status_log_nodata_path = os.path.join(self.log_folder, self.status_log_nodata_file)
        self.logger_nodata = logging.getLogger('nodata')
        self.logger_nodata.level = logging.INFO
        handler_nodata = logging.FileHandler(self.status_log_nodata_path, mode='a')  # Use mode='a' for append
        formatter_nodata = logging.Formatter('%(asctime)s,%(levelname)s,%(message)s')
        handler_nodata.setFormatter(formatter_nodata)
        self.logger_nodata.addHandler(handler_nodata)
        
        self.status_log_error_data_path = os.path.join(self.log_folder, self.status_log_error_file)
        self.logger_error_data = logging.getLogger('errordata')
        self.logger_error_data.level = logging.INFO
        handler_error_data = logging.FileHandler(self.status_log_error_data_path, mode='a')  # Use mode='a' for append
        formatter_no_data = logging.Formatter('%(asctime)s,%(levelname)s,%(message)s')
        handler_error_data.setFormatter(formatter_no_data)
        self.logger_error_data.addHandler(handler_error_data)

    def print(self, *args, **kwargs):
        frame_info = inspect.stack()[1]
        calling_file = os.path.basename(frame_info.filename)
        message = ' '.join(map(str, args))
        
        self.logger1.info(f"{calling_file}: {message}")

    def status(self, *args, **kwargs):
        frame_info = inspect.stack()[1]
        calling_file = os.path.basename(frame_info.filename)
        message = ' '.join(map(str, args))

        self.logger2.info(f"{calling_file}: {message}")
    
    def log_nodata(self, *args, **kwargs):
        frame_info = inspect.stack()[1]
        calling_file = os.path.basename(frame_info.filename)
        message = ' '.join(map(str, args))

        self.logger_nodata.info(f"{calling_file}: {message}")
    
    def log_error(self, *args, **kwargs):
        frame_info = inspect.stack()[1]
        calling_file = os.path.basename(frame_info.filename)
        message = ' '.join(map(str, args))

        self.logger_error_data.info(f"{calling_file}: {message}")
        
    print = print
    status = status
    no_data = log_nodata
    error_data = log_error
    
logger_instance = Logger("log")
def set_log_path(log_path):
    logger_instance = Logger(log_path)
def status(*args, **kwargs):
    status = logger_instance.status
def no_data(*args, **kwargs):
    no_data = logger_instance.no_data
def error_data(*args, **kwargs):
    error_data = logger_instance.error_data

if __name__ == '__main__': 
    print("This is a debug message")  # Goes to logger1 in output.txt
    set_log_path(os.path.join(os.pardir, "log"))
    status("This is a status message")  # Goes to logger2 in STATUS_*.CSV
