import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../utils")))

from fyers_apiv3 import fyersModel
from brokers.base import BrokerInterface
from utils.config import * 
import webbrowser
import pandas as pd
from time import sleep
import os
from utils.https_listener import *
from datetime import datetime

class FyersBroker(BrokerInterface):
    appSession = None
    client_id = None
    fyers = None
    log_path = None
    cfg = None

    def __init__(self, config, log_path):
        self.authenticated = False
        config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../", "private", "config.json"))

        self.config_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../..", "private"))
        self.access_token_file = os.path.join(self.config_folder, "access_token.txt")
        self.cfg = Config(config_path=config_path)
        self.config = config    
        self.log_path = log_path
        redirect_uri = self.config["redirect_uri"]
        self.client_id = self.config["client_id"]                                        ## Client_id here refers to APP_ID of the created app
        secret_key = self.config["secret_key"]                                          ## app_secret key which you got after creating the app 
        grant_type = self.config["grant_type"]                  ## The grant_type always has to be "authorization_code"
        response_type = self.config["response_type"]                             ## The response_type always has to be "code"
        state = self.config["state"]
        self.appSession = fyersModel.SessionModel(client_id=self.client_id,
                                                    redirect_uri = redirect_uri,
                                                    response_type = response_type,
                                                    state = state,
                                                    secret_key=secret_key,
                                                    grant_type=grant_type)
        self.main()

    def __init1__(self, config_file, log_path):
        self.authenticated = False
        self.config_folder = os.path.dirname(config_file)
        self.access_token_file = os.path.join(self.config_folder, "access_token.txt");
        self.cfg = Config(config_path=config_file)
        self.config =  self.cfg.get_config()    
        self.log_path = log_path
        redirect_uri= self.config["redirect_uri"]                     ## redircet_uri you entered while creating APP.
        self.client_id = self.config["client_id"]                                        ## Client_id here refers to APP_ID of the created app
        secret_key = self.config["secret_key"]                                          ## app_secret key which you got after creating the app 
        grant_type = self.config["grant_type"]                  ## The grant_type always has to be "authorization_code"
        response_type = self.config["response_type"]                             ## The response_type always has to be "code"
        state = self.config["state"] 
        self.appSession = fyersModel.SessionModel(client_id = self.client_id,
                                                    redirect_uri = redirect_uri,
                                                    response_type = response_type,
                                                    state = state,
                                                    secret_key=secret_key,
                                                    grant_type=grant_type)
        self.main()

    def generate_auth_token(self) :
        generateTokenUrl = self.appSession.generate_authcode()
        print((generateTokenUrl))  
        webbrowser.open(generateTokenUrl,new=1)
        start_server(self.generate_access_token)    
        
    def generate_access_token(self, ath_code):        
        self.appSession.set_token(ath_code)
        response = self.appSession.generate_token()
        try: 
            access_token =  response["access_token"]   
            self.cfg.save_text_to_file(self.access_token_file, access_token)     
        except Exception as e:
            print(e,response)
        self.set_fyers_model(access_token)

    def set_fyers_model(self, access_token):
        try:
            self.fyers = fyersModel.FyersModel(token=access_token,
                                            is_async = False,
                                            client_id = self.client_id,
                                            log_path = self.log_path)
            profile = self.fyers.get_profile()
            print(profile)
            if profile['s'] == 'error':
                self.authenticated = False   
            else :
                self.authenticated = True         
        except Exception as e:
            print(e, access_token)
            self.authenticated = False
        return self.authenticated
    
    def wait_for_authentication(self, timeout_seconds=60):
        start_time = datetime.now()

        while not self.authenticated:
            elapsed_time = datetime.now() - start_time
            if elapsed_time.total_seconds() > timeout_seconds:
                print("Timeout reached. Condition not met.")
                return False

            sleep(1)

        print("Authentication successful.")
        return True
    
    def main(self):  
        access_token = self.cfg.read_text_from_file(self.access_token_file)
        if access_token:
            if self.set_fyers_model(access_token) == False :
                self.generate_auth_token()
            else:
                print("Success")
        else:        
            self.generate_auth_token() 

    def place_order(self, symbol, qty, side, order_type, product_type, price=0):
        data = {
            "symbol": symbol,
            "qty": qty,
            "type": order_type,
            "side": side,
            "productType": product_type,
            "limitPrice": price,
            "stopPrice": 0,
            "validity": "DAY",
            "offlineOrder": "False"
        }
        return self.fyers.place_order(data=data)

    def get_positions(self):
        return self.fyers.positions()

    def get_order_status(self, order_id):
        orders = self.fyers.orderbook()
        for o in orders.get("orderBook", []):
            if o["id"] == order_id:
                return o
        return None

    def get_mtm(self):
        pos = self.fyers.positions()
        if pos.get("code") == 200:
            return pos["overall"]["pl_total"]
        return None

    def cancel_order(self, order_id):
        return self.fyers.cancel_order(data={"id": order_id})
    
    def start_process(self,dt) :
        return self.historical_by_date(dt['symbol'], dt['start_date'], dt['end_date'], 1, 0)
    
    def historical_by_date(self, symbol,sd,ed, interval = 1, retry = 3):
        data = {"symbol": str(symbol), 
                "resolution":"1",
                "date_format":"1",
                "range_from":str(sd),
                "range_to":str(ed),
                "cont_flag":"1",
                "oi_flag":"1"}
        data_str = ", ".join(f"{key}:{value}" for key, value in data.items())
        try: 
            nx = self.fyers.history(data)
            if 's' in nx:
                status_code = nx['s']                
                if status_code == 'error':
                    print("{}, {}".format(nx, data_str))
                    error_data("Error,{},{},{}".format(symbol, data_str, nx))
                    self.authenticated = False
                    if retry > 0:
                        self.generate_auth_token()
                        self.wait_for_authentication()
                        retry = retry-1
                        print("Retrying {} {}, {}".format(retry, nx, data_str))
                        self.historical_by_date(self, symbol,sd,ed, interval, retry)                        
                    return pd.DataFrame()
                if status_code == 'no_data' :
                    print('no data: {}'.format(data_str))
                    no_data("No Data,{},{},{}".format(symbol, data_str, nx))
                    return pd.DataFrame()        
                else: #if status_code == 'ok' :
                    print('data: {}'.format(data_str))
                    status("Downloaded Data,{},{}".format(symbol, data_str))
                    return self.formatted_df(nx)        
        except Exception as e:
            print(f"Error making API call: {e}")
            error_data("Error,{},{},{}".format(symbol, data_str, e))
            return pd.DataFrame()

    def getOptionChain(self, symbol, strikecount, timestamp, retry = 3):
        
        data = {"symbol": symbol, 
                "strikecount":strikecount,                
                "timestamp": timestamp
                }
        data_str = ", ".join(f"{key}:{value}" for key, value in data.items())
        try: 
            nx = self.fyers.optionchain(data)
            if 's' in nx:
                status_code = nx['s']                
                if status_code == 'error':
                    print("{}, {}".format(nx, data_str))
                    error_data("Error,{},{},{}".format(symbol, data_str, nx))
                    self.authenticated = False
                    if retry > 0:
                        self.generate_auth_token()
                        self.wait_for_authentication()
                        retry = retry-1
                        print("Retrying {} {}, {}".format(retry, nx, data_str))
                        #self.historical_by_date(self, symbol, start_date, end_date, interval, retry)
                    return pd.DataFrame()
                if status_code == 'no_data' :
                    print('no data: {}'.format(data_str))
                    no_data("No Data,{},{},{}".format(symbol, data_str, nx))
                    return pd.DataFrame()        
                else: #if status_code == 'ok' :
                    print('data: {}'.format(data_str))
                    status("Downloaded Data,{},{}".format(symbol, data_str))
                    return nx['data']        
        except Exception as e:
            print(f"Error making API call: {e}")
            error_data("Error,{},{},{}".format(symbol, data_str, e))
            return pd.DataFrame()
        
    def formatted_df(self, nx):
        cols = ['datetime','open','high','low','close','volume']
        df = pd.DataFrame.from_dict(nx['candles'])
        if (len(df.columns) > len(cols)):
            cols.append('oi')
        df.columns = cols
        df['datetime'] = pd.to_datetime(df['datetime'],unit = "s")
        df['datetime'] = df['datetime'].dt.tz_localize('utc').dt.tz_convert('Asia/Kolkata')
        df['datetime'] = df['datetime'].dt.tz_localize(None)    
        df = df.set_index('datetime')   
        return df