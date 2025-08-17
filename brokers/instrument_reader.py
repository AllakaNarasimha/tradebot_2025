import pandas as pd
import requests
import io
import csv
import os
from datetime import datetime
from enum import Enum
from custom_logger import *

class UrlType(Enum):
    CM = "cm"
    FO = "fo"
    CD = "cd"
    MCX_COM = "mcx_com"
    BSE_COM = "bse_com"
    BSE_FO = "bse_fo"
    
class InstrumentReader:
    data_folder = "market_data"
    def initialize(self):
        # Create the 'log' folder if it doesn't exist
        if not os.path.exists(self.data_folder):
            os.makedirs(self.data_folder)
        if not os.path.exists(self.contract_folder):
            os.makedirs(self.contract_folder)
                            
        self.url_dict = {
            UrlType.CM: "https://public.fyers.in/sym_details/NSE_CM.csv",
            UrlType.FO: "https://public.fyers.in/sym_details/NSE_FO.csv",
            UrlType.CD: "https://public.fyers.in/sym_details/NSE_CD.csv",
            UrlType.MCX_COM: "https://public.fyers.in/sym_details/MCX_COM.csv",
            UrlType.BSE_COM: "https://public.fyers.in/sym_details/BSE_CM.csv",
            UrlType.BSE_FO: "https://public.fyers.in/sym_details/BSE_FO.csv"
        }        
        self.nse_file_dict = {
            UrlType.CM: "nse_cm.csv",
            UrlType.FO: "nse_fo.csv",
            UrlType.CD: "nse_cd.csv",
            UrlType.MCX_COM: "nse_mcx_com.csv",
            UrlType.BSE_COM: "nse_bse_com.csv",
            UrlType.BSE_FO: "nse_bse_fo.csv"
        }
        self.ab_nse_file_dict = {
            UrlType.CM: "ab_nse_cm.csv",
            UrlType.FO: "ab_nse_fo.csv",
            UrlType.CD: "ab_nse_cd.csv",
            UrlType.MCX_COM: "ab_nse_mcx_com.csv",
            UrlType.BSE_COM: "ab_nse_bse_com.csv",
            UrlType.BSE_FO: "ab_nse_bse_fo.csv"
        }

    def __init__(self, _data_folder):
        self.data_folder = _data_folder
        self.contract_folder =  _data_folder #os.path.join(self.data_folder, 'contract')
        self.initialize()
        
    def get_csv_data(self, url_type):
        self.url = self.url_dict.get(url_type, "Invalid URL type")
        self.nse_file = self.nse_file_dict.get(url_type, "Invalid")
        self.ab_nse_file = self.ab_nse_file_dict.get(url_type, "Invalid")
        self.nse_path = os.path.join(self.contract_folder, self.nse_file)
        self.ab_nse_path = os.path.join(self.contract_folder, self.ab_nse_file)

        if self.url == "Invalid URL type" :
            print("url not found")
            return None
        else:
            response = requests.get(self.url)
            instruments = pd.read_csv(io.StringIO(response.text),header=None)
            instruments.to_csv(self.nse_path,index=False)
            return instruments

    def get_instruments_info(self, url_type, spot_name):
        spot_file = '{}\\NSE_{}_{}.csv'.format(self.contract_folder, url_type.name, spot_name)
        instruments = self.get_csv_data(url_type)
        wr = csv.writer(open(spot_file,'w'))
        instruments=instruments[instruments[13]==spot_name]
        if not instruments.empty : 
            df = pd.DataFrame()       
            instruments[8]=pd.to_datetime(instruments[8],unit='s')
            instruments[8]=instruments[8].dt.strftime('%Y-%m-%d')
            df["expiry_start_date"]  = instruments[7]
            df["expiry_date"]  = instruments[8]
            df["strike_price"] = instruments[15]
            df["option"] = instruments[16]
            df["symbol"] = instruments[9]
            df["symbol_info"] = instruments[1]
            df["spot"] = instruments[13]
            df.to_csv(spot_file)
            
    def get_instrument_details(self, url_type, spot_name, broker = "", retry = 0):
        spot_file = '{}\\NSE_{}_{}.csv'.format(self.contract_folder, url_type.name, spot_name)
        if os.path.isfile(spot_file):
            print('{} existed.'.format(spot_file))
            df = pd.read_csv(spot_file)
            df = df.sort_values(by="option", key = lambda x: x.map({value: index for index, value in enumerate(['XX', 'CE', 'PE'])}))
            return df
        else :
            if retry > 3:
                print("unable to get the spot price")
                return None
            retry = retry + 1
            print('downloading {}'.format(spot_file))
            if broker == "ab": self.get_instruments_info(url_type, spot_name)
            else : self.get_instruments_info(url_type, spot_name)
            return self.get_instrument_details(url_type, spot_name, broker = "", retry=retry)
    
    def get_symbol(self, df):
        dfs = pd.DataFrame()
        dfs['symbol'] = df['symbol']
        dfs['symbol_info'] = df['symbol_info']
        dfs['strike_price'] = df['strike_price']
        dfs['expiry_date'] = df['expiry_date']
        return dfs
    
    def get_active_futures(self, spot_name) :
        df = self.get_instrument_details(UrlType.FO, spot_name, broker = "", retry = 3)
        df = df[df['option'] == "XX"]    
        return self.get_symbol(df)
    
    def nearest_strike(self, current_price):
        strike = round(current_price / 100) * 100
        return strike 

    def getOptionExpires(self, spot_name):
        df = self.get_instrument_details(UrlType.FO, spot_name, broker = "", retry = 3)
        expires = df['expiry_date'].unique()         
        return expires
    
    def get_options_by_range(self, spot_name, min_spot, max_spot) :
        df = self.get_instrument_details(UrlType.FO, spot_name, broker = "", retry = 3)
        df = df[df['option'] != "XX"]
        lower_bound = self.nearest_strike(min_spot) 
        upper_bound = self.nearest_strike(max_spot)
        df = df[(df['strike_price'] >= lower_bound) & (df['strike_price'] <= upper_bound)]
        return self.get_symbol(df)
    
    def get_inst_details(self, stock_name):
        df = self.get_instrument_details(UrlType.CM, stock_name, broker = "", retry = 3)
        return self.get_symbol(df)    
        
if __name__ == '__main__':
    data_folder = os.path.join(os.path.pardir, "Data", "market_data")
    is_reader = InstrumentReader(data_folder)    
    #mdf = is_reader.get_instrument_details(UrlType.FO, "BANKNIFTY")
    #print("get_instruments_details:\n{}".format(mdf))   
    
    #mdf = is_reader.get_active_futures("BANKNIFTY") 
    #print("get_instruments_details:\n{}".format(mdf))   
    
    sdf = is_reader.get_instruments_info(UrlType.CM, "INDIAVIX") 
    print("get_csv_data:\n{}".format(sdf)) 