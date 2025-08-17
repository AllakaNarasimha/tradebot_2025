from tqdm import tqdm
from utils.download_utils import *
from past_dates import *
from brokers.fyers_fo import *

class DataProcessor:
    def __init__(self, broker, sql_util):
        self.broker = broker
        self.sql_util = sql_util

    def process_stock_download(self):
        self.download_type = "STOCK"
        res = self.broker_fo.ins_reader.get_inst_details(self.symbol) 
        return res
    
    def process_fut_download(self):
        self.download_type = "FUT"
        res = self.broker_fo.ins_reader.get_active_futures(self.symbol)         
        return res

    def process_all(self, status, dates, symbols, process, savedf) :
        dates = update_symbol_to_date(dates, symbols)
        existed_dates, new_dates = self.sql_util.check_records(dates)
        return process_new_all(status, new_dates, process, savedf)
    
    def process_options_download1(self): 
        self.download_type = "OPTION"
        sym = self.broker_fo.ins_reader.get_inst_details(self.symbol)
        optsymbol = sym['symbol'][0]
        oichain = self.broker.getOptionChain(optsymbol, self.strikes_count,  "")
        self.sql_util.insert_or_update_option_chain(oichain)
        oidfs = []
        min_strike = None
        max_strike = None
        for exp in oichain['expiryData']:            
            oic = self.broker.getOptionChain(optsymbol, self.strikes_count, exp['expiry'])
            self.sql_util.insert_or_update_option_chain(oic)
            oidfs.append(oic)
            mi, mx = get_min_max_strike_prices(oic['optionsChain'])
            if min_strike is None or mi < min_strike:
                min_strike = mi
            if max_strike is None or mx > max_strike:
                max_strike = mx                
        print(oidfs)
        res = self.broker_fo.ins_reader.get_options_by_range(self.symbol, min_strike, max_strike)
        print(res['expiry_date'].unique())
        res_next = getNextMonthsExpiry(res, 4)
        print(res_next['expiry_date'].unique())
        return res_next
    
    def process_options_download(self): 
        self.download_type = "OPTION"
        sym = self.broker_fo.ins_reader.get_inst_details(self.symbol)
        optsymbol = sym['symbol'][0]
        oichain = self.broker.getOptionChain(optsymbol, self.strikes_count,  "")
        self.sql_util.insert_or_update_option_chain(oichain['optionsChain'])
        oidfs = []
        min_strike = None
        max_strike = None
        for exp in oichain['expiryData']:            
            oic = self.broker.getOptionChain(optsymbol, self.strikes_count, exp['expiry'])
            self.sql_util.insert_or_update_option_chain(oic['optionsChain'])
            oidfs.append(oic)
            mi, mx = get_min_max_strike_prices(oic['optionsChain'])
            if min_strike is None or mi < min_strike:
                min_strike = mi
            if max_strike is None or mx > max_strike:
                max_strike = mx    
        
        all_options = []
        for oic in oidfs:
            all_options.extend(oic['optionsChain'])
        df = pd.DataFrame(all_options)
        symbols_df = df[['symbol']].drop_duplicates().reset_index(drop=True)
            
        res = self.broker_fo.ins_reader.get_options_by_range(self.symbol, min_strike, max_strike)
        filtered_res = res[res['symbol'].isin(symbols_df['symbol'])].reset_index(drop=True)
        print(filtered_res)
        return filtered_res
    
    def process_download_generic(self, symbol, process_function, start_date, range_month=1):
        self.symbol = symbol
        res = process_function()       
        current_date =  get_nth_month_first_date(1) 
        mdates = get_past_dates(start_date, range_month)     
        dates = group_dates_by_span(mdates, 2)             
        self.dfs = self.process_all("{}...".format(symbol), dates, res, self.broker.start_process, self.download_df_save)
        print("{}: {}".format(symbol, len(self.dfs)))
        if (len(self.dfs) > 0):
            save_result(self.dfs, current_date, symbol, self.broker_fo.fyers_util.data_folder)
            result_df = get_result_dfs(self.dfs)
            self.sql_util.insert_or_update_result_log(result_df)

    def process_download_india_vix(self, start_date, range_month = 1):
        self.symbol = "INDIAVIX" 
        res = self.process_stock_download()
        dates = get_past_dates(start_date, range_month)  
        dfs = self.process_all("{}...".format(self.symbol), dates, res, self.broker.start_process, self.download_df_save)   
        print("INDIAVIX: {}".format(len(dfs)))

    def process_download_by_stock(self):
        res = self.process_stock_download()
        current_date =  get_nth_month_first_date(1)
        start_date = datetime(current_date.year, current_date.month, 1) - timedelta(days=1)
        dates = get_past_dates(start_date, 1)  
        dfs = self.process_all("{}...".format(self.symbol), dates, res, self.broker.start_process, self.download_df_save)   
        print(len(dfs))
    
    def process_data(self, data):
        self.broker.start_process(data)
        processed_data = data  
        return processed_data   
    
    def downloadIndices(self, source_folder, symbol, start_date, range_month, strikes_count):
        self.symbol = symbol
        self.strikes_count = strikes_count
        self.data_folder = os.path.join(source_folder, self.symbol)
        if not os.path.exists(self.data_folder):
            os.makedirs(self.data_folder)

        self.process_download_generic(self.symbol, self.process_stock_download, start_date, range_month)
        self.process_download_generic(self.symbol, self.process_fut_download, start_date, range_month)
        self.process_download_generic(self.symbol, self.process_options_download, start_date, range_month)    

    def download_df_save(self, data):
        dt = data['dt']
        if self.download_type == "STOCK" or self.download_type == "FUT":
            directory = os.path.join(self.broker_fo.data_folder, self.download_type, dt['month'])
            if (self.download_type == "FUT"):
                self.sql_util.insert_or_update_futures(data['df'])
            elif (self.download_type == "STOCK"):
                self.sql_util.insert_or_update_stock(data['df'])
        else:
            strike_price = round(data["strike_price"])
            directory = os.path.join(self.data_folder, self.download_type, str(dt['expiry_date']), str(dt['month']), str(strike_price))
            self.sql_util.insert_or_update_options(data['df'])
        save_df(data, directory)
    
    def initialize(self):
        dt_string  = datetime.now().strftime("%m%Y_%H%M%S")
        parent_data_folder = os.path.join("data", dt_string)
        config_folder = os.path.join(os.pardir, "private")
        start_input = 1
        end_input = 1
        depth_input = 1     
        strikes_count = 5
        set_log_path(os.path.join(os.pardir, "log"))
        output_ranges = calculate_ranges(start_input, end_input, depth_input)          
        print(output_ranges)
        for lower, upper in output_ranges:
            current_date =  get_nth_month_first_date(lower) 
            start_date = datetime(current_date.year, current_date.month, 1) - timedelta(days=1)
            start_date = min(start_date, datetime.now())    
            dates = get_past_dates(start_date, upper)
            gdates = group_dates_by_span(dates, upper)        
            print(dates);
            print(gdates) 

        for lower, upper in output_ranges:  
            data_folder = os.path.join(parent_data_folder, "data")
            contract_folder = os.path.join(parent_data_folder, "contract")
            self.broker_fo = FyersFO(self.broker, lower, upper, config_folder, data_folder, contract_folder)
            current_date =  get_nth_month_first_date(lower) # 1 for current month
            start_date = datetime(current_date.year, current_date.month, 1) - timedelta(days=1)
            start_date = min(start_date, datetime.now())    

            dates = get_past_dates(start_date, upper);
            print(dates);    
            #self.broker.wait_for_authentication()     

            self.process_download_generic("INDIAVIX", self.process_stock_download, start_date, range_month=self.broker_fo.range_index)
            sourcedir = self.broker_fo.fyers_util.data_folder        

            self.downloadIndices(sourcedir, "NIFTY", start_date, self.broker_fo.range_index, strikes_count)
        zip(data_folder,  os.path.join(parent_data_folder,"backup"), '')