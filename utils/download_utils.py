import pandas as pd
from tqdm import tqdm
from datetime import  *
from past_dates import *
from custom_logger import *
from zipper import *

def update_symbol_to_date(dates, symbols) :
    res = []
    for i, sym in symbols.iterrows():
        for dt in dates:    
            c_dt = dt.copy()
            c_dt['symbol'] = sym['symbol']
            c_dt['strike_price'] = sym['strike_price']            
            c_dt['symbol_info'] = sym['symbol_info']
            c_dt['expiry_date'] = sym['expiry_date']
            res.append(c_dt)
    return res            
def process_new_all(status, dates, process, savedf):
    dfs = []
    with tqdm(total=len(dates), desc=status) as pbar:
        for dt in dates:
            df = process(dt)
            if not df.empty :            
                df['symbol'] = dt['symbol']
                df['symbol_info'] = dt['symbol_info']
                df['strike_price'] = dt['strike_price']
                dt = {
                    "dt" : dt,
                    "symbol" : dt["symbol"].replace("NSE:", ""),
                    "symbol_info" : dt['symbol_info'],
                    "strike_price" : dt['strike_price'],
                    "df" : df                
                }  
                if savedf :          
                    savedf(dt) 
                dfs.append(dt)  
            pbar.update(1) 
    return dfs

def merge_df(dataframes):
    # Extract the DataFrames from the list
    dfs = [entry["df"] for entry in dataframes if not entry['df'].empty]
    if not dfs:
        return {
            "min_close" : 0,
            "max_close" : 0,
            "min_dateTime" : datetime.now,
            "max_dateTime" : datetime.now            
        }
    # Concatenate the list of DataFrames into a single DataFrame
    combined_df = pd.concat(dfs, ignore_index=False)

    # Find the minimum and maximum values of the "Close" column
    min_close = combined_df['close'].min()
    max_close = combined_df['close'].max()
    min_dateTime = combined_df.index.min().strftime("%Y-%m-%d")
    max_dateTime = combined_df.index.max().strftime("%Y-%m-%d")
    
    return {        
        "min_close" : min_close,
        "max_close" : max_close,
        "min_dateTime" : min_dateTime,
        "max_dateTime" : max_dateTime
    }

def calculate_ranges(start, end, depth):
    ranges = []
    step = (end) / depth
    i = step
    j = 0
    while i > 0:  
        if j == 0:      
            lower_bound = round(start)
        else :
            lower_bound -= (depth + 1)
        ranges.append((lower_bound, depth))
        i -= 1
        j += 1
    return ranges
    
def test(dt):
    print(dt)

def get_min_max_strike_prices(options):
    strike_prices = [option['strike_price'] for option in options if option['strike_price'] != -1]
    if strike_prices:
        min_strike = min(strike_prices)
        max_strike = max(strike_prices)
        return min_strike, max_strike
    else:
        return None, None
    
def update_symbol_to_date(dates, symbols) :
    res = []
    for i, sym in symbols.iterrows():
        for dt in dates:    
            c_dt = dt.copy()
            c_dt['symbol'] = sym['symbol']
            c_dt['strike_price'] = sym['strike_price']            
            c_dt['symbol_info'] = sym['symbol_info']
            c_dt['expiry_date'] = sym['expiry_date']
            res.append(c_dt)
    return res   

def process_new_all(status, dates, process, savedf):
    dfs = []
    with tqdm(total=len(dates), desc=status) as pbar:
        for dt in dates:
            df = process(dt)
            if not df.empty :            
                df['symbol'] = dt['symbol']
                df['symbol_info'] = dt['symbol_info']
                df['strike_price'] = dt['strike_price']
                dt = {
                    "dt" : dt,
                    "symbol" : dt["symbol"].replace("NSE:", ""),
                    "symbol_info" : dt['symbol_info'],
                    "strike_price" : dt['strike_price'],
                    "df" : df                
                } 
                df['status'] = "Success"
                if savedf :          
                    savedf(dt) 
                dfs.append(dt)  
            else:
                df['symbol'] = dt['symbol']
                df['symbol_info'] = dt['symbol_info']
                df['strike_price'] = dt['strike_price']
                df['status'] = "No Data"
                dt = {
                    "dt" : dt,
                    "symbol" : dt["symbol"].replace("NSE:", ""),
                    "symbol_info" : dt['symbol_info'],
                    "strike_price" : dt['strike_price'],
                    "df" : df                
                } 
                dfs.append(dt)   
            pbar.update(1) 
    return dfs

def save_result(dts, current_date, sym, result_folder_path):
    result_df = get_result_dfs(dts)
    result_file_path = os.path.join(result_folder_path,
                    current_date.strftime("STATUS_%d%m%Y_%H%M%S_INDEX_{}.CSV".format(sym)))
    result_df.to_csv(result_file_path)

def get_result_dfs(dts):
    rows = []    
    for dt in dts: 
        if 'status' in dt:
            status = dt['status']
        else:
            status = None          
        row_data = {
            'start_date': dt['dt']['start_date'],
            'end_date': dt['dt']['end_date'],
            'month': dt['dt']['month'],
            'symbol': dt['dt']['symbol'],
            'expiry_date' : dt['dt']['expiry_date'],
            'symbol_info': dt['symbol_info'],
            'strike_price': dt['strike_price'],  
            'status': status,                           
        }
        rows.append(row_data) 
    df = pd.DataFrame(rows)    
    return df

def save_df(data, directory):
    df = data['df']
    sym = data["symbol_info"]
    os.makedirs(directory, exist_ok=True)
    df.to_csv(os.path.join(directory, f"{sym}.csv"))

def getNextMonthsExpiry(symbols, month_count):
    symbols['expiry_date'] = pd.to_datetime(symbols['expiry_date'])
    symbols = symbols.sort_values('expiry_date')
    
    cmaxexp = set(symbols['expiry_date'].unique()[:month_count])
    
    c_expiry = pd.Timestamp.now().normalize()
    max_date = c_expiry + pd.DateOffset(months = 2)
    next_symbols = symbols[symbols['expiry_date'] <= max_date]
    maxexp = next_symbols['expiry_date'].unique().max()
    
    if maxexp not in cmaxexp: 
        cmaxexp.add(maxexp)
    
    next_symobls = symbols[symbols['expiry_date'].isin(cmaxexp)]
    next_symobls['expiry_date'] = next_symobls['expiry_date'].dt.strftime('%Y-%m-%d')
    return next_symobls

def zip(folder_to_zip, target_folder, symbol):
    zip_file_name = os.path.join(target_folder, datetime.now().strftime("BACKUP_{}_%d%m%Y_%H%M%S.zip".format(symbol)))
    folder_zipper = Zipper(folder_to_zip, target_folder, zip_file_name)
    folder_zipper.zip_folder()
        

if __name__ == '__main__':
    start_date_str = "2023-01-01"
    end_date_str = "2023-12-31"
    today_date_str = datetime.now().strftime('%Y-%m-%d')
    symbol = "NSE:BANKNIFTY"