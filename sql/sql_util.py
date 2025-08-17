import datetime
import numpy as np
import pandas as pd
from sql.monthly_db_manger import MonthlyDatabaseManager
from sql.sql_script import create_tables

class SQLUtil:
    def __init__(self, current_date):
        month_db = MonthlyDatabaseManager('sqldb')
        self.sqldb = month_db.get_manager(current_date)
        create_tables(month_db.get_db_path(current_date))
    
    def _prepare_df_with_index(self, df, index_column_name='id'):
        """Helper method to include the DataFrame index as a column if needed"""
        if isinstance(df, dict):
        # Convert dictionary to DataFrame (various approaches depending on structure)
            if all(isinstance(v, (list, tuple)) for v in df.values()):
                # Dict of lists/tuples
                df = pd.DataFrame(df)
            elif any(isinstance(v, dict) for v in df.values()):
                # Nested dictionaries
                df = pd.DataFrame.from_dict(df, orient='index')
            else:
                # Single row dict
                df = pd.DataFrame([df])
        else:
            df = df

        if isinstance(df, list):
            df = pd.DataFrame(df)

        is_default_index = isinstance(df.index, pd.RangeIndex) and df.index.start == 0 and df.index.step == 1
    
        if index_column_name in df.columns or is_default_index:
            return df
        
        df_copy = df.copy()
        
        if index_column_name not in df_copy.columns:
            df_copy = df_copy.reset_index().rename(columns={'index': index_column_name})
            
        return df_copy

    def _prepare_values_for_sqlite(self, values_list):
        """
        Convert values in the list to SQLite-compatible types.
        Particularly handles pandas Timestamp objects.
        """
        def convert_value(val):
            # Convert pandas Timestamp to datetime string
            if hasattr(val, 'strftime'):  # Check if it has strftime method (datetime-like)
                return val.strftime('%Y-%m-%d %H:%M:%S')
            # Convert numpy types to Python native types
            elif isinstance(val, np.integer):
                return int(val)
            elif isinstance(val, np.floating):
                return float(val)
            elif isinstance(val, np.bool_):
                return bool(val)
            # Return other types as is
            return val

        # Process each row
        converted_values = []
        for row in values_list:
            converted_row = [convert_value(val) for val in row]
            converted_values.append(converted_row)

        return converted_values
    
    def insert_or_update_stock(self, df):
        df = self._prepare_df_with_index(df)
        converted_values = self._prepare_values_for_sqlite(df.values.tolist())

        self.sqldb.insert_many_ignore_duplicates('stocks', df.columns.tolist(), converted_values)

    def insert_or_update_futures(self, df):
        df = self._prepare_df_with_index(df)
        converted_values = self._prepare_values_for_sqlite(df.values.tolist())

        self.sqldb.insert_many_ignore_duplicates('futures', df.columns.tolist(), converted_values)

    def insert_or_update_options(self, df):
        df = self._prepare_df_with_index(df)
        converted_values = self._prepare_values_for_sqlite(df.values.tolist())
        self.sqldb.insert_many_ignore_duplicates('options', df.columns.tolist(), converted_values)

    def insert_or_update_option_chain(self, df):
        df = self._prepare_df_with_index(df)
        converted_values = self._prepare_values_for_sqlite(df.values.tolist())
        self.sqldb.insert_many_ignore_duplicates('option_chain', df.columns.tolist(), converted_values)

    def insert_or_update_result_log(self, df):
        df = self._prepare_df_with_index(df)
        converted_values = self._prepare_values_for_sqlite(df.values.tolist())
        self.sqldb.insert_many_ignore_duplicates('logger', df.columns.tolist(), converted_values)

    def check_records(self, df):
        query = """
            SELECT 1 FROM logger
            WHERE start_date = ? AND end_date = ? AND month = ? AND symbol = ? AND strike_price = ?
            LIMIT 1
        """
        existing = []
        new = []
        for rec in df:
            result = self.sqldb.select(query, (rec["start_date"], rec["end_date"], rec["month"], rec["symbol"], rec["strike_price"]))
            if result:
                existing.append(rec)
            else:
                new.append(rec)

        return existing, new
    
    def get_atm_strike_price(self, expiry_date, datetime = "2025-08-01 09:15:00", stock_symbol = "NSE:NIFTY50-INDEX", underlying = "NSE:NIFTY"):
        atm_query = """
            WITH stock_close AS (
                SELECT price,
                       CAST(ROUND(price / 50.0) * 50 AS INT) AS atm_strike
                FROM stocks
                WHERE symbol = :stock_symbol
                  AND datetime = :time
            )
            SELECT o.symbol, o.last_price
            FROM options o
            JOIN stock_close sc
              ON o.symbol LIKE :underlying || '%' 
             AND o.symbol LIKE '%' || sc.atm_strike || '%'
            WHERE o.datetime = :time;
            """
        params = {
            "stock_symbol": stock_symbol,
            "time": datetime,
            "underlying": underlying
        }