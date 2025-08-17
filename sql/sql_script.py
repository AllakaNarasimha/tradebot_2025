import sqlite3

def create_tables(db_path='market_data.db'):
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        # Table: optionchain
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS option_chain (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ask REAL,
                bid REAL,
                description TEXT,
                ex_symbol TEXT,
                exchange TEXT,
                fp REAL,
                fpch REAL,
                fpchp REAL,
                fyToken TEXT,
                ltp REAL,
                ltpch REAL,
                ltpchp REAL,
                option_type TEXT,
                strike_price REAL,
                symbol TEXT,
                oi INTEGER,
                oich INTEGER,
                oichp REAL,
                prev_oi INTEGER,
                volume INTEGER
            )
        ''')

        # Table: option
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS options (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                datetime TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                oi INTEGER,
                symbol TEXT,
                symbol_info TEXT,
                strike_price REAL,
                status TEXT,
                UNIQUE(datetime, symbol, strike_price)
            )
        ''')
        # Table: futures
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS futures (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                datetime TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                oi INTEGER,
                symbol TEXT,
                symbol_info TEXT,
                strike_price REAL,
                status TEXT,
                UNIQUE(datetime, symbol, strike_price)
            )
        ''')

        # Table: stock_indices
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stocks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                datetime TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                symbol TEXT,
                symbol_info TEXT,
                strike_price REAL,
                status TEXT,
                UNIQUE(datetime, symbol)
            )
        ''')

        # Table: logger
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS logger (
                sno INTEGER PRIMARY KEY AUTOINCREMENT,
                start_date TEXT,
                end_date TEXT,
                month TEXT,
                symbol TEXT,
                symbol_info TEXT,
                strike_price REAL,
                expiry_date TEXT,
                status TEXT
            )
        ''')

        conn.commit()

if __name__ == '__main__':
    create_tables()
    print("Tables created successfully.")
