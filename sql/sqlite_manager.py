import sqlite3
from turtle import pd

class SQLiteManager:
    def __init__(self, db_path):
        self.db_path = db_path    

    def create_table(self, create_table_sql):
        """Create a table with the given SQL statement."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(create_table_sql)
            conn.commit()

    def insert_one(self, table, columns, values):
        """Insert a single row into a table."""
        cols = ', '.join(columns)
        placeholders = ', '.join(['?'] * len(values))
        sql = f'INSERT INTO {table} ({cols}) VALUES ({placeholders})'
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(sql, values)
            conn.commit()

    def insert_many(self, table, columns, values_list):
        """Insert multiple rows into a table."""
        cols = ', '.join(columns)
        placeholders = ', '.join(['?'] * len(columns))
        sql = f'INSERT INTO {table} ({cols}) VALUES ({placeholders})'
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.executemany(sql, values_list)
            conn.commit()
            
    def insert_many_ignore_duplicates(self, table, columns, values_list):
        cols = ', '.join(columns)
        placeholders = ', '.join(['?'] * len(columns))
        sql = f'INSERT OR IGNORE INTO {table} ({cols}) VALUES ({placeholders})'
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.executemany(sql, values_list)
            conn.commit()

    def select(self, query, params=None):
        """Run a SELECT query and return all results."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(query, params or ())
            results = cursor.fetchall()
        return results

    def execute(self, sql, params=None):
        """Execute custom SQL commands (e.g., UPDATE, DELETE)."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(sql, params or ())
            conn.commit()

# ======= Sample Usage ========

if __name__ == '__main__':
    # Initialize the SQLiteManager with a database file
    db = SQLiteManager('market_data.db')    

    # Insert a single row
    db.insert_one('stocks_indices',
                  ['datetime', 'symbol', 'strike_price', 'open', 'high', 'low', 'close', 'volume', 'symbol_info'],
                  ['2025-08-17 09:15:00', 'AAPL', 150.0, 148.50, 150.10, 147.80, 149.90, 200, 'Stock AAPL'])

    # Insert multiple rows
    rows = [
        ['2025-08-17 09:16:00', 'AAPL', 151.0, 149.00, 151.50, 148.80, 150.90, 250, 'Stock AAPL'],
        ['2025-08-17 09:17:00', 'MSFT', 320.0, 319.80, 321.00, 318.50, 320.50, 300, 'Stock MSFT']
    ]
    db.insert_many('stocks_indices',
                   ['datetime', 'symbol', 'strike_price', 'open', 'high', 'low', 'close', 'volume', 'symbol_info'],
                   rows)

    # Retrieve data
    results = db.select('SELECT * FROM stocks_indices WHERE symbol = ?', ('AAPL',))
    for row in results:
        print(row)

    # Custom SQL execution
    db.execute('DELETE FROM stocks_indices WHERE symbol = ?', ('MSFT',))
