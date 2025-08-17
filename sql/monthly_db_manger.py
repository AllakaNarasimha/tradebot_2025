import datetime
import os

from sql.sql_script import create_tables
from sql.sqlite_manager import SQLiteManager

class MonthlyDatabaseManager:
    def __init__(self, base_dir="databases"):
        self.base_dir = base_dir
        # Create base directory if it doesn't exist
        if not os.path.exists(base_dir):
            os.makedirs(base_dir)
        
    def get_db_path(self, date=None):
        """Generate database path based on month and year"""
        if date is None:
            date = datetime.now()
        
        # Format: YYYY_MM.db (e.g., 2025_08.db for August 2025)
        month_year = date.strftime("%Y_%m")
        db_name = f"{month_year}.db"
        return os.path.join(self.base_dir, db_name)
    
    def get_manager(self, date=None):
        """Get a SQLiteManager for the appropriate month's database"""
        db_path = self.get_db_path(date)
        return SQLiteManager(db_path)
    
if __name__ == '__main__':
    # Initialize the MonthlyDatabaseManager
    month_db = MonthlyDatabaseManager('market_data')

    current_date_time = datetime.now()

    # Get manager for current month
    current_month_db = month_db.get_manager(current_date_time)
    
    # Create table if it doesn't exist
    create_tables(month_db.get_db_path(current_date_time)) 
   