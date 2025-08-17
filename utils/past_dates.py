from datetime import datetime, timedelta
from custom_logger import *

def get_nth_month_first_date(n):
    current_date = datetime.now()
    target_month = current_date.month + n
    year_adjustment = (target_month - 1) // 12
    target_year = current_date.year + year_adjustment
    target_month_within_year = (target_month - 1) % 12 + 1
    first_day_of_target_month = datetime(
        target_year,
        target_month_within_year,
        1,
        current_date.hour,
        current_date.minute,
        current_date.second,
        current_date.microsecond
    )
    return first_day_of_target_month

def get_past_date_symbols(symbol, num_months):
    result = []
    today = datetime.now()

    for i in range(num_months):
        # Calculate start and end dates for each month
        end_date = today.replace(day=1) - timedelta(days=1)
        start_date = end_date.replace(day=1)

        # Format the dates as per your requirement
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")

        # Create the symbol using the last three characters of the month abbreviation and last two digits of the year
        symbol_date_str = end_date.strftime("%b%y").upper()
        symbol_str = f"{symbol}{symbol_date_str}FUT"

        # Add the result to the list
        result.append({
            "start_date": start_date_str,
            "end_date": end_date_str,
            "symbol": symbol_str,
            "month" : symbol_date_str
        })

        # Move to the previous month
        today = start_date
    return result

def get_past_dates(num_months):
    today = datetime.now()
    return get_past_date_symbols(today, num_months)

def group_dates_by_span(dates, numOfMonths):
    if(numOfMonths <= 1) :
        return dates
    result = []
    for i in range(0, len(dates), numOfMonths):
        start_date = datetime.strptime(dates[i]['start_date'], '%Y-%m-%d')        
        if i + 1 < len(dates):
            end_date = datetime.strptime(dates[i + 1]['end_date'], '%Y-%m-%d')
        else:
            end_date = start_date + (numOfMonths * timedelta(days=32))
            end_date = end_date.replace(day=1) - timedelta(days=1)        
        result.append({'start_date': start_date.strftime("%Y-%m-%d"), 'end_date': end_date.strftime("%Y-%m-%d"), 'month' : start_date.strftime("%b%y").upper() + "-" + end_date.strftime("%b%y").upper() })    
    return result
def get_past_dates(start_date, num_months):  
    today = start_date
    result = []
    result.append({
        "start_date": today.replace(day=1).strftime("%Y-%m-%d"),
        "end_date": today.strftime("%Y-%m-%d"),
        "month": today.strftime("%b%y").upper()
    })   
    if num_months <= 1:
        return result

    for i in range(num_months):  # Add 1 to include the current month
        # Calculate start and end dates for each month
        end_date = today.replace(day=1) - timedelta(days=1)
        start_date = end_date.replace(day=1)

        # Format the dates as per your requirement
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")

        # Create the symbol using the last three characters of the month abbreviation and last two digits of the year
        symbol_date_str = end_date.strftime("%b%y").upper()

        # Add the result to the list
        result.append({
            "start_date": start_date_str,
            "end_date": end_date_str,
            "month" : symbol_date_str
        })

        # Move to the previous month
        today = start_date

    return result

# Define your process method
def do_process(entry):
    # Replace this with your actual processing logic
    print(f"Custom processing data for {entry['month']} - {entry['start_date']} to {entry['end_date']}")

def process_all1(result, process_method):
    for entry in result:
        process_method(entry)

if __name__ == '__main__':
    # Example usage
    current_date = get_nth_month_first_date(0) #datetime.now()
    start_date = datetime(current_date.year, current_date.month, 1) - timedelta(days=1)
    dates = get_past_dates(start_date, 6)   
    process_all1(dates, do_process)