#from scraper import get_StockPrices
import yfinance as yf
import pandas as pd
from datetime import datetime
import time
def get_StockPrices(ticker, start="2023-01-01", end="2023-10-01", interval="1d"):
    try:
        stock_data = yf.download(ticker, start=start, end=end, interval=interval)
        stock_data.reset_index(inplace=True)
        stock_data['ticker'] = ticker
        #stock_data.rename(columns={'Date': 'date', 'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Adj Close': 'adjClose', 'Volume': 'volume'}, inplace=True)
        return stock_data
    except Exception as e:
        print(f"Error fetching data for {ticker}: {e}")
        return pd.DataFrame()
#joined_df['ticker'].unique().tolist()
#,"%Y-%m-%d

def days_in_month(year, month):
    """
    Zwraca liczbę dni w danym miesiącu.
    """
    from calendar import monthrange
    return monthrange(year, month)[1]

def subtract_quarter(date):
    """
    Zwraca datę o jeden kwartał wcześniej, zachowując dzień miesiąca jeśli możliwe.
    """
    if isinstance(date, str):
        date = datetime.strptime(date, "%Y-%m-%d")

    month = date.month
    year = date.year

    # Oblicz poprzedni kwartał
    new_month = month - 3
    if new_month <= 0:
        new_month += 12
        year -= 1

    # Upewnij się, że dzień nie przekracza liczby dni w nowym miesiącu
    day = min(date.day, days_in_month(year, new_month))

    return datetime(year, new_month, day)


# # Funkcja klasyfikująca zmianę
def classify_change(ticker, end="2023-10-01"):
    df = get_StockPrices(ticker, start=subtract_quarter(end) , end=end)

    pct = ((df['Close'].iloc[0] - df['Close'].iloc[-1]) * 100 / df['Close'].iloc[-1]).values[0]
    # print(diffrence)
    # print(classify_change(diffrence))

    if pd.isna(pct):
        return None
    if pct < -5:
        return 1  # Large negative change
    elif -5 <= pct < -2:
        return 2  # Medium negative change
    elif -2 <= pct < 0:
        return 3  # Small negative change
    elif pct == 0:
        return 4  # No change
    elif 0 < pct <= 2:
        return 5  # Small positive change
    elif 2 < pct <= 5:
        return 6  # Medium positive change
    elif pct > 5:
        return 7  # Large positive change

    time.sleep(0.5)  # To avoid hitting API rate limits
    
    return pct
    
if __name__ == "__main__":
    #print(classify_change("AAPL"))  # Example usage, replace with actual ticker
    pass
