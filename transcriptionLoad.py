import os
import requests
import pandas as pd

def fetch_earnings_transcript(ticker: str, year: int, quarter: int) -> dict:
    """
    Pobiera transkrypt wyników kwartalnych dla podanego tickera.
    Zwraca JSON lub zgłasza wyjątek.
    """
    url = "https://api.api-ninjas.com/v1/earningstranscript"
    params = {
        "ticker": ticker,
        "year": year,
        "quarter": quarter
    }
    api_key = "XXX" #os.getenv("API_NINJAS_KEY")  # klucz w zmiennej środowiskowej
    if not api_key:
        raise ValueError("Brakuje klucza API. Ustaw zmienną środowiskową API_NINJAS_KEY")
    
    headers = {"X-Api-Key": api_key}
    resp = requests.get(url, headers=headers, params=params, timeout=10)
    resp.raise_for_status()  # wyjątek jeśli kod != 200
    return resp.json()

def save_json(data: dict, filename: str):
    import json
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        
        
tickers = ["MSFT",  "GOOGL", "AAPL", "AMZN", "META", "TSLA", "NFLX", "NVDA", "AMD", "INTC"]
years = [2024,2025]
quarters = [1,2,3,4]

for ticker in tickers:
    ticker_csv = pd.DataFrame([{
                "ticker": None,
                "year": None,
                "quarter": None,
                "transcriptID": None,
                "transcript": None,
                "date": None
            }])
    for year in years:
        for quarter in quarters:
            #if quarter == 4 and year == 2025:
            #    break  # nie ma jeszcze transkryptów dla Q3 i Q4 2025
            transcriptID = f"{year}-Q{quarter}-{ticker}"
            print(f"Pobieranie transkryptu dla {ticker} Q{quarter} {year}")
            transcript = fetch_earnings_transcript(ticker, year, quarter)
            #print(transcript)
            temp_df = pd.DataFrame([{
                "ticker": ticker,
                "year": year,
                "quarter": quarter,
                "transcriptID": transcriptID,
                "transcript": None if type(transcript) ==list else transcript["transcript"],
                "date": None if type(transcript) ==list else transcript["date"]
            }])
            ticker_csv = pd.concat([ticker_csv, temp_df], ignore_index=True)
    
    ticker_csv.dropna(axis=0, inplace=True)
    ticker_csv.to_csv(f"transcriptData/transcripts_{ticker}.csv", index=False, encoding="utf-8")
