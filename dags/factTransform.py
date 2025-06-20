from scraper import generate_CIK_TICKER, get_spy500_formWiki, \
    fillTo10D, get_SEC_filings, get_companyfacts, get_StockPrices
from tqdm import tqdm
import pandas as pd
from multiprocessing.pool import ThreadPool
import asyncio
import time
import multiprocessing
import numpy as np
from async_request import get_fact_async,get_companyfacts,get_prices_async 
from ingestion import get_SEC_filings_and_companyfacts
from stockPriceDim import classify_change
import regex as re

sec_xbrl_tags = {
    "Assets": [
        "Assets",
        "AssetsCurrent",
        "AssetsNoncurrent",
        "AssetsHeldForSale"
    ],
    
    "CashAndCashEquivalents": [
        "CashAndCashEquivalentsAtCarryingValue",
        "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
        "CashAndCashEquivalents"
    ],
    
    "EarningsPerShareBasic": [
        "EarningsPerShareBasic",
        "BasicEarningsLossPerShare"
    ],
    
    "GrossProfit": [
        "GrossProfit",
        "GrossProfitLoss"
    ],
    
    "Liabilities": [
        "Liabilities",
        "LiabilitiesCurrent",
        "LiabilitiesNoncurrent",
        "LiabilitiesHeldForSale"
    ],
    
    "NetIncomeLoss": [
        "NetIncomeLoss",
        "ProfitLoss",
        "NetIncomeLossAvailableToCommonStockholdersBasic"
    ],
    
    "OperatingCashFlow": [
        "NetCashProvidedByUsedInOperatingActivities",
        "NetCashProvidedByUsedInOperatingActivitiesIndirect",
        "NetCashProvidedByUsedInOperatingActivitiesContinuing"
    ],
    
    "Revenues": [
        "Revenues",
        "SalesRevenueNet",
        "SalesRevenueServicesNet",
        "SalesRevenueGoodsNet",
        "RevenueFromContractWithCustomerExcludingAssessedTax"
    ],
    
    "SharesOutstanding": [
        "SharesOutstanding",
        "CommonStockSharesOutstanding"
    ],
    
    "StockholdersEquity": [
        "StockholdersEquity",
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
        "StockholdersEquityIncludingPortionAttributableToParent"
    ],
    
    "EBITDA": [
        "EarningsBeforeInterestTaxesDepreciationAndAmortization",
        "EBITDA",
        "OperatingIncomeLossBeforeDepreciationAndAmortization",
        "OperatingIncomeLossBeforeDepreciationAmortizationAndImpairment",
        "OperatingProfitLossBeforeDepreciationAndAmortization"
    ]
}


def map_Fintype_to_lower_Hierarchy(df, sec_xbrl_tags):
    reverse_map = {}

    for key, val in sec_xbrl_tags.items():
        if isinstance(val, list):
            for item in val:
                reverse_map[item] = key
        else:
            reverse_map[val] = key

    # Funkcja mapująca pojedynczą wartość na klucz nadrzędny
    def map_to_parent(fin_type):
        return reverse_map.get(fin_type, fin_type)  # jeśli brak w mapie, zwróć oryginał
    
    df['finType'] = df['finType'].apply(map_to_parent)
    
    return df


def clean_company_facts(company_facts):
    # Usunięcie duplikatów
    company_facts_filtered = company_facts[company_facts['finType'].isin(sec_xbrl_tags)].copy()
    company_facts_filtered.sort_values(by=['yearMonthDay', 'val'], inplace=True, ascending=False)
    
    company_facts_filtered['endDate'] = pd.to_datetime(company_facts_filtered['yearMonthDay'], format='%Y%m%d')
    #company_facts_filtered = company_facts_filtered.dropna(subset=['frame'])
    # company_facts_filtered = company_facts_filtered[
    #     (company_facts_filtered['frame'].notna()) | 
    #     (company_facts_filtered['finType'] == 'OperatingCashFlow')
    # ]
    
    company_facts_filtered = company_facts_filtered[['finType','end','val','accn','fp','filed','start','ticker', 'endDate','frame']].reset_index(drop=True)
    #company_facts_filtered.sort_values(by=['val'], inplace=True)
    
    return company_facts_filtered

def pivot_company_facts(company_facts_filtered):
    # Pivotowanie danych, aby uzyskać kolumny dla każdego typu finansowego
    company_facts_to_pivot = company_facts_filtered.fillna("NotAvailable")

    
    company_facts_to_pivot = company_facts_to_pivot[['ticker', 'accn', 'fp', 'filed','val','finType', 'endDate']]
    company_facts_to_pivot['filed'] = pd.to_datetime(company_facts_to_pivot['filed'], format='%Y-%m-%d')
    company_facts_to_pivot['endDate'] = pd.to_datetime(company_facts_to_pivot['endDate'], format='%Y-%m-%d')
    #company_facts_filtered = company_facts_filtered[company_facts_filtered['frame'].str.contains(company_facts_to_pivot['endDate'].dt.year)]
    #company_facts_to_pivot = company_facts_to_pivot[company_facts_to_pivot['endDate'].dt.year == company_facts_to_pivot['filed'].dt.year]        
        
    company_facts_pivot = company_facts_to_pivot.pivot_table(index=['ticker', 'accn', 'fp', 'filed', 'endDate'], columns='finType', values='val', aggfunc='first').reset_index()
    company_facts_pivot.rename(columns={'accn': 'accessionNumber', 'filed' : 'filingDate', 'fp':'financialPeriod'}, inplace=True)
    
    financial_columns = [
    'Assets', 'CashAndCashEquivalents', 'EarningsPerShareBasic',
    'GrossProfit', 'Liabilities', 'NetIncomeLoss', 'OperatingCashFlow',
    'Revenues', 'SharesOutstanding', 'StockholdersEquity'
    ]

    # Zliczamy liczbę brakujących wartości w każdej z wybranych kolumn
    company_facts_pivot['missing_count'] = company_facts_pivot[financial_columns].isna().sum(axis=1)

    # Usuwamy te wiersze, gdzie braków jest co najmniej 5
    company_facts_pivot = company_facts_pivot[company_facts_pivot['missing_count'] < 4].drop(columns=['missing_count'])

    
    return company_facts_pivot



def merge_company_facts_with_sec_filings(company_facts_pivot, sec_filling):
    # Łączenie danych z company_facts_pivot i sec_filling
    sec_filling['filingDate'] = pd.to_datetime(sec_filling['filingDate'], format='%Y-%m-%d')
    sec_filling['reportDate'] = pd.to_datetime(sec_filling['reportDate'], format='%Y-%m-%d')
    sec_filling_to_merge = sec_filling[['accessionNumber', 'reportDate', 'form', 'filingDate', 'primaryDocDescription','fileURL']].copy()
    joined_df = pd.merge(sec_filling_to_merge, company_facts_pivot, on=['accessionNumber','filingDate'], how='inner')


    joined_df = joined_df[joined_df['endDate'].dt.year == joined_df['reportDate'].dt.year]


    return joined_df

def normalize_to_quarters(company_facts_with_sec_filings):
    value_cols = [
        'GrossProfit', 'NetIncomeLoss', 'OperatingCashFlow', 'Revenues'
    ]

    period_order = {'Q1': 1, 'Q2': 2, 'Q3': 3, 'FY': 4}
    df = company_facts_with_sec_filings.copy()
    df[df.columns[df.columns.get_loc('reportDate')]] = pd.to_datetime(df['reportDate'])
    df.loc[:, 'year'] = df['reportDate'].dt.year
    df.loc[:, 'periodOrder'] = df['financialPeriod'].map(period_order)


    normalized_df = pd.DataFrame()
    for ticker in company_facts_with_sec_filings['ticker'].unique():
        df = company_facts_with_sec_filings[company_facts_with_sec_filings['ticker'] == ticker].copy()
        df = df.reset_index()
        for i in range(0, len(df) - 1 ):
            curr = df.iloc[i]
            prev = df.iloc[i + 1]
            if (
                curr['financialPeriod'] in ['Q2', 'Q3', 'FY'] and
                prev['financialPeriod'] in ['Q1', 'Q2', 'Q3']
            ):
                for col in value_cols:
                    if pd.notnull(curr[col]) and pd.notnull(prev[col]):
                        df.at[i, col] = curr[col] - prev[col]
                # print(curr['financialPeriod'], prev['financialPeriod'])
                # print(curr['year'],curr['Revenues'], prev['Revenues'], curr['Revenues'] - prev['Revenues'])

        normalized_df = pd.concat([df,normalized_df])

    normalized_df['financialPeriod'] = normalized_df['financialPeriod'].replace({'FY': 'Q4'})
    
    return normalized_df


def add_stock_price_dim(joined_df):
    joined_df['stockPriceChange'] = joined_df.apply(
        lambda row: classify_change(row['ticker'], end=row['reportDate']), axis=1
    )
    joined_df = joined_df[['accessionNumber','ticker', 'reportDate','filingDate','endDate', 'form','financialPeriod','stockPriceChange',
       'Assets', 'CashAndCashEquivalents', 'EarningsPerShareBasic',
       'GrossProfit', 'Liabilities', 'NetIncomeLoss', 'OperatingCashFlow',
       'Revenues', 'SharesOutstanding', 'StockholdersEquity','fileURL']]

    return joined_df    

def analyze_transcript(text):
    positive_words = ["growth", "profit", "strong", "increase", "record", "improvement", "innovation"]
    negative_words = ["decline", "loss", "weak", "decrease", "problem", "challenge", "risk"]

    try:
        # Obsługa brakujących danych
        if pd.isna(text) or not isinstance(text, str) or text.strip() == "":
            return [0, 0, 0, 0, 0.0]

        text_clean = text.lower().strip()
        words = re.findall(r'\b\w+\b', text_clean)
        total_tokens = len(words)

        # Liczenie słów z list
        positive_count = sum(1 for word in words if word in positive_words)
        negative_count = sum(1 for word in words if word in negative_words)

        # Obliczanie wyników
        sentiment_score = positive_count - negative_count + total_tokens * 0.0001
        trans_length = len(text_clean)

        return [trans_length, total_tokens, positive_count, negative_count, sentiment_score]

    except Exception as e:
        print(f"Błąd przetwarzania tekstu: {str(e)}")
        return [0, 0, 0, 0, 0.0]


def add_transcript_info(df):
    df["TransLength"] = 0
    df["TotalTokens"] = 0
    df["PositiveWordCount"] = 0
    df["NegativeWordCount"] = 0
    df["SentimentScore"] = 0.0

    # Wypełnianie kolumn danymi
    for i, row in df.iterrows():
        result = analyze_transcript(row["transcript"])
        df.at[i, "TransLength"] = result[0]
        df.at[i, "TotalTokens"] = result[1]
        df.at[i, "PositiveWordCount"] = result[2]
        df.at[i, "NegativeWordCount"] = result[3]
        df.at[i, "SentimentScore"] = result[4]
        df.at[i, "transcriptID"] = df.at[i, "ticker"]+'Q'+df.at[i, "quarter"].astype(str) + df.at[i, "year"].astype(str)
    
    df = df[[
        "transcriptID",
        "TransLength", "TotalTokens", "PositiveWordCount", 
        "NegativeWordCount", "SentimentScore"
    ]]
    return df


if __name__ == "__main__":
    pass