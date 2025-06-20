from scraper import get_spy500_formWiki, get_SEC_comanyDescription
from tqdm import tqdm
import pandas as pd
from multiprocessing.pool import ThreadPool
import asyncio
import time
import multiprocessing
import numpy as np
from async_request import get_fact_async,get_companyfacts,get_prices_async 

def get_fact_dataframes_dict(cik, ticker):
    start = time.time()
    res = asyncio.run(get_fact_async(cik, ticker))
    
    print(cik, ticker, 'loaded in', time.time() - start)

    return res

def get_batch_sec(pool, ticketList, start_step, end_step):
    ticker = list(ticketList[start_step:end_step,0])
    ciks = list(ticketList[start_step:end_step,1])
    #batch = pool.starmap(get_fact_dataframes_dict, zip(ciks, ticker))
    batch = [get_fact_dataframes_dict(cik, tick) for cik, tick in zip(ciks, ticker)]
    return batch

def append_to_csv_file(df):
    pass


def get_spy500_batch_sec(ticketList, batchSize = 16):
    """"
    ticketList : numpy array with two columns: ticker and cik
    """
    #pool = multiprocessing.Pool(batchSize)
    batch_control = 1
    start_step = 0
    all_batches = []
    for end_step in range(batchSize, len(ticketList), batchSize):
        #batch = [get_batch_sec(pool, ticketList, start_step, end_step)]
        batch = [get_batch_sec(None, ticketList, start_step, end_step)]
        start_step = end_step
        print('Batch', batch_control, "loaded")
        batch_control +=1
        all_batches.append(batch)
        
    if start_step != len(ticketList):
        #batch = [get_batch_sec(pool, ticketList, start_step, len(ticketList))]
        batch = [get_batch_sec(None, ticketList, start_step, len(ticketList))]
        print('Batch', batch_control, "loaded")
        print("Process has compleded")
        all_batches.append(batch)
        
    #pool.close()
    #pool.join()
    print("Data has loaded")
    print("-------------- Loaded tickers --------------")
    print(ticketList)
    
    return all_batches


def get_SEC_filings_and_companyfacts(tickers):
    """
    returns two dataframes: SEC_filings and company_facts
    """
    spytable = get_spy500_formWiki()
    #tickers = ["MSFT",  "GOOGL", "AAPL", "AMZN", "META", "TSLA", "NFLX", "NVDA", "AMD", "INTC"]
    spytable = spytable[spytable['Symbol'].isin(tickers)]
    spytable = spytable[['Symbol', 'CIK']].values
    print("Spy table loaded with", len(spytable), "tickers")
    all_batches = get_spy500_batch_sec(spytable, 16)
    all_batches = np.array(all_batches).flatten().tolist()
    all_sec_filings_df = pd.DataFrame()
    all_company_facts_df = pd.DataFrame()
    for element in all_batches:
        SEC_filings_df = element['SEC_filings']
        company_facts_df = element['company_facts']
        all_sec_filings_df = pd.concat([all_sec_filings_df, SEC_filings_df], ignore_index=True)
        all_company_facts_df = pd.concat([all_company_facts_df, company_facts_df], ignore_index=True)


    return all_sec_filings_df,all_company_facts_df


def transform_companyDescription(json_api):
    selected_keys = [
        'cik',
        'entityType',
        'sicDescription',
        'ownerOrg',
        'name',
        'tickers',
        'exchanges',
        'description',
        'website',
        'investorWebsite',
        'category',
        'fiscalYearEnd',
        'stateOfIncorporation',
    ]

    # Załóżmy, że masz oryginalny słownik 'data'
    flat_data = {k: v for k, v in json_api.items() if k in selected_keys}

    # Jeśli niektóre pola są listami – zamień je na stringi
    for key, value in flat_data.items():
        if isinstance(value, list):
            flat_data[key] = ', '.join(map(str, value))

    company_df = pd.DataFrame([flat_data])
    return company_df


def get_companyDescription(cik):
    json_api = get_SEC_comanyDescription(cik)
    company_df = transform_companyDescription(json_api)
    return company_df

if __name__ == "__main__":

    spytable = get_spy500_formWiki()
    tickers = ["MSFT",  "GOOGL", "AAPL", "AMZN", "META", "TSLA", "NFLX", "NVDA", "AMD", "INTC"]
    spytable = spytable[spytable['Symbol'].isin(tickers)]
    spytable = spytable[['Symbol', 'CIK']].values
    print("Spy table loaded with", len(spytable), "tickers")
    all_batches = get_spy500_batch_sec(spytable, 16)
    all_batches = np.array(all_batches).flatten().tolist()
    all_sec_filings_df = pd.DataFrame()
    all_company_facts_df = pd.DataFrame()
    for element in all_batches:
        SEC_filings_df = element['SEC_filings']
        company_facts_df = element['company_facts']

        all_sec_filings_df = pd.concat([all_sec_filings_df, SEC_filings_df], ignore_index=True)
        all_company_facts_df = pd.concat([all_company_facts_df, company_facts_df], ignore_index=True)

    all_sec_filings_df.to_csv('all_sec_filings.csv', index=False)
    all_company_facts_df.to_csv('all_company_facts.csv', index=False)
