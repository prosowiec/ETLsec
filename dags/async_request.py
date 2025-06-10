from scraper import get_SEC_filings, get_companyfacts, get_StockPrices
import traceback
import asyncio

async def async_get_companyfacts(cik, ticker):
    return await asyncio.to_thread(get_companyfacts, cik, ticker)

async def async_get_SEC_filings(cik, ticker):
    return await asyncio.to_thread(get_SEC_filings, cik, ticker)

async def async_get_StockPrices(ticker, minPeriod, maxPeriod):
    return await asyncio.to_thread(get_StockPrices, ticker, minPeriod, maxPeriod)

async def get_fact_async(cik, ticker):

    compFacts_th, filings_th = await asyncio.gather(
        async_get_companyfacts(cik, ticker), async_get_SEC_filings(cik, ticker) #, async_get_StockPrices(ticker)
    )

    resDic = {'SEC_filings':filings_th, 'company_facts':compFacts_th}
    
    return resDic

async def get_prices_async(ticker, minPeriod, maxPeriod):

    try:
        prices = await asyncio.gather(
            async_get_StockPrices(ticker, minPeriod, maxPeriod) 
        )
        prices_dic = {"stock_prices" : prices[0]}
    except Exception as e:
        print("Error while proccesing", ticker)
        print(e)
        traceback.print_exc()
        prices_dic = {"stock_prices" : "error"}
        
    return prices_dic




if __name__=="__main__":
    pass