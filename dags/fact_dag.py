from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

from factTransform import (
    get_SEC_filings_and_companyfacts,
    map_Fintype_to_lower_Hierarchy,
    clean_company_facts,
    pivot_company_facts,
    merge_company_facts_with_sec_filings,
    add_stock_price_dim,
    add_transcript_info,
    sec_xbrl_tags
)

import pandas as pd

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Global variable to pass between tasks (if using XCom instead, adapt this logic)
global_data = {}

with DAG(
    dag_id='sec_stock_price_etl',
    default_args=default_args,
    description='ETL pipeline for SEC filings + stock price change classification',
    #schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['SEC', 'stock', 'ETL'],
) as dag:

    def extract_data(**kwargs):
        tickers = ["MSFT", "GOOGL", "AAPL", "AMZN", "META", "TSLA", "NFLX", "NVDA", "AMD", "INTC"]
        sec_filling, company_facts = get_SEC_filings_and_companyfacts(tickers)
        kwargs['ti'].xcom_push(key='sec_filling', value=sec_filling)
        kwargs['ti'].xcom_push(key='company_facts', value=company_facts)

    def transform_hierarchy(**kwargs):
        ti = kwargs['ti']
        df = ti.xcom_pull(key='company_facts', task_ids='extract_sec_data')
        transformed_df = map_Fintype_to_lower_Hierarchy(df, sec_xbrl_tags)
        ti.xcom_push(key='company_facts_transformed', value=transformed_df)

    def filter_facts(**kwargs):
        ti = kwargs['ti']
        df = ti.xcom_pull(key='company_facts_transformed', task_ids='transform_fintype_hierarchy')
        filtered_df = clean_company_facts(df)
        ti.xcom_push(key='company_facts_filtered', value=filtered_df)

    def pivot_facts(**kwargs):
        ti = kwargs['ti']
        df = ti.xcom_pull(key='company_facts_filtered', task_ids='filter_company_facts')
        pivot_df = pivot_company_facts(df)
        ti.xcom_push(key='company_facts_pivot', value=pivot_df)

    def join_facts(**kwargs):
        ti = kwargs['ti']
        pivot_df = ti.xcom_pull(key='company_facts_pivot', task_ids='pivot_company_facts')
        sec_filling = ti.xcom_pull(key='sec_filling', task_ids='extract_sec_data')
        joined_df = merge_company_facts_with_sec_filings(pivot_df, sec_filling)
        ti.xcom_push(key='joined_df', value=joined_df)

    def enrich_with_stock_price(**kwargs):
        ti = kwargs['ti']
        df = ti.xcom_pull(key='joined_df', task_ids='join_sec_with_company_facts')
        result_df = add_stock_price_dim(df)
        #global_data['joined_df'] = result_df
        ti.xcom_push(key='result_df', value=result_df)
        #result_df.to_csv('/tmp/SEC_stock_facts_final.csv', index=False)
        #print(result_df.head())
        
    def concat_transcripts(**kwargs):
        ti = kwargs['ti']
        result_df = ti.xcom_pull(key='result_df', task_ids='add_stock_price_change')
        TRANSCRIPTS_DIR = '/opt/airflow/transcriptData'
        all_files = [
            os.path.join(TRANSCRIPTS_DIR, f)
            for f in os.listdir(TRANSCRIPTS_DIR)
            if f.endswith('.csv')
        ]

        if not all_files:
            print("❌ Brak plików CSV w folderze.")
            return

        df_list = []
        for file in all_files:
            print(f"📄 Wczytywanie: {file}")
            try:
                df = pd.read_csv(file)
                df['source_file'] = os.path.basename(file)  # Dodaj nazwę pliku jako kolumnę
                df_list.append(df)
            except Exception as e:
                print(f"⚠️ Błąd przy pliku {file}: {e}")

        if df_list:
            df_concat = pd.concat(df_list, ignore_index=True)
        
        trascript_df = add_transcript_info(df_concat)
        print(trascript_df)
        ti.xcom_push(key='transcript_df', value=trascript_df)
        ti.xcom_push(key='result_df', value=result_df)


    def join_transcripts(**kwargs):
        ti = kwargs['ti']
        trascript_df = ti.xcom_pull(key='transcript_df', task_ids='concat_transcripts')
        result_df = ti.xcom_pull(key='result_df', task_ids='concat_transcripts')
        # print(trascript_df.head())
        # print(result_df.head())
        result_df['transcriptID'] = result_df['ticker'] + result_df['financialPeriod'].astype(str) + result_df['reportDate'].str[0:4]
        enriched_df = pd.merge(
            result_df, 
            trascript_df, 
            on='transcriptID', 
            how='left'
        )
        
        #??
        ti.xcom_push(key='enriched_df', value=enriched_df)

    def validate_data(**kwargs):
        ti = kwargs['ti']
        df = ti.xcom_pull(key='enriched_df', task_ids='join_transcripts')

        errors = []

        # Sprawdź brakujące wartości w kluczowych kolumnach
        required_columns = ['ticker', 'reportDate', 'filingDate', 'financialPeriod', 'stockPriceChange']
        for col in required_columns:
            if df[col].isnull().any():
                errors.append(f"🚫 Brakujące wartości w kolumnie: {col}")

        # Sprawdź czy daty mają odpowiedni format
        try:
            pd.to_datetime(df['reportDate'])
            pd.to_datetime(df['filingDate'])
        except Exception as e:
            errors.append(f"Błąd konwersji dat: {e}")

        allowed_classes = [x for x in range(0, 8)]
        if not df['stockPriceChange'].isin(allowed_classes).all():
            errors.append("Niekatologowane wartości w kolumnie 'stockPriceChange'")

        # Sprawdź duplikaty po ticker+financialPeriod+reportDate
        if df.duplicated(subset=['ticker', 'financialPeriod', 'reportDate']).any():
            errors.append("Duplikaty rekordów po ticker + financialPeriod + reportDate")

        if errors:
            print("\n".join(errors))
            raise ValueError("Walidacja danych nie powiodła się. Zobacz szczegóły powyżej.")
        else:
            print("Walidacja danych zakończona sukcesem.")

    def store_validation_table(**kwargs):
        ti = kwargs['ti']
        df = ti.xcom_pull(key='enriched_df', task_ids='join_transcripts')

        financial_columns = [
            "stockPriceChange", "Assets", "CashAndCashEquivalents",
            "EarningsPerShareBasic", "GrossProfit", "Liabilities",
            "NetIncomeLoss", "OperatingCashFlow", "Revenues",
            "SharesOutstanding", "StockholdersEquity"
        ]

        missing_data_df = df[df[financial_columns].isnull().any(axis=1)]

        # Wybierz tylko kolumny do walidacji + unikalność
        #validation_df = missing_data_df[["ticker", "reportDate", "filingDate", "accessionNumber"]].drop_duplicates()

        validation_df = missing_data_df[["ticker", "reportDate", "filingDate", "accessionNumber"] + financial_columns].copy()

       # Zamień wartości na True (brak) / False (obecność danych)
        for col in financial_columns:
            validation_df[col] = validation_df[col].isnull()

        validation_df.drop_duplicates(inplace=True)

        # Zapisz do PostgreSQL
        hook = PostgresHook(postgres_conn_id='sec_postgres')
        engine = hook.get_sqlalchemy_engine()

        validation_df.to_sql(
            name='ValidationMissingData',
            con=engine,
            index=False,
            if_exists='replace',  # Możesz zmienić na 'append' jeśli chcesz tworzyć historię
            schema='stockDB',
            method='multi'
        )

        print("Tabela walidacji 'ValidationMissingData' została zapisana w bazie.")

        
    def write_to_postgres_sqlalchemy(**kwargs):
        #df = global_data['joined_df']  
        #df_final = add_stock_price_dim(df)
        df_final = kwargs['ti'].xcom_pull(key='enriched_df', task_ids='join_transcripts')
        # Upewnij się, że daty są w dobrym formacie
        df_final['reportDate'] = pd.to_datetime(df_final['reportDate']).dt.date
        df_final['filingDate'] = pd.to_datetime(df_final['filingDate']).dt.date

        hook = PostgresHook(postgres_conn_id='sec_postgres')
        engine = hook.get_sqlalchemy_engine()

        df_final.to_sql(
            name='FillingsFact',
            con=engine,
            index=False,
            if_exists='replace',
            schema='stockDB',
            method='multi'  
        )

        print("✅ DataFrame zapisany do PostgreSQL przez SQLAlchemy.")
    
    task_extract = PythonOperator(
        task_id='extract_sec_data',
        python_callable=extract_data,
    )

    task_transform = PythonOperator(
        task_id='transform_fintype_hierarchy',
        python_callable=transform_hierarchy,
    )

    task_filter = PythonOperator(
        task_id='filter_company_facts',
        python_callable=filter_facts,
    )

    task_pivot = PythonOperator(
        task_id='pivot_company_facts',
        python_callable=pivot_facts,
    )

    task_join = PythonOperator(
        task_id='join_sec_with_company_facts',
        python_callable=join_facts,
    )

    task_enrich = PythonOperator(
        task_id='add_stock_price_change',
        python_callable=enrich_with_stock_price,
    )
    
    task_load_trascripts = PythonOperator(
        task_id='concat_transcripts',
        python_callable=concat_transcripts,
    )
    
    task_join_trascripts = PythonOperator(
        task_id='join_transcripts',
        python_callable=join_transcripts,
    )
    
    task_sqlalchemy_export = PythonOperator(
        task_id='export_to_postgres_sqlalchemy',
        python_callable=write_to_postgres_sqlalchemy,
    )
    
    task_validate = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
    )
    task_store_validation = PythonOperator(
        task_id='store_validation_table',
        python_callable=store_validation_table,
    )

    task_extract >> task_transform >> task_filter >> task_pivot >> task_join >> task_enrich >> task_load_trascripts >> task_join_trascripts \
    >> task_validate >> task_store_validation >>task_sqlalchemy_export
