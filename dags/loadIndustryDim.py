from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine

from scraper import get_spy500_formWiki


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

def extract_IndustryDim(**kwargs):
    ti = kwargs['ti']
    tickers = ["MSFT", "GOOGL", "AAPL", "AMZN", "META", "TSLA", "NFLX", "NVDA", "AMD", "INTC"]
    spytable = get_spy500_formWiki()
    spytable = spytable[spytable['Symbol'].isin(tickers)]

    print(spytable.head())
    ti.xcom_push(key='spytable', value=spytable)
    
def load_IndustryDim(**kwargs):
    ti = kwargs['ti']
    result_df = ti.xcom_pull(key='spytable', task_ids='extract_IndustryDim')
    hook = PostgresHook(postgres_conn_id='sec_postgres')
    engine = hook.get_sqlalchemy_engine()

    result_df.to_sql(
        name='dimIndustry',
        con=engine,
        index=False,
        if_exists='replace',
        schema='stockDB',
        method='multi'  
    )


        # ti = kwargs['ti']
        # trascript_df = ti.xcom_pull(key='transcript_df', task_ids='concat_transcripts')
        # result_df = ti.xcom_pull(key='result_df', task_ids='concat_transcripts')
        # # print(trascript_df.head())
        # # print(result_df.head())
        # result_df['transcriptID'] = result_df['ticker'] + result_df['financialPeriod'].astype(str) + result_df['reportDate'].str[0:4]
        # enriched_df = pd.merge(
        #     result_df, 
        #     trascript_df, 
        #     on='transcriptID', 
        #     how='left'
        # )
        
        # #??
        # ti.xcom_push(key='enriched_df', value=enriched_df)


with DAG(
    dag_id='loadIndustryDim',
    default_args=default_args,
    #schedule_interval=None,
    catchup=False,
    tags=['test'],
) as dag:

    read_csv_task = PythonOperator(
        task_id='extract_IndustryDim',
        python_callable=extract_IndustryDim,
    )
    load_to_db = PythonOperator(
        task_id='load_to_db',
        python_callable=load_IndustryDim,
    )
    
    read_csv_task >> load_to_db
