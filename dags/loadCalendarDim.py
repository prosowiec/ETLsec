from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

def extract_dimCalendar(**kwargs):
    start_date = datetime(2010, 1, 1)
    end_date = datetime(2030, 12, 31)

    date_range = pd.date_range(start=start_date, end=end_date)
    calendar_df = pd.DataFrame({
        'date': date_range.strftime('%Y-%m-%d'),
        'day': date_range.day,
        'month': date_range.month,
        'year': date_range.year,
        'day_of_week': date_range.dayofweek,
        'day_name': date_range.day_name(),
        'month_name': date_range.month_name(),
        'week': date_range.isocalendar().week,
        'quarter': date_range.quarter,
        'is_weekend': date_range.dayofweek >= 5,
    })

    ti = kwargs['ti']
    ti.xcom_push(key='calendar_df', value=calendar_df)

def load_dimCalendar(**kwargs):
    ti = kwargs['ti']
    calendar_df = ti.xcom_pull(key='calendar_df', task_ids='extract_dimCalendar')

    hook = PostgresHook(postgres_conn_id='sec_postgres')
    engine = hook.get_sqlalchemy_engine()

    calendar_df.to_sql(
        name='dimCalendar',
        con=engine,
        index=False,
        if_exists='replace',
        schema='stockDB',
        method='multi'
    )

with DAG(
    dag_id='loadCalendarDim',
    default_args=default_args,
    description='Generate and load dimCalendar into PostgreSQL',
    catchup=False,
    tags=['dim', 'calendar'],
) as dag:

    task_extract_calendar = PythonOperator(
        task_id='extract_dimCalendar',
        python_callable=extract_dimCalendar,
    )

    task_load_calendar = PythonOperator(
        task_id='load_dimCalendar',
        python_callable=load_dimCalendar,
    )

    task_extract_calendar >> task_load_calendar
