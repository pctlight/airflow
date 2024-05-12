from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('bd_to_bd_1',
          default_args=default_args,
          description='Process flight delays data',
          schedule_interval=None,
          start_date=datetime(2024, 5, 11),
          catchup=False)

def process_flight_delays():
    # Çàãðóçêà äàííûõ èç ôàéëà flight_delays.csv
    flight_delays_df = pd.read_csv('/opt/airflow/dags/flight_delays.csv')

    # Çàãðóçêà äàííûõ èç ôàéëà Month.csv
    month_df = pd.read_csv('/opt/airflow/dags/Month.csv', sep=';', header=None, names=['Month', 'Month_Name'], index_col='Month', encoding='cp1252')

    # Ïðåîáðàçîâàíèå êîäîâ ìåñÿöåâ â ñîîòâåòñòâóþùèå íàçâàíèÿ èç Month.csv
    flight_delays_df['Month'] = flight_delays_df['Month'].map(month_df['Month_Name'])

    # Çàãðóçêà äàííûõ èç ôàéëà DayOfWeek.csv
    day_of_week_df = pd.read_csv('/opt/airflow/dags/DayOfWeek.csv', sep=';', header=None, names=['DayOfWeek', 'DayOfWeek_Name'], index_col='DayOfWeek', encoding='cp1252')

    # Ïðåîáðàçîâàíèå êîäîâ äíåé íåäåëè â ñîîòâåòñòâóþùèå íàçâàíèÿ èç DayOfWeek.csv
    flight_delays_df['DayOfWeek'] = flight_delays_df['DayOfWeek'].map(day_of_week_df['DayOfWeek_Name'])

    # Ñîõðàíåíèå â íîâûé CSV ôàéë
    flight_delays_df.to_csv('/opt/airflow/dags/merged_flight_delays.csv', index=False)

    # Âûâîä ðåçóëüòàòà
    print(flight_delays_df)

process_flight_delays_task = PythonOperator(
    task_id='process_flight_delays',
    python_callable=process_flight_delays,
    dag=dag,
)

process_flight_delays_task
