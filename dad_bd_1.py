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

dag = DAG('bd_to_bd',
          default_args=default_args,
          description='Process flight delays data',
          schedule_interval=None,
          start_date=datetime(2024, 5, 11),
          catchup=False)

def process_flight_delays():
    # Загрузка данных из файла flight_delays.csv
    flight_delays_df = pd.read_csv('/opt/airflow/dags/flight_delays.csv')

    # Загрузка данных из файла Month.csv
    month_df = pd.read_csv('/opt/airflow/dags/Month.csv', sep=';', header=None, names=['Month', 'Month_Name'], index_col='Month', encoding='cp1252')

    # Преобразование кодов месяцев в соответствующие названия из Month.csv
    flight_delays_df['Month'] = flight_delays_df['Month'].map(month_df['Month_Name'])

    # Загрузка данных из файла DayOfWeek.csv
    day_of_week_df = pd.read_csv('/opt/airflow/dags/DayOfWeek.csv', sep=';', header=None, names=['DayOfWeek', 'DayOfWeek_Name'], index_col='DayOfWeek', encoding='cp1252')

    # Преобразование кодов дней недели в соответствующие названия из DayOfWeek.csv
    flight_delays_df['DayOfWeek'] = flight_delays_df['DayOfWeek'].map(day_of_week_df['DayOfWeek_Name'])

    # Сохранение в новый CSV файл
    flight_delays_df.to_csv('/opt/airflow/dags/merged_flight_delays.csv', index=False)

    # Вывод результата
    print(flight_delays_df)

process_flight_delays_task = PythonOperator(
    task_id='process_flight_delays',
    python_callable=process_flight_delays,
    dag=dag,
)

process_flight_delays_task
