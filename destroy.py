from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import DagBag
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('integration_flow',
          default_args=default_args,
          description='Integration flow for Apache Airflow',
          schedule_interval=None,
          start_date=datetime(2024, 5, 11),
          catchup=False)

def process_integration_flow():

    flight_delays_params = pd.read_csv('/opt/airflow/dags/FLIGHT_DELAYS.csv')

    dagbag = DagBag()
    dag_bd_dag = dagbag.get_dag('bd_to_bd')
    process_flight_delays_task = dag_bd_dag.get_task('process_flight_delays')
    merged_flight_delays_path = process_flight_delays_task.execute(context={'params': flight_delays_params})

    merged_flight_delays_df = pd.read_csv(merged_flight_delays_path)

    xml_data = merged_flight_delays_df.to_xml()

    xml_data_path = '/opt/airflow/dags/flight_delays_params.xml'
    with open(xml_data_path, 'w') as f:
        f.write(xml_data)

    print("Integration flow completed. XML data saved to:", xml_data_path)

process_integration_flow_task = PythonOperator(
    task_id='process_integration_flow',
    python_callable=process_integration_flow,
    dag=dag,
)

process_integration_flow_task
