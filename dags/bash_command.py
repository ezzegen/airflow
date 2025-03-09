from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum


with DAG(
    'bash_commands',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule_interval='@once',
    catchup=False
) as dag:
    t1 = BashOperator(
        task_id='print_ls',
        bash_command='ls /opt/airflow/data',
    )
    '''t2 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )
    
    t3 = BashOperator(
        task_id='print_ls',
        bash_command='pwd',
    )
   
    t1 >> t2
    t2 >> t3'''
