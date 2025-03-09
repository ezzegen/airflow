from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pendulum

def get_data_from_db(**kwargs):
    connection = psycopg2.connect(user="airflow",
                password="airflow",
                host="172.18.0.3",
                port="5432",
                database="postgres")

       # Выполнение запроса и извлечение данных
    with connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM airflow_users;")
            result = cursor.fetchall()  # Получение всех данных
            print('YES', result)

       # Передача результатов через XCom
    connection.close()

dag = DAG(
       'connect_db',
       schedule_interval='@daily',  # Например, запускается раз в день
       start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),  # Убедитесь, что start_date установлен
       catchup=False  # Это предотвратит выполнение пропущенных запусков
   )

get_data_task = PythonOperator(
       task_id='get_data_task',
       python_callable=get_data_from_db,
       provide_context=True,
       dag=dag,
   )
