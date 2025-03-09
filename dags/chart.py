import pandas as pd
import numpy as np
import sklearn.linear_model as skl
import matplotlib.pyplot as plt
from airflow import DAG
from airflow.decorators import dag, task
import pendulum

@dag(
    dag_id='chart',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule_interval='@once',
    catchup=False
)
def chart_dag():

    @task
    def get_data_and_chart():
        data = pd.read_excel('/opt/airflow/data/linear_func.xlsx')
        X = data['X'].to_numpy().reshape(-1, 1)
        y = data['Y'].to_numpy().reshape(-1, 1)
        plt.figure(figsize=(5, 5))
        plt.grid()
        plt.plot(X, y)
        plt.xlabel('X')
        plt.ylabel('Y')
        plt.title('Линейная зависимость')
        # Сохранение графика в файл
        plt.savefig('/opt/airflow/downloads/linear_plot.png')
        plt.close()  # Закрыть фигуру после сохранения, чтобы освободить память
        
        
    get_data_and_chart()

# Создаем экземпляр DAG
dag_instance = chart_dag()
