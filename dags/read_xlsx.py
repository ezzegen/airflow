import pandas as pd
import numpy as np
import sklearn.linear_model as skl
import matplotlib.pyplot as plt
from airflow import DAG
from airflow.decorators import dag, task
import pendulum



@dag(
    dag_id='read_data',
    start_date=pendulum.now('Europe/Samara'),
    schedule = None,
    catchup=False
)
def read_data_dag():

    @task()
    def get_data():
        data = pd.read_excel('/opt/airflow/data/linear_func.xlsx')
        X = data['X'].to_numpy().reshape(-1, 1)
        y = data['Y'].to_numpy().reshape(-1, 1)
        return X.tolist(), y.tolist()

    @task
    def fit_model(variables):
        X, y = variables
        model = skl.LinearRegression()  # создание объекта модели
        model.fit(np.array(X), np.array(y))  # обучение модели
        score = model.score(np.array(X), np.array(y))
       
        return score

    fit_model(get_data())


# Создаем экземпляр DAG
dag_instance = read_data_dag()
