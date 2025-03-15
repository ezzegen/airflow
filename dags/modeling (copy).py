import pandas as pd
import numpy as np
from sklearn.neural_network import MLPRegressor
from airflow import DAG
from airflow.decorators import dag, task
import pendulum

@dag(
    dag_id='modeling',
    start_date=pendulum.now('Europe/Samara'),
    schedule=None,
    catchup=False
)
def modeling():
    df = pd.read_csv('data/sources/LM_pneumonia.csv')

    # Выделяем столбцы для истории и задачи
    df_history = df.iloc[:, :50]
    df_challenge = df.iloc[:, 50:75]
    
    def predict_with_model(model_class, dir_name):
        df_challenge_pred = pd.DataFrame(index=df_challenge.index, columns=df_challenge.columns)

        for k in range(1, 11):
            print(f'Processing history dive = {k}')
            # Обновляем df_history для каждой итерации
            df_history = df.iloc[:, :50].copy()

            for col in df_challenge.columns:
                y_pred_list = []

                for i in range(len(df)):
                    # Формируем y, исключая текущий индекс
                    y = df_challenge[col].drop(index=i)
                    # Формируем X, исключая текущий индекс
                    X = df_history.iloc[:, -k:].drop(index=i)

                    # Обучение модели на текущем подмножестве данных
                    model = model_class()
                    model.fit(X, y)

                    # Предсказание на основе последней строки df_history
                    X_pred = df_history.iloc[[i], -k:].values.reshape(1, -1)
                    y_pred = model.predict(X_pred)[0]
                    y_pred_list.append(y_pred)

                # Заполняем DataFrame предсказаниями
                df_challenge_pred[col] = y_pred_list
                
                # Обновляем df_history только с состоянием до текущего прогноза
                df_history[col] = df_challenge[col]
    
            df_challenge_pred.to_csv(f'data/results/{dir_name}/history_dive_{k}.csv', index=False)
    
    @task
    def process_mlp_regressor():
        predict_with_model(MLPRegressor, 'MLPR')

    process_mlp_regressor()

# Создаем экземпляр DAG
modeling = modeling()
