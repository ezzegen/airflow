import pandas as pd
import numpy as np
import sklearn.linear_model as skl
from sklearn.neural_network import MLPRegressor
from sklearn.linear_model import LinearRegression
from airflow import DAG
from airflow.decorators import dag, task
import pendulum



@dag(
    dag_id='modeling_dag',
    start_date=pendulum.now('Europe/Samara'),
    schedule = None,
    catchup=False
)
def modeling():

    df = pd.read_csv('data/sources/LM_pneumonia.csv')

    df_history = df[df.columns[:50]]
    df_challenge = df[df.columns[50:75]]

    def predict_with_model(model_class, dir_name):
        df_challenge_pred = pd.DataFrame(index = df_challenge.index, columns = df_challenge.columns)

        for k in range(1, 11):
            print(f'Processing history dive = {k}')
            df_history = df[df.columns[:50]]

            for col in df_challenge.columns:
                y_pred_list = []

                for i in range(len(df)):
                    y = df_challenge[col].drop(index = [i])
                    X = df_history[df_history.columns[-k:]].drop(index = [i])
                    #print(len(y))
                    print(len(X.columns))

                    model = model_class()
                    model.fit(X, y)

                    X_pred = df_history[df_history.columns[-k:]].loc[i].values.reshape(1, -1)

                    y_pred = model.predict(X_pred)[0]
                    y_pred_list.append(y_pred)

                df_challenge_pred[col] = y_pred_list
                df_history[col] = df_challenge[col]
    
            df_challenge_pred.to_csv('data/results/'+ dir_name +'/history_dive_' + str(k) + '.csv', index = False)
    
    @task
    def process_mlp_regressor():
        columns = df.columns
        history_columns = columns[:50]
        challenge_columns = columns[50:75]
        results = predict_with_model(MLPRegressor, 'MLPR')
    

    process_mlp_regressor()


# Создаем экземпляр DAG
modeling_dag = modeling()
