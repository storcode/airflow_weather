import psycopg2
from psycopg2 import OperationalError
import requests
import pytz
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import pendulum
from database import *


def download():
    import key_appid
    url = f'https://api.openweathermap.org/data/2.5/weather?q=Cheboksary,ru&APPID={key_appid.key_appid}&units=metric'
    reg_json = requests.get(url=url).json()
    json.dumps(reg_json)
    print(reg_json)
    return reg_json


def process_weather_data():
    import key_PSQL
    req_json = download()
    msc = pytz.timezone('europe/moscow')
    date_downloads = datetime.now(msc).strftime("%Y-%m-%d")
    time_downloads = datetime.now(msc).strftime("%H:%M:%S")
    try:
        connection = psycopg2.connect(user=key_PSQL.user,
                                      password=key_PSQL.password,
                                      host="postgres",  # название контейнера в docker-compose
                                      port="5432",
                                      database="airflow")
        print("Подключение к базе PostgreSQL выполнено")
        cursor = connection.cursor()
        count_weather = insert_weather(cursor, date_downloads, time_downloads, req_json)
        print(count_weather, "Запись успешно вставлена в таблицу 'weather'")
        count_dim_clouds = insert_dim_clouds(cursor)
        print(count_dim_clouds, "Запись успешно вставлена в таблицу 'dim_clouds'")
        count_dim_coordinates = insert_dim_coordinates(cursor)
        print(count_dim_coordinates, "Запись успешно вставлена в таблицу 'dim_coordinates'")
        count_dim_date = insert_dim_date(cursor)
        print(count_dim_date, "Запись успешно вставлена в таблицу 'dim_date'")
        count_dim_sun_light = insert_dim_sun_light(cursor)
        print(count_dim_sun_light, "Запись успешно вставлена в таблицу 'dim_sun_light'")
        count_dim_time = insert_dim_time(cursor)
        print(count_dim_time, "Запись успешно вставлена в таблицу 'dim_time'")
        count_dim_timezone = insert_dim_timezone(cursor)
        print(count_dim_timezone, "Запись успешно вставлена в таблицу 'dim_timezone'")
        count_dim_timezone_name = insert_dim_timezone_name(cursor)
        print(count_dim_timezone_name, "Запись успешно вставлена в таблицу 'dim_timezone_name'")
        count_dim_weather_descr = insert_dim_weather_descr(cursor)
        print(count_dim_weather_descr, "Запись успешно вставлена в таблицу 'dim_weather_descr'")
        count_dim_wind = insert_dim_wind(cursor)
        print(count_dim_wind, "Запись успешно вставлена в таблицу 'dim_wind'")
        count_stage_fact_weather = insert_stage_fact_weather(cursor)
        print(count_stage_fact_weather, "Запись успешно вставлена в таблицу 'stage_fact_weather'")
        connection.commit()
        cursor.close()
        connection.close()
        print("Соединение с PostgreSQL закрыто")
    except OperationalError as e:
        print(f"Произошла ошибка {e}")


with DAG(dag_id='weather', default_args={
    'owner': 'storcode',
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=1),
    'catchup': False,
    'depends_on_past': True
    },
         schedule_interval='*/10 * * * *',
         start_date=pendulum.datetime(2023, 1, 1), catchup=False, ) as dag:
    hello = BashOperator(
        task_id="hello",
        bash_command="echo hello"
    )

    weather_data = PythonOperator(
        task_id='process_weather_data',
        python_callable=process_weather_data,
        dag=dag
    )
    hello >> weather_data

