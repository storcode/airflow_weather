import psycopg2
from psycopg2 import OperationalError
import requests
import json
import pytz
import datetime as dt
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def process_weather_data():
    import key_appid
    import key_PSQL
    url = f'https://api.openweathermap.org/data/2.5/weather?q=Cheboksary,ru&APPID={key_appid.key_appid}&units=metric'
    req_json = requests.get(url=url).json()

    with open('weather_city.json', 'w') as json_file:
        json.dump(req_json, json_file)
    print("Файл успешно скачан")

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
        cursor.execute("""
            insert into dwh.weather (date_downloads,time_downloads,coord,weather,base,main,visibility,wind,clouds,
            dt,sys,timezone,name) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                       (date_downloads, time_downloads,
                        json.dumps(req_json["coord"]), json.dumps(req_json["weather"]), req_json["base"],
                        json.dumps(req_json["main"]),
                        req_json["visibility"], json.dumps(req_json["wind"]), json.dumps(req_json["clouds"]),
                        req_json["dt"],
                        json.dumps(req_json["sys"]), req_json["timezone"], req_json["name"]))
        count_weather = cursor.rowcount
        print(count_weather, "Запись успешно вставлена в таблицу 'weather'")
        cursor.execute("""
            insert into dwh.dim_coordinates (longitude, latitude)
        	    select (select * from json_to_record(w.coord) as x(lon float)) as longitude,
        	            (select * from json_to_record(w.coord) as x(lat float)) as latitude
        	    from dwh.weather  w
                where w.id > (select max(coord_id) from dwh.dim_coordinates""")
        count_dim_coordinates = cursor.rowcount
        print(count_dim_coordinates, "Запись успешно вставлена в таблицу 'dim_coordinates'")
        cursor.execute("""
            insert into dwh.dim_date (full_date, initial_date, "year", "month", "day", "quarter", number_week,
            day_week, day_year, month_year)
                select  (select to_char(w.date_downloads, 'YYYYMMDD')::int4) as full_date,
                        w.date_downloads,
                        (select extract(year from w.date_downloads)) as "year",
                        (select extract(month from w.date_downloads)) as "month",
                        (select extract(day from w.date_downloads)) as "day",
                        (select extract(quarter from w.date_downloads)) as "quarter",
                        (select extract(week from w.date_downloads)) as number_week,
                        (select extract(isodow from w.date_downloads)) as day_week,
                        (select extract(doy from w.date_downloads)) as day_year,
                        (select extract(month from w.date_downloads)) as month_year
                from dwh.weather w
                where w.id > (select max(date_id) from dwh.dim_date""")
        count_dim_date = cursor.rowcount
        print(count_dim_date, "Запись успешно вставлена в таблицу 'dim_date'")
        cursor.execute("""
            insert into dwh.dim_sun_light (sunrise, sunset, sunrise_unix, sunset_unix)
                select (select * from json_to_record(w.sys) as x(sunrise int4)) as sunrise,
                (select * from json_to_record (w.sys) as x(sunset int4)) as sunset,
                (select timestamp with time zone 'epoch' + (select * from json_to_record (w.sys)
                    as x(sunrise int4)) * interval '1 second') as sunrise_unix,
                (select timestamp with time zone 'epoch' + (select * from json_to_record (w.sys)
                    as x(sunset int4)) * interval '1 second') as sunset_unix
                from dwh.weather w
                where w.id > (select max(sun_light_id) from dwh.dim_sun_light""")
        count_dim_sun_light = cursor.rowcount
        print(count_dim_sun_light, "Запись успешно вставлена в таблицу 'dim_sun_light'")
        cursor.execute("""
            insert into dwh.dim_time (full_time, initial_time, "hour", "minute", "second", night , morning, afternoon, evening)
                select  (select to_char(w.time_downloads, 'HH24MISS')::int4) as full_time,
                        w.time_downloads,
                        (select extract(hour from w.time_downloads)) as "hour",
                        (select extract(minute from w.time_downloads)) as "minute",
                        (select extract(second from w.time_downloads)) as "second",
                        (select case when w.time_downloads between '00:00:00' and '05:59:59' then 'night' end) as night,
                        (select case when w.time_downloads between '06:00:00' and '11:59:59' then 'morning' end) as morning,
                        (select case when w.time_downloads between '12:00:00' and '17:59:59' then 'afternoon' end) as afternoon,
                        (select case when w.time_downloads between '18:00:00' and '23:59:59' then 'evening' end) as evening
                from dwh.weather w
                where w.id > (select max(time_id) from dwh.dim_time""")
        count_dim_time = cursor.rowcount
        print(count_dim_time, "Запись успешно вставлена в таблицу 'dim_time'")
        cursor.execute("""
            insert into dwh.dim_timezone (timezone)
                select w.timezone
                from dwh.weather w
                where w.id > (select max(timezone_id) from dwh.dim_timezone""")
        count_dim_timezone = cursor.rowcount
        print(count_dim_timezone, "Запись успешно вставлена в таблицу 'dim_timezone'")
        cursor.execute("""
            insert into dwh.dim_timezone_name ("name", country)
                select w."name",
                (select * from json_to_record(w.sys) as x(country text)) as country
                from dwh.weather w
                where w.id > (select max(timezone_name_id) from dwh.dim_timezone_name""")
        count_dim_timezone_name = cursor.rowcount
        print(count_dim_timezone_name, "Запись успешно вставлена в таблицу 'dim_timezone_name'")
        cursor.execute("""
            insert into dwh.dim_weather_descr (condition_id, group_main_params, weather_condition_groups, clouds)
                select (select * from json_to_recordset(w.weather) as x(id int4)) as condition_id,
                (select * from json_to_recordset(w.weather) as x(main text)) as group_main_params,
                (select * from json_to_recordset(w.weather) as x(description text)) as weather_condition_groups,
                (select * from json_to_record(w.clouds) as x("all" int4)) as clouds
                from dwh.weather w
                where w.id > (select max(weather_descr_id) from dwh.dim_weather_descr""")
        count_dim_weather_descr = cursor.rowcount
        print(count_dim_weather_descr, "Запись успешно вставлена в таблицу 'dim_weather_descr'")
        #      cursor.execute("""
        #           insert into fact_weather (dim_coordinates_id, dim_date_id, dim_sun_light_id, dim_time_id,
        #           dim_timezone_id, dim_timezone_name_id, dim_weather_descr_id, temperature, feels_like,
        #           temp_min, temp_max, pressure, humidity, visibility)
        #            select ******""")
        #      print(count_fact_weather, "Запись успешно вставлена в таблицу 'fact_weather'")
        connection.commit()
        cursor.close()
        connection.close()
        print("Соединение с PostgreSQL закрыто")
    except OperationalError as e:
        print(f"Произошла ошибка {e}")


args = {
    'owner': 'storcode',
    'start_date': dt.datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'schedule_interval': '*/5 * * * *',
    'depends_on_past': False
}

with DAG(dag_id='weather', default_args=args) as dag:
    weather_data = PythonOperator(
        task_id='process_weather_data',
        python_callable=process_weather_data,
        dag=dag
    )
