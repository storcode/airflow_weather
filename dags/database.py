import json


def insert_weather(cursor, date_downloads, time_downloads, r):
    cursor.execute(
        "INSERT INTO public.weather (date_downloads,time_downloads,coord,weather,base,main,visibility,wind,clouds,dt,sys,timezone,name)"
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (date_downloads, time_downloads,
         json.dumps(r["coord"]), json.dumps(r["weather"]), r["base"], json.dumps(r["main"]), r["visibility"],
         json.dumps(r["wind"]), json.dumps(r["clouds"]), r["dt"], json.dumps(r["sys"]), r["timezone"], r["name"]))
    count_weather = cursor.rowcount
    return count_weather

def insert_dim_coordinates(cursor):
    cursor.execute("""
    insert into dwh.dim_coordinates (longitude, latitude)
	    select (select * from json_to_record(w.coord) as x(lon float)) as longitude,
	    (select * from json_to_record(w.coord) as x(lat float)) as latitude
	    from public.weather  w
        where w.id > (select max(coord_id) from dwh.dim_coordinates)"""
    count_dim_coordinates = cursor.rowcount
    return count_dim_coordinates


def insert_dim_date(cursor):
    cursor.execute("""
    insert into dwh.dim_date (full_date, initial_date, "year", "month", "day", "quarter", number_week , day_week, day_year, month_year)
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
        from public.weather w
        where w.id > (select max(date_id) from dwh.dim_date)"""
    count_dim_date = cursor.rowcount
    return count_dim_date


def insert_dim_sun_light(cursor):
    cursor.execute("""
    insert into dwh.dim_sun_light (sunrise, sunset, sunrise_unix, sunset_unix)
        select (select * from json_to_record(w.sys) as x(sunrise int4)) as sunrise,
        (select * from json_to_record (w.sys) as x(sunset int4)) as sunset,
        (select timestamp with time zone 'epoch' + (select * from json_to_record (w.sys) as x(sunrise int4)) * interval '1 second') as sunrise_unix,
        (select timestamp with time zone 'epoch' + (select * from json_to_record (w.sys) as x(sunset int4)) * interval '1 second') as sunset_unix
        from public.weather w
        where w.id > (select max(sun_light_id) from dwh.dim_sun_light)"""
    count_dim_sun_light = cursor.rowcount
    return count_dim_sun_light


def insert_dim_time(cursor):
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
        from public.weather w
        where w.id > (select max(time_id) from dwh.dim_time)"""
    count_dim_time = cursor.rowcount
    return count_dim_time


def insert_dim_timezone(cursor):
    cursor.execute("""
    insert into dwh.dim_timezone (timezone)
        select w.timezone
        from public.weather w
        where w.id > (select max(timezone_id) from dwh.dim_timezone)"""
    count_dim_timezone = cursor.rowcount
    return count_dim_timezone


def insert_dim_timezone_name(cursor):
    cursor.execute("""
    insert into dwh.dim_timezone_name ("name", country)
        select w."name",
        (select * from json_to_record(w.sys) as x(country text)) as country
        from public.weather w
        where w.id > (select max(timezone_name_id) from dwh.dim_timezone_name)"""
    count_dim_timezone_name = cursor.rowcount
    return count_dim_timezone_name


def insert_dim_weather_descr(cursor):
    cursor.execute("""
    insert into dwh.dim_weather_descr (condition_id, group_main_params, weather_condition_groups, clouds)
        select (select * from json_to_recordset(w.weather) as x(id int4)) as condition_id,
        (select * from json_to_recordset(w.weather) as x(main text)) as group_main_params,
        (select * from json_to_recordset(w.weather) as x(description text)) as weather_condition_groups,
        (select * from json_to_record(w.clouds) as x("all" int4)) as clouds
        from public.weather w
        where w.id > (select max(weather_descr_id) from dwh.dim_weather_descr)"""
    count_dim_weather_descr = cursor.rowcount
    return count_dim_weather_descr


    insert into dwh.fact_weather (dim_coordinates_id, dim_date_id, dim_sun_light_id, dim_time_id, dim_timezone_id, dim_timezone_name_id, dim_weather_descr_id, temperature, feels_like, temp_min, temp_max, pressure, humidity, visibility)
        select ******