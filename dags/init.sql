CREATE SCHEMA IF NOT EXISTS dwh;

CREATE TABLE IF NOT EXISTS dwh.weather
(
	id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
	date_downloads date NULL,
	time_downloads time NULL,
	coord json NULL,
	weather json NULL,
	base text NULL,
	main json NULL,
	visibility text NULL,
	wind json NULL,
	clouds json NULL,
	dt int4 NULL,
	sys json NULL,
	timezone int4 NULL,
	name text NULL,
	CONSTRAINT pk_weather PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS dwh.dim_clouds
(
	clouds_id int4 generated always as identity,
	clouds int4 not null,
	sys_ts timestamp(0) default now(),
	constraint pk_dim_clouds primary key (clouds_id)
);

CREATE TABLE IF NOT EXISTS dwh.dim_coordinates
(
	coord_id int4 generated always as identity,
	longitude float not null,
	latitude float not null,
	sys_ts timestamp(0) default now(),
	constraint pk_dim_coordinates primary key(coord_id)
);

CREATE TABLE IF NOT EXISTS dwh.dim_date 
(
	date_id int4 generated always as identity,
	full_date int4 not null,
	initial_date date not null,
	"year" int4 not null,
	"month" int4 not null,
	"day" int4 not null,
	"quarter" int4 not null,
	number_week int4 not null,
	day_week int4 not null,
	day_year int4 not null,
	month_year int4 not null,
	sys_ts timestamp(0) default now(),
	constraint pk_date primary key (date_id)
);

CREATE TABLE IF NOT EXISTS dwh.dim_sun_light
(
	sun_light_id int4 generated always as identity,
	sunrise int4 not null,
	sunset int4 not null,
	sunrise_unix timestamp not null,
	sunset_unix timestamp not null,
	sys_ts timestamp(0) default now(),
	constraint pk_dim_sun_light primary key(sun_light_id)
);

CREATE TABLE IF NOT EXISTS dwh.dim_time
(
	time_id int4 generated always as identity,
	full_time int4 not null,
	initial_time time not null,
	"hour" int4 not null,
	"minute" int4 not null,
	"second" int4 not null,
	night text,
	morning text,
	afternoon text,
	evening text,
	sys_ts timestamp(0) default now(),
	constraint pk_dim_time primary key (time_id)
);

CREATE TABLE IF NOT EXISTS dwh.dim_timezone
(
	timezone_id int4 generated always as identity,
	timezone int4 not null,
	sys_ts timestamp(0) default now(),
	constraint pk_dim_timezone primary key(timezone_id)
);

CREATE TABLE IF NOT EXISTS dwh.dim_timezone_name
(
	timezone_name_id int4 generated always as identity,
	"name" text not null,
	country text not null,
	sys_ts timestamp(0) default now(),
	constraint pk_dim_timezone_name primary key(timezone_name_id)
);

CREATE TABLE IF NOT EXISTS dwh.dim_weather_descr
(
	weather_descr_id int4 generated always as identity,
	condition_id int4 not null,
	group_main_params text not null,
	weather_condition_groups text not null,
	clouds int4 not null,
	sys_ts timestamp(0) default now(),
	constraint pk_dim_weather_descr primary key (weather_descr_id)
);

CREATE TABLE IF NOT EXISTS dwh.dim_wind
(
	wind_id int4 generated always as identity,
	speed int4 not null,
	"degree" int4 not null,
	sys_ts timestamp(0) default now(),
	constraint pk_dim_wind primary key(wind_id)
);

CREATE TABLE IF NOT EXISTS dwh.stage_fact_weather
(
	stage_fact_weather_id int4 generated always as identity,
	weather_id int4 not null,
	hash uuid not null,
	dim_date_id int4 not null,
	date_downloads date not null,
	dim_time_id int4 not null,
	time_downloads time not null,
	dim_coordinates_id int4 not null,
	longitude float not null,
	latitude float not null,
	dim_sun_light_id int4 not null,
	sun_light_type int4 not null,
	sun_l_id int4 not null,
	sun_l_country text not null,
	sun_l_sunrise int8 not null,
	sun_l_sunset int8 not null,
	dim_timezone_id int4 not null,
	timezone int4 not null,
	dim_timezone_name_id int4 not null,
	timezone_name text not null,
	dim_weather_descr_id int4 not null,
	weather_descr_id int4 not null,
	weather_main text not null,
	weather_description text not null,
	dim_clouds_id int4 not null,
	cloudiness int4 not null,
	dim_wind_id int4 not null,
	wind_speed int4 not null,
	wind_direction int4 not null,
	temperature float not null,
	feels_like float not null,
	temp_min float not null,
	temp_max float not null,
	pressure int4 not null,
	humidity int4 not null,
	visibility int4 not null,
	time_calculation int8 not null,
	sys_ts timestamp(0) default now()
);
