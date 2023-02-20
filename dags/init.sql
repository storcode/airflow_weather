CREATE SCHEMA IF NOT EXISTS dwh;

CREATE TABLE IF NOT EXISTS dwh.weather (
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

CREATE TABLE IF NOT EXISTS dwh.fact_weather
(
	fact_weather_id int4 generated always as identity,
	dim_coordinates_id int4 not null,
	dim_date_id int4 not null,
	dim_sun_light_id int4 not null,
	dim_time_id int4 not null,
	dim_timezone_id int4 not null,
	dim_timezone_name_id int4 not null,
	dim_weather_descr_id int4 not null,
	temperature float not null,
	feels_like float not null,
	temp_min float not null,
	temp_max float not null,
	pressure int4 not null,
	humidity int4 not null,
	visibility int4 not null,
	sys_ts timestamp(0) default now(),
	CONSTRAINT pk_fact_weather PRIMARY KEY (fact_weather_id),
	CONSTRAINT fk_coord FOREIGN KEY (dim_coordinates_id) REFERENCES dwh.dim_coordinates(coord_id) ON DELETE CASCADE ON UPDATE CASCADE,
	CONSTRAINT fk_dim_date FOREIGN KEY (dim_date_id) REFERENCES dwh.dim_date(date_id) ON DELETE CASCADE ON UPDATE CASCADE,
	CONSTRAINT fk_sun_light FOREIGN KEY (dim_sun_light_id) REFERENCES dwh.dim_sun_light(sun_light_id) ON DELETE CASCADE ON UPDATE CASCADE,
	CONSTRAINT fk_time FOREIGN KEY (dim_time_id) REFERENCES dwh.dim_time(time_id) ON DELETE CASCADE ON UPDATE CASCADE,
	CONSTRAINT fk_timezone FOREIGN KEY (dim_timezone_id) REFERENCES dwh.dim_timezone(timezone_id) ON DELETE CASCADE ON UPDATE CASCADE,
	CONSTRAINT fk_timezone_name FOREIGN KEY (dim_timezone_name_id) REFERENCES dwh.dim_timezone_name(timezone_name_id) ON DELETE CASCADE ON UPDATE CASCADE,
	CONSTRAINT fk_weather_descr FOREIGN KEY (dim_weather_descr_id) REFERENCES dwh.dim_weather_descr(weather_descr_id) ON DELETE CASCADE ON UPDATE CASCADE
);
