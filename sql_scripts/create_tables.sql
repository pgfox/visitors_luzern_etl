drop schema visitors_schema CASCADE; 
CREATE SCHEMA visitors_schema;
SET search_path TO "visitors_schema";

--drop table visitors;

CREATE TABLE visitors_raw(
        id SERIAL PRIMARY KEY,
        nodeid TEXT NOT NULL, 
        name TEXT NOT NULL,
        counter INTEGER,
        time BIGINT NOT NULL,
		iso_time timestamp NOT NULL,
        ltr NUMERIC,
        rtl NUMERIC,
        temperature NUMERIC,
        windspeed NUMERIC,
        weather_code SMALLINT NOT NULL,
	 	weather_desc TEXT NOT NULL,
        UNIQUE (nodeid, time)
);

--select * from test_star.visitors;

--Truncate table test_star.visitors;

CREATE TABLE d_date(
  date_dim_id              INT NOT NULL,
  date_actual              DATE NOT NULL,
  epoch                    BIGINT NOT NULL,
  day_suffix               VARCHAR(4) NOT NULL,
  day_name                 VARCHAR(9) NOT NULL,
  day_of_week              INT NOT NULL,
  day_of_month             INT NOT NULL,
  day_of_quarter           INT NOT NULL,
  day_of_year              INT NOT NULL,
  week_of_month            INT NOT NULL,
  week_of_year             INT NOT NULL,
  week_of_year_iso         CHAR(10) NOT NULL,
  month_actual             INT NOT NULL,
  month_name               VARCHAR(9) NOT NULL,
  month_name_abbreviated   CHAR(3) NOT NULL,
  quarter_actual           INT NOT NULL,
  quarter_name             VARCHAR(9) NOT NULL,
  year_actual              INT NOT NULL,
  first_day_of_week        DATE NOT NULL,
  last_day_of_week         DATE NOT NULL,
  first_day_of_month       DATE NOT NULL,
  last_day_of_month        DATE NOT NULL,
  first_day_of_quarter     DATE NOT NULL,
  last_day_of_quarter      DATE NOT NULL,
  first_day_of_year        DATE NOT NULL,
  last_day_of_year         DATE NOT NULL,
  mmyyyy                   CHAR(6) NOT NULL,
  mmddyyyy                 CHAR(10) NOT NULL,
  weekend_indr             BOOLEAN NOT NULL
);


ALTER TABLE d_date ADD CONSTRAINT d_date_date_dim_id_pk PRIMARY KEY (date_dim_id);

CREATE INDEX  d_date_date_actual_idx
  ON d_date(date_actual);
  

INSERT INTO d_date
SELECT TO_CHAR(datum, 'yyyymmdd')::INT AS date_dim_id,
       datum AS date_actual,
       EXTRACT(EPOCH FROM datum) AS epoch,
       TO_CHAR(datum, 'fmDDth') AS day_suffix,
       TO_CHAR(datum, 'TMDay') AS day_name,
       EXTRACT(ISODOW FROM datum) AS day_of_week,
       EXTRACT(DAY FROM datum) AS day_of_month,
       datum - DATE_TRUNC('quarter', datum)::DATE + 1 AS day_of_quarter,
       EXTRACT(DOY FROM datum) AS day_of_year,
       TO_CHAR(datum, 'W')::INT AS week_of_month,
       EXTRACT(WEEK FROM datum) AS week_of_year,
       EXTRACT(ISOYEAR FROM datum) || TO_CHAR(datum, '"-W"IW-') || EXTRACT(ISODOW FROM datum) AS week_of_year_iso,
       EXTRACT(MONTH FROM datum) AS month_actual,
       TO_CHAR(datum, 'TMMonth') AS month_name,
       TO_CHAR(datum, 'Mon') AS month_name_abbreviated,
       EXTRACT(QUARTER FROM datum) AS quarter_actual,
       CASE
           WHEN EXTRACT(QUARTER FROM datum) = 1 THEN 'First'
           WHEN EXTRACT(QUARTER FROM datum) = 2 THEN 'Second'
           WHEN EXTRACT(QUARTER FROM datum) = 3 THEN 'Third'
           WHEN EXTRACT(QUARTER FROM datum) = 4 THEN 'Fourth'
           END AS quarter_name,
       EXTRACT(YEAR FROM datum) AS year_actual,
       datum + (1 - EXTRACT(ISODOW FROM datum))::INT AS first_day_of_week,
       datum + (7 - EXTRACT(ISODOW FROM datum))::INT AS last_day_of_week,
       datum + (1 - EXTRACT(DAY FROM datum))::INT AS first_day_of_month,
       (DATE_TRUNC('MONTH', datum) + INTERVAL '1 MONTH - 1 day')::DATE AS last_day_of_month,
       DATE_TRUNC('quarter', datum)::DATE AS first_day_of_quarter,
       (DATE_TRUNC('quarter', datum) + INTERVAL '3 MONTH - 1 day')::DATE AS last_day_of_quarter,
       TO_DATE(EXTRACT(YEAR FROM datum) || '-01-01', 'YYYY-MM-DD') AS first_day_of_year,
       TO_DATE(EXTRACT(YEAR FROM datum) || '-12-31', 'YYYY-MM-DD') AS last_day_of_year,
       TO_CHAR(datum, 'mmyyyy') AS mmyyyy,
       TO_CHAR(datum, 'mmddyyyy') AS mmddyyyy,
       CASE
           WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE
           ELSE FALSE
           END AS weekend_indr
FROM (SELECT '2023-01-01'::DATE + SEQUENCE.DAY AS datum
      FROM GENERATE_SERIES(0, 730) AS SEQUENCE (DAY)
      GROUP BY SEQUENCE.DAY) DQ
ORDER BY 1;

--SELECT * FROM d_date

CREATE TABLE d_time
(
  time_dim_id              INT NOT NULL,
  time_of_day              TEXT,
  hour                     INTEGER NOT NULL,
  minute                   SMALLINT NOT NULL,
  day_time_name            TEXT NOT NULL,
  timezone				   TEXT NOT NULL	
);

ALTER TABLE d_time ADD CONSTRAINT d_time_time_dim_id_pk PRIMARY KEY (time_dim_id);

CREATE INDEX d_time_time_of_day_idx
  ON d_time(time_of_day);
  
INSERT INTO d_time
select TO_CHAR(minute, 'hh24mi')::INT AS time_dim_id,
     to_char(minute, 'hh24:mi') AS time_of_day,
	-- Hour of the day (0 - 23)
	extract(hour from minute) as the_hour, 
	-- Minute of the day (0 - 1439)
	extract(hour from minute)*60 + extract(minute from minute) as the_minute,
	-- Names of day periods
	case when to_char(minute, 'hh24:mi') between '05:00' and '07:59'
		then 'Morning'
	     when to_char(minute, 'hh24:mi') between '08:00' and '11:59'
		then 'AM'
	     when to_char(minute, 'hh24:mi') between '12:00' and '17:59'
		then 'PM'
	     when to_char(minute, 'hh24:mi') between '18:00' and '22:29'
		then 'Evening'
	     else 'Night'
	end as day_time_name,
	'UTC' as timezone
from (SELECT '0:00'::time + (sequence.minute || ' minutes')::interval AS minute
	FROM generate_series(0,1439) AS sequence(minute)
	GROUP BY sequence.minute
     ) DQ
order by 1;

CREATE TABLE d_device(
  device_dim_id            SERIAL NOT NULL,
  device_node_id           TEXT NOT NULL,   
  device_name              TEXT NOT NULL,
  device_type              TEXT NOT NULL,   -- wifi, radar
  device_type_code         SMALLINT NOT NULL, -- 0, 1 
  latitude                 FLOAT NOT NULL,	
  longitude                FLOAT NOT NULL
);

ALTER TABLE d_device ADD CONSTRAINT d_device_device_dim_id_pk PRIMARY KEY (device_dim_id);

CREATE INDEX d_device_device_node_id_idx ON d_device(device_node_id);


INSERT INTO d_device (device_node_id, device_name,device_type, device_type_code,latitude,longitude) 
	values('7076FF006905162D','Löwendenkmal','wifi',0,47.05855694959379,8.31106525427953);
	
INSERT INTO d_device (device_node_id, device_name,device_type, device_type_code,latitude,longitude) 
	values('7076FF0069051527','Rathausquai','wifi',0,47.05249464751403,8.308011455766191);
	
INSERT INTO d_device (device_node_id, device_name,device_type, device_type_code,latitude,longitude) 
	values('7076FF006905162F','Hertensteinstrasse','wifi',0,47.05465423381454,8.308944371108982);
	
INSERT INTO d_device (device_node_id, device_name,device_type, device_type_code,latitude,longitude) 
	values('7076FF006905162E','Schwanenplatz','wifi',0,47.0542931191461,8.308507617681869);
	
INSERT INTO d_device (device_node_id, device_name,device_type, device_type_code,latitude,longitude) 
	values('7076FF0069051634','Kapellbrücke Wifi','wifi',0,47.05180969029125,8.30751364042351);
	
INSERT INTO d_device (device_node_id, device_name,device_type, device_type_code,latitude,longitude) 
	values('343531315230790A','Kapellbrücke Radar','radar',1,47.05180969029125,8.30751364042351);

--select * from d_device;


CREATE TABLE d_weather
(
  weather_dim_id           SERIAL NOT NULL,
  temperature          	   FLOAT NOT NULL,   
  wind_speed			   FLOAT NOT NULL,
  weather_code			   SMALLINT NOT NULL,
  weather_code_desc			   TEXT NOT NULL 	
);

ALTER TABLE d_weather ADD CONSTRAINT d_weather_weather_dim_id_pk PRIMARY KEY (weather_dim_id);

CREATE INDEX d_weather_device_temperature_idx ON d_weather(temperature);

--select * from d_weather;


CREATE TABLE f_visitor_frequency
(
  device_dim_id 		   INT REFERENCES d_device(device_dim_id),
  date_dim_id			   INT REFERENCES d_date(date_dim_id),	
  time_dim_id              INT REFERENCES d_time(time_dim_id),   
  weather_dim_id		   INT REFERENCES d_weather(weather_dim_id),	
  counter 				   INT NOT NULL,
  ltr					   INT NOT NULL,
  rtl 					   INT NOT NULL
);

ALTER TABLE f_visitor_frequency ADD CONSTRAINT f_visitor_frequency_id_pk PRIMARY KEY (device_dim_id,date_dim_id,time_dim_id);

CREATE TABLE visitors_raw_history(
        id SERIAL PRIMARY KEY,
        nodeid TEXT NOT NULL, 
        name TEXT NOT NULL,
        counter INTEGER,
        time BIGINT NOT NULL,
		iso_time timestamp NOT NULL,
        ltr NUMERIC,
        rtl NUMERIC,
        temperature NUMERIC,
        windspeed NUMERIC,
        weather_code SMALLINT NOT NULL,
		weather_desc TEXT NOT NULL
        --UNIQUE (nodeid, time)
);




--select * from f_visitor_frequency ;

--select * from d_weather;

--truncate table visitors;

--select * from visitors_raw;

--select * from visitors_raw_history;




