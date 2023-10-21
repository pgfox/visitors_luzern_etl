SET search_path TO "visitors_schema";

select count(*) from visitors_raw_history;

select * from d_weather;

select count(*) from f_visitor_frequency;

select * from f_visitor_frequency limit 2;

select * from d_device;

select distinct day_time_name from d_time;

select distinct * from d_date;

select * from visitors_raw_history limit 2;

select distinct nodeid, time from visitors_raw_history;

select * from d_time ;

select temperature,  count(weather_dim_id) from d_weather group by temperature order by count desc;

select count(*) from f_visitor_frequency where date_dim_id = 20231016;

select dd.device_name, fv.date_dim_id,fv.time_dim_id, fv.weather_dim_id, dw.temperature from f_visitor_frequency fv  
inner join d_weather dw on dw.weather_dim_id = fv.weather_dim_id 
inner join d_device dd on dd.device_dim_id = fv.device_dim_id 
where fv.date_dim_id = 20231017;


select distinct fv.time_dim_id, dw.temperature from f_visitor_frequency fv  
inner join d_weather dw on dw.weather_dim_id = fv.weather_dim_id 
inner join d_device dd on dd.device_dim_id = fv.device_dim_id 
where fv.date_dim_id = 20231017 and dd.device_name = 'Löwendenkmal';

-- how many entries per device in f_visitor_frequency on 20231016 
select dd.device_name, count(*) from f_visitor_frequency fv  
inner join d_weather dw on dw.weather_dim_id = fv.weather_dim_id 
inner join d_device dd on dd.device_dim_id = fv.device_dim_id 
where fv.date_dim_id = 20231016 
group by dd.device_name

-- how many people visited each location on 20231016
select dd.device_name, sum(fv.counter) from f_visitor_frequency fv  
inner join d_device dd on dd.device_dim_id = fv.device_dim_id 
where fv.date_dim_id = 20231017 
group by dd.device_name

-- how many people visited when temperature less than 3 
select dd.device_name, sum(fv.counter) from f_visitor_frequency fv  
inner join d_device dd on dd.device_dim_id = fv.device_dim_id 
inner join d_weather dw on dw.weather_dim_id = fv.weather_dim_id 
where fv.date_dim_id = 20231017 and dw.temperature < 3
group by dd.device_name

-- how many times was the temperature less then 3
select dd.device_name, count(*) from f_visitor_frequency fv  
inner join d_device dd on dd.device_dim_id = fv.device_dim_id 
inner join d_weather dw on dw.weather_dim_id = fv.weather_dim_id 
where fv.date_dim_id = 20231017 and dw.temperature < 3
group by dd.device_name

-- how many visitors on a Tuesday morning 
select dd.device_name, sum(fv.counter) from f_visitor_frequency fv  
inner join d_device dd on dd.device_dim_id = fv.device_dim_id 
inner join d_date da on da.date_dim_id = fv.date_dim_id 
where fv.date_dim_id = 20231017 and da.day_name = 'Tuesday'
group by dd.device_name


-- how many visitors between 10 - 1200 in morning
select dd.device_name, sum(fv.counter) from f_visitor_frequency fv  
inner join d_device dd on dd.device_dim_id = fv.device_dim_id 
where fv.time_dim_id between 1000 and 1200
group by dd.device_name
