#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 12 14:04:18 2023

@author: pfox
"""

import requests
import pandas as pd
from pandas import json_normalize
import sched, time
from pathlib import Path
from datetime import datetime  
import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy import DateTime
import pandas as pd

from sqlalchemy import MetaData
from sqlalchemy import Integer, String, Column, Table

from sqlalchemy import text
import logging
import sqlalchemy as sa

LOG = logging.getLogger("visitors-logger")

DATABASE_URI = 'postgresql+psycopg2://postgres:password@localhost:5432/visitor_luzern'
SCHEMA_NAME = 'visitors_schema'

    

class DBHelper:
    
    def __init__(self, db_uri=DATABASE_URI, schema_name=SCHEMA_NAME):
        
       LOG.debug('initialising DB connection') 
       self._db_uri = db_uri
       self._schema = schema_name
       self._engine = create_engine(self._db_uri,connect_args={'options': '-csearch_path={}'.format( self._schema )})
       
       
    def _run_raw_sql(self,the_sql):
         LOG.info(f'runing sql: {the_sql}')
         
         with self._engine.connect() as conn:
             sql = text(the_sql)       
             conn.execute(sql) 
             conn.commit()
   
    
        
    def truncate_visitors_table(self):     
                
        truncate_visitors_sql='''
            truncate table visitors_raw;
        '''  
        
        LOG.debug('truncate visitors_raw table')
        self._run_raw_sql(truncate_visitors_sql)
        
        
    def update_weather_dim_table(self):
        
        update_weather_sql='''
        insert into d_weather(temperature,wind_speed,weather_code,weather_code_desc) 
        select  DISTINCT vi.temperature, vi.windspeed, vi.weather_code, vi.weather_desc from visitors_raw as vi
        WHERE NOT EXISTS(select we.temperature, we.wind_speed, we.weather_code from d_weather as we
        					where we.temperature =  vi.temperature AND
        				 		  we.wind_speed = vi.windspeed AND
        				 		  we.weather_code = vi.weather_code	
        				);
        '''
        
        LOG.debug('update weather dimension table')
        self._run_raw_sql(update_weather_sql)
        
        

    def drop_f_vistor_frequency_STAGE_table(self):

        drop_f_vistor_frequency_STAGE_sql = '''
        drop table if exists f_visitor_frequency_STAGE
        '''

        self._run_raw_sql(drop_f_vistor_frequency_STAGE_sql)


    def create_f_vistor_frequency_STAGE_table(self):

        create_f_vistor_frequency_STAGE_sql='''
       CREATE TABLE f_visitor_frequency_STAGE AS
       select 
       (SELECT de.device_dim_id from d_device de where de.device_node_id = vi.nodeid ) as device_dim_id,
       (SELECT da.date_dim_id from d_date da where da.date_actual = vi.iso_time::Date ) as date_dim_id,
       (SELECT ti.time_dim_id from d_time ti where ti.time_of_day = to_char(vi.iso_time,'HH24:mi')) as time_dim_id,
       (select we.weather_dim_id from d_weather we where we.temperature =  vi.temperature AND
       				 		  we.wind_speed = vi.windspeed AND
       				 		  we.weather_code = vi.weather_code	) as weather_dim_id,
        vi.counter, vi.ltr, vi.rtl						  						  
       from visitors_raw as vi
       '''

        LOG.debug('create f_vistor_frequency_STAGE table')
        self._run_raw_sql(create_f_vistor_frequency_STAGE_sql)

    def add_to_f_visitor_frequenct_table(self):
    
        add_to_f_visitor_frequenct_sql = '''
        INSERT INTO f_visitor_frequency
        select vi_stage.device_dim_id,  vi_stage.date_dim_id, vi_stage.time_dim_id , vi_stage.weather_dim_id ,vi_stage.counter, vi_stage.ltr, vi_stage.rtl
        	from f_visitor_frequency_STAGE as vi_stage
        WHERE NOT EXISTS(select vi.device_dim_id, vi.date_dim_id, vi.time_dim_id from f_visitor_frequency as vi
        					where vi.device_dim_id =  vi_stage.device_dim_id AND
        				 		  vi.date_dim_id = vi_stage.date_dim_id AND
        				 		  vi.time_dim_id = vi_stage.time_dim_id	)
        '''
        
        self._run_raw_sql(add_to_f_visitor_frequenct_sql)
        

    def add_to_visitors_raw_history(self):
     
         add_to_visitors_raw_history = '''
         INSERT INTO visitors_raw_history(
             nodeid, name, counter, time, iso_time, ltr, rtl, temperature, windspeed,  weather_code , weather_desc
             )
         select nodeid, name, counter, time, iso_time, ltr, rtl, temperature, windspeed, weather_code, weather_desc
         	from visitors_raw 
         '''
         
         self._run_raw_sql(add_to_visitors_raw_history)
        

    def update_star(self):
        
        self.update_weather_dim_table()
        self.drop_f_vistor_frequency_STAGE_table()
        self.create_f_vistor_frequency_STAGE_table()
        self.add_to_f_visitor_frequenct_table()
        self.add_to_visitors_raw_history()
        self.truncate_visitors_table()

    def insert_data( self, df):
            
        metadata2=MetaData()
        with self._engine.connect() as conn:
            
            LOG.info('Loading visitors_RAW from DB.')
            
            visitors_table=Table("visitors_raw",
                                metadata2,
                                autoload_with=conn)
        
            print(visitors_table.c)
            
            
            date_format = '%Y-%m-%d %H:%M:%S'               
            the_time = datetime.strptime('2023-09-30 17:10:55',date_format)   
            
            for index, row in df.iterrows():   
                print( row['nodeid'] )
                try:
                    stmt = insert(visitors_table).values(
                        nodeid=row['nodeid'], 
                        name= row['name'], 
                        counter=row['counter'] , 
                        time= row['time'],
                        iso_time = datetime.strptime(row['ISO_time'],date_format),
                        #iso_time =datetime(['ISO_time']),
                        ltr = row['ltr'] ,
                        rtl = row['rtl']  ,
                        temperature= row['temperature'] ,
                        windspeed= row['windspeed'] ,
                        weather_desc = row['weather_desc'],
                        weather_code= row['weather_code'] )
                    
                    compiled = stmt.compile()
                    result = conn.execute(stmt)
                    conn.commit()
                except IntegrityError as ex :
                   print(ex)
                   conn.rollback()
                   print('CARRY ON!!!')
                   
    def check_table(self):
        print('check tables')    
        engine = sa.create_engine(DATABASE_URI, echo=False)
        meta = sa.MetaData()
        meta.reflect(engine, schema='visitors_schema')
        print(meta.tables.keys())
        