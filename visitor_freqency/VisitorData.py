#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct  9 16:52:22 2023

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
from visitor_freqency.DBUtils import DBHelper
from visitor_freqency.APIUtils import WeatherHelper
from visitor_freqency.APIUtils import VisitorHelper


import logging
logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger("visitors-logger")



DATABASE_URI = 'postgresql+psycopg2://postgres:password@localhost:5432/visitor_luzern'
pd.set_option('display.max_columns', None)



def write_csv_file(df : pd.DataFrame,location:str):
    now = datetime.now()
    dt_string = now.strftime("%d_%m_%Y_%H_%M")
    filepath = Path(location+'_'+dt_string+'.csv')  
    filepath.parent.mkdir(parents=True, exist_ok=True)  
    df.to_csv(filepath,index=False)


def add_weather_data(raw_visitor_df,weather_df):
    augmented_df = raw_visitor_df.copy()
    augmented_df['temperature'] = weather_df.loc[0,'temperature']
    augmented_df['windspeed'] = weather_df.loc[0,'windspeed']
   
    augmented_df['weather_code'] = weather_df.loc[0,'weathercode']      
    return augmented_df

     
def fetch_data(weather_helper : WeatherHelper , visitior_helper : VisitorHelper):
    #get data from API
    raw_visitor_df = visitior_helper.invoke_visitor_api()
    #save raw file
    write_csv_file(raw_visitor_df,'./data/archive/raw/visitor_df')
    
    #get current weather
    weather_df = weather_helper.invoke_weather_api()
    
    #add to visitor data
    augmented_df = add_weather_data(raw_visitor_df,weather_df)
    
    augmented_df['weather_desc'] = weather_helper.get_weather_desc(augmented_df.loc[0,'weather_code'])

    
    #fillna for ltr or rtl
    augmented_df['ltr'] = augmented_df['ltr'].fillna(0)
    augmented_df['rtl'] = augmented_df['rtl'].fillna(0)
    
    #write augmented data 
    write_csv_file(augmented_df,'./data/archive/augmented/visitor_df')
    
    #TODO add lattitude and longitude 
    return augmented_df


def import_data():
    LOG.info('Starting RUN...')
    db_helper = DBHelper()
    weather_helper = WeatherHelper()
    visitior_helper = VisitorHelper()
    
    
    while True:
        logging.info(f'Fetching data as {datetime.now()}')    
        df = fetch_data(weather_helper,visitior_helper)    
        
        #log data
        LOG.info(df)
        
        #insert data
        db_helper.insert_data(df)
        
        #update star schema
        db_helper.update_star()
        time.sleep((60*12))
