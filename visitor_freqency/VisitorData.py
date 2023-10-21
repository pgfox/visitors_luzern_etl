#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Modules fetches data from Visitors API and weather API and saves the data
to the DB.

@author: pfox
"""


import pandas as pd
import time
from pathlib import Path
from datetime import datetime  
from visitor_freqency.DBUtils import DBHelper
from visitor_freqency.APIUtils import WeatherHelper
from visitor_freqency.APIUtils import VisitorHelper


import logging
logging.basicConfig(level=logging.DEBUG)

LOG = logging.getLogger("visitors-logger")
fh = logging.FileHandler('logs/visitor_data_import_db.log')
fh.setLevel(logging.INFO)

formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)

LOG.addHandler(ch)
LOG.addHandler(fh)


def _write_csv_file(df : pd.DataFrame,location:str):
    '''
    write dataframe to a CSV file
    '''
    now = datetime.now()
    dt_string = now.strftime("%d_%m_%Y_%H_%M")
    filepath = Path(location+'_'+dt_string+'.csv')  
    filepath.parent.mkdir(parents=True, exist_ok=True)  
    df.to_csv(filepath,index=False)


def _add_weather_data(raw_visitor_df,weather_df):
    '''
    add weather data to visitor data
    '''
    augmented_df = raw_visitor_df.copy()
    augmented_df['temperature'] = weather_df.loc[0,'temperature']
    augmented_df['windspeed'] = weather_df.loc[0,'windspeed']
   
    augmented_df['weather_code'] = weather_df.loc[0,'weathercode']      
    return augmented_df

     
def _fetch_data(weather_helper : WeatherHelper , visitior_helper : VisitorHelper):
    '''
    fetch from Visitor API and weather API . Return dataframe with combined data
    '''
    
    raw_visitor_df = visitior_helper.invoke_visitor_api()
    #save raw file
    _write_csv_file(raw_visitor_df,'./data/archive/raw/visitor_df')
    
    #get current weather
    weather_df = weather_helper.invoke_weather_api()
    
    #add to visitor data
    augmented_df = _add_weather_data(raw_visitor_df,weather_df)
    
    augmented_df['weather_desc'] = weather_helper.get_weather_desc(augmented_df.loc[0,'weather_code'])

    
    #fillna for ltr or rtl
    augmented_df['ltr'] = augmented_df['ltr'].fillna(0)
    augmented_df['rtl'] = augmented_df['rtl'].fillna(0)
    
    #write augmented data 
    _write_csv_file(augmented_df,'./data/archive/augmented/visitor_df')
    
   
    return augmented_df


def import_data():
    '''
    a loop to fetch data from the API and then sleep for 12 minutess

    '''
    
    LOG.info('Starting import_data() operation.')
    db_helper = DBHelper()
    weather_helper = WeatherHelper()
    visitior_helper = VisitorHelper()
    
    
    while True:
        LOG.info('Running fetch and insert iteration')    
        df = _fetch_data(weather_helper,visitior_helper)    
        
        #log data
        LOG.debug(df)
        
        #insert data
        db_helper.insert_data(df)
        
        #update star schema
        db_helper.update_star()
        the_pause = 60*12
        LOG.info(f'sleeping for {(the_pause/60)} minutes') 
        time.sleep(the_pause)
