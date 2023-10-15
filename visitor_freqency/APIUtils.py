#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 13 15:21:58 2023

@author: pfox
"""
import pandas as pd
from pandas import json_normalize
import requests
import logging



WEATHER_URL='https://api.open-meteo.com/v1/forecast?latitude=47.0505&longitude=8.3064&current_weather=True'
VISITOR_API_URL = 'https://portal.alfons.io/app/devicecounter/api/sensors?api_key=3ad08d9e67919877e4c9f364974ce07e36cbdc9e'

WEATHER_DESC = {
    0:  'clear sky',
    1:  'mainly clear',
    2:  'partly cloudy',
    3:  'overcast',
    45: 'fog' ,
    46: 'depositing rime fog',
    51: 'light drizzle', 
    53: 'moderate drizzle',
    55: 'dense intensity drizzle',
    56: 'freezing drizzle',
    57: 'freezing dense intensity drizzle',
    61: 'slight rain',
    63: 'moderate rain',
    65: 'heavy rain',
    71: 'slight snow fall',
    73: 'moderate snow fall',
    75: 'heavy snow fall',
    77: 'snow grains',
    80: 'slight rain showers',
    81: 'moderate rain showers',
    82: 'violent rain showers',
    85: 'slight snow showers',
    86: 'heavy snow showers',
    95: 'slight or moderate thunderstorm',
    96: 'thunderstorm with slight hail',
    99: 'thunderstorm with heavy hail'
    }

class WeatherHelper:
    
    
    def __init__(self, weather_uri=WEATHER_URL):
       self._weather_uri = weather_uri 

    def invoke_weather_api(self):
        url = self._weather_uri
        logging.debug(f'calling weather API using url {url}')    
        response = requests.get(url)
        response_json = response.json()
        df = json_normalize(response_json['current_weather'])
        return df

    def get_weather_desc(self, weather_code: int):
        #the_code = int(weather_code)
        #print(the_code)
        #print(type(the_code))
        return WEATHER_DESC.get(weather_code, 'UNDEFINED WEATHER CODE')
         

class VisitorHelper:
    
    def __init__(self, visitor_api_uri=VISITOR_API_URL):
       self._visitor_api_uri = visitor_api_uri 

    '''
    def invoke_weather_api(self):
        url = self._weather_uri
        response = requests.get(url)
        response_json = response.json()
        df = json_normalize(response_json['current_weather'])
        return df
    '''
    
    def invoke_visitor_api(self):
        url = self._visitor_api_uri
        response = requests.get(url)
        response_json = response.json()
        df = json_normalize(response_json['data'])
        return df
    
