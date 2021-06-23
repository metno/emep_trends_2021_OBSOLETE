#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 23 09:32:56 2021

@author: hansb
"""
import os, tqdm
import numpy as np
import pandas as pd
import iris
import cf_units
import pyaerocom as pya
from pyaerocom.trends_helpers import SEASONS


DEFAULT_RESAMPLE_HOW = 'mean'

PERIODS = [(2000, 2019, 14),
           (2000, 2010, 7),
           (2010, 2019, 7)]

OUTPUT_DIR = 'mod_output'

def get_first_last_year(periods):
    first=2100
    last=1900
    for st, end, _ in periods:
        if st < first:
            first = st
        if end > last:
            last = end
    return str(first-1), str(last+1)

start_yr, stop_yr = get_first_last_year(PERIODS)

preface = '/home/hansb'
PATHS = {
    '1999-2017' : f'{preface}/lustre/storeB/project/fou/kl/emep/ModelRuns/2019_REPORTING/TRENDS/',
    '2018'      : f'{preface}/lustre/storeB/project/fou/kl/emep/ModelRuns/2020_REPORTING/EMEP01_rv4_35_2018_emepCRef2',
    '2019'      : f'{preface}/lustre/storeB/project/fou/kl/emep/ModelRuns/2020_REPORTING/EMEP01_rv4_35_2019_tnoCRef2'    
    }
# path = f'{preface}lustre/storeB/project/fou/kl/emep/ModelRuns/2019_REPORTING/TRENDS/'
# years = range(2017,2020)
years = [2016,2018]

var_info = {'concpm10':{'units':'ug m-3'},
            'concpm25':{'units':'ug m-3'},
            'concno2':{'units':'ug m-3'},
            'concso4':{'units':'ug m-3'},
            }

EMEP_VARS = [
             'concpm10',
             'concpm25',
             'concno2',
             'concso4',
             ]

if __name__ == '__main__':

    for var in EMEP_VARS:
        site_info = pd.read_csv(f'obs_output/sitemeta_{var}.csv',index_col=0)
    
        data = []
        for year in years:
            if year < 2018:
                path = PATHS['1999-2017']
                data_id = f'{path}/{year}/Base_day.nc'
            elif year == 2018:
                path = PATHS['2018']
                data_id = f'{path}/Base_day.nc'
            elif year == 2019:
                path = PATHS['2019']
                data_id = f'{path}/Base_day.nc'
            else:
                raise ValueError
        
            reader = pya.io.ReadMscwCtm(data_id)
            temp = reader.read_var(var)
            if temp.cube.units == 'unknown':
                temp.cube.units=var_info[var]['units']
            tcoord = temp.cube.coords('time')[0]
            if tcoord.units.calendar == 'proleptic_gregorian':
                tcoord.units = cf_units.Unit(tcoord.units.origin, calendar='gregorian')
            
            data.append(temp.cube)
            
        concatenated = pya.GriddedData(pya.io.iris_io.concatenate_iris_cubes(iris.cube.CubeList(data),True))
        
        longitudes = list(site_info['longitude'])
        latitudes = list(site_info['latitude'])
        
        station_metadata = {'station_id':site_info['station_id'],
                            'station_name':site_info['station_name'],
                            'latitude':site_info['latitude'],'longitude':site_info['longitude'],
                            'altitude':site_info['altitude']}
        
        station_data = concatenated.to_time_series(longitude=longitudes,latitude=latitudes,add_meta=station_metadata)
        
        tst = 'monthly'
        for site in station_data:
            te = pya.trends_engine.TrendsEngine
            ts = site[var].loc[start_yr:stop_yr]
            
            subdir = os.path.join(OUTPUT_DIR, f'data_{var}')
            
            site_id = site.station_id
            os.makedirs(subdir, exist_ok=True)
            fname = f'data_{var}_{site_id}_{tst}.csv'

            siteout = os.path.join(subdir, fname)
            # ts.to_csv(siteout)
            
            unit = site.unit

        

