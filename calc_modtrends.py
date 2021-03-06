#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 23 09:32:56 2021

@author: hansb
"""
import os, socket, tqdm
import numpy as np
import pandas as pd
import iris
import cf_units
import pyaerocom as pya
from pyaerocom.trends_helpers import SEASONS


def dummy(cube):
    return cube

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

HOSTNAME = socket.gethostname()

if HOSTNAME == 'pc5302':
    preface = '/home/hansb'
else:
    preface = '/'
    
PATHS = {
    '1999-2016' : f'{preface}/lustre/storeB/project/fou/kl/emep/ModelRuns/2019_REPORTING/TRENDS/',
    '2017'      : f'{preface}/lustre/storeB/project/fou/kl/emep/ModelRuns/2019_REPORTING/EMEP01_L20EC_rv4_33.2017',
    '2018'      : f'{preface}/lustre/storeB/project/fou/kl/emep/ModelRuns/2020_REPORTING/EMEP01_rv4_35_2018_emepCRef2',
    '2019'      : f'{preface}/lustre/storeB/project/fou/kl/emep/ModelRuns/2020_REPORTING/EMEP01_rv4_35_2019_tnoCRef2'    
    }
# path = f'{preface}lustre/storeB/project/fou/kl/emep/ModelRuns/2019_REPORTING/TRENDS/'
# years = range(1999,2020)
years = [2016,2017]

var_info = {'concpm10':{'units':'ug m-3','data_freq':'day'},
            'concpm25':{'units':'ug m-3','data_freq':'day'},
            'concno2':{'units':'ug m-3','data_freq':'day'},
            'concso4':{'units':'ug m-3','data_freq':'day'},
            }

EMEP_VARS = [
             'concpm10',
              # 'concpm25',
              # 'concno2',
              # 'concso4',
             ]

#example syntax. Not implemented yet
CALCULATE_HOW = {'concox':{'req_vars':['conco3','concno2'],
                           'function':pya.io.aux_read_cubes.add_cubes}}

if __name__ == '__main__':

    for var in EMEP_VARS:
        print(f'Processing {var}')
        try:
            site_info = pd.read_csv(f'obs_output/sitemeta_{var}.csv',index_col=0)
        except FileNotFoundError:
            print(f'No sitemeta file found for {var}, skipping...')
            continue
        
        try:
            calculate_how = CALCULATE_HOW[var]
        except KeyError:
            calculate_how = {'req_vars' : [var],'function' : dummy}
        
        data_freq =  var_info[var]['data_freq']
        data = []
        for year in years:
            if year < 2017:
                path = PATHS['1999-2016']
                data_id = f'{path}/{year}/Base_{data_freq}.nc'
            elif year == 2017:
                path = PATHS['2017']
                data_id = f'{path}/Base_{data_freq}.nc'
            elif year == 2018:
                path = PATHS['2018']
                data_id = f'{path}/Base_{data_freq}.nc'
            elif year == 2019:
                path = PATHS['2019']
                data_id = f'{path}/Base_{data_freq}.nc'
            else:
                raise ValueError
        
            reader = pya.io.ReadMscwCtm(data_id)
            

            temp_data = []    
            for req_var in calculate_how['req_vars']:
                temp = reader.read_var(req_var)
                if temp.cube.units == 'unknown':
                    temp.cube.units=var_info[req_var]['units']
                tcoord = temp.cube.coords('time')[0]
                if tcoord.units.calendar == 'proleptic_gregorian':
                    tcoord.units = cf_units.Unit(tcoord.units.origin, calendar='gregorian')
                
                temp_data.append(temp.cube)
            
            calc_temp = calculate_how['function'](*temp_data)
            data.append(calc_temp)
            
        concatenated = pya.GriddedData(pya.io.iris_io.concatenate_iris_cubes(iris.cube.CubeList(data),True))
        
        longitudes = list(site_info['longitude'])
        latitudes = list(site_info['latitude'])
        
        station_metadata = {'station_id':site_info['station_id'],
                            'station_name':site_info['station_name'],
                            'latitude':site_info['latitude'],'longitude':site_info['longitude'],
                            'altitude':site_info['altitude']}
        
        station_data = concatenated.to_time_series(longitude=longitudes,latitude=latitudes,add_meta=station_metadata)
        
        del concatenated
        
        tst = 'monthly'
        trendtab =  []
        for site in station_data:
            try:
                site = site.resample_time(
                    var_name=var,
                    ts_type=tst,
                    how=DEFAULT_RESAMPLE_HOW)
            except pya.exceptions.TemporalResolutionError:
                continue # lower res than monthly
            
            te = pya.trends_engine.TrendsEngine
            ts = site[var].loc[start_yr:stop_yr]
            
            subdir = os.path.join(OUTPUT_DIR, f'data_{var}')
            
            site_id = site.station_id
            os.makedirs(subdir, exist_ok=True)
            fname = f'data_{var}_{site_id}_{tst}.csv'

            siteout = os.path.join(subdir, fname)
            ts.to_csv(siteout)
            
            unit = str(site.var_info[var]['units'])
            
            for (start,stop,min_yrs) in PERIODS:
                for seas in SEASONS:
                    trend = te.compute_trend(ts, tst, start, stop, min_yrs,
                                              seas)

                    row = [var, site_id, trend['period'], trend['season'],
                            trend[f'slp_{start}'], trend[f'slp_{start}_err'],
                            trend[f'reg0_{start}'], trend['m'], trend['m_err'],
                            trend['n'], trend['pval'], unit]

                    trendtab.append(row)                    

        

        trenddf = pd.DataFrame(trendtab,
                               columns=['var',
                                       'station_id',
                                       'period',
                                       'season',
                                       'trend [%/yr]',
                                       'trend err [%/yr]',
                                       'yoffs',
                                       'slope',
                                       'slope err',
                                       'num yrs',
                                       'pval',
                                       'unit'
                                       ])

        trendout = os.path.join(OUTPUT_DIR, f'trends_{var}.csv')

        trenddf.to_csv(trendout)