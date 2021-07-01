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


def dummy(cube):
    return cube

def not_implemented(cube):
    raise NotImplementedError

def read_model(var,paths,start_yr,stop_yr,var_info,calc_how):
    
    CALCULATE_HOW = calc_how
    
    print(f'Processing {var}')
    
    try:
        calculate_how = CALCULATE_HOW[var]
    except KeyError:
        calculate_how = {'req_vars' : [var],'function' : dummy}

    data_freq =  var_info[var]['data_freq']
    data = []
    
    years = range(int(start_yr),int(stop_yr))
    
    for year in tqdm.tqdm(years, desc=var):
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
    
    return concatenated

if __name__ == '__main__':
    
    start_yr = 2015
    stop_yr = 2018
    
    var_info = {'concpm10':{'units':'ug m-3','data_freq':'day'},
            'concpm25':{'units':'ug m-3','data_freq':'day'},
            'concno2':{'units':'ug m-3','data_freq':'day'},
            'concso4':{'units':'ug m-3','data_freq':'day'},
            'concox':{'units':'ug m-3','data_freq':'day'},
            'conco3':{'units':'ug m-3','data_freq':'day'},
            }

    EMEP_VARS = [
             'concpm10',
              # 'concpm25',
              # 'concno2',
              # 'concso4',
             ]

    CALCULATE_HOW = {'concox':{'req_vars':['conco3','concno2'],
                           'function':pya.io.aux_read_cubes.add_cubes}}
    
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

    model_data = read_model('concox',PATHS,start_yr,stop_yr,var_info,CALCULATE_HOW)