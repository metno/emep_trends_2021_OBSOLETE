#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 23 09:32:56 2021

@author: hansb
"""
import os, socket, tqdm
import iris
import cf_units
import pyaerocom as pya


def dummy(cube):
    return cube


def not_implemented(cube):
    raise NotImplementedError


def read_model(var, getfile, start_yr, stop_yr, var_info, calc_how={}):
    """
    Read a model variable from multiple annual EMEP runs

    Parameters
    ----------
    var : string
        Variable name (in pyaerocom).
    getfile : function (int, str) -> str
        Function to get full path to the model data file for a specified year.
        First input argument should be the year and second should be the data
        time frequency, e.g. 'day', 'month', 'hour'
    start_yr : string or int
        Start year as sting or int.
    stop_yr : string or int
        Stop year as sting or int.
    var_info : dict
        Dict of dicts of variable metadata/info. Needs to have a key for var
        which has at least the keys "units" and "data_freq".
        It is verified that the final cube has units equivalent to "units",
        and "data_freq" is used as input to "getfile".
    calc_how : dict, optional
        If var is a variable that can not be read directly from the model data,
        calc_how should be provided. The dict must then contain an entry for the
        variable you want to process which must be another dict with keys "req_vars"
        and "function". "req_vars" must be a list of variable names and "function"
        must be a fuction that calculates the variable and returns an iris.cubc.Cube
        object. This returned cube object must have properties "var_name"=var and
        units equivalent to var_info[var]['units'].

    Returns
    -------
    concatenated : pyaerocom.GriddedData
        GriddedData object containing the requested variable covering the requested
        time period.
    """
    print(f'Processing {var}')

    try:
        calculate_how = calc_how[var]
    except KeyError:
        calculate_how = {'req_vars': [var], 'function': dummy}

    data_freq = var_info[var]['data_freq']
    data = []

    years = range(int(start_yr), int(stop_yr))

    for year in tqdm.tqdm(years, desc=var):
        data_id = getfile(year, data_freq)

        reader = pya.io.ReadMscwCtm(data_id)

        temp_data = []
        for req_var in calculate_how['req_vars']:
            print('req_var=', req_var)
            temp = reader.read_var(req_var)
            tcoord = temp.cube.coords('time')[0]
            if tcoord.units.calendar == 'proleptic_gregorian':
                tcoord.units = cf_units.Unit(tcoord.units.origin, calendar='gregorian')
            temp_data.append(temp.cube)
        calc_temp = calculate_how['function'](*temp_data)
        data.append(calc_temp)

    concatenated = pya.GriddedData(pya.io.iris_io.concatenate_iris_cubes(iris.cube.CubeList(data), True))
    # verify final var_name and units
    assert concatenated.cube.var_name == var
    if concatenated.cube.units != var_info[var]['units']:
        error_str = ('Calculation of variable "%s" result in units "%s", not the expected units "%s"'
                     % (var, concatenated.cube.units, var_info[var]['units']))
        raise ValueError(error_str)

    return concatenated


if __name__ == '__main__':
    import derive_cubes as der

    start_yr = 2018
    stop_yr = 2019

    var = 'concCecpm25'
    dfreq = 'month'

    HOSTNAME = socket.gethostname()

    if HOSTNAME == 'pc5302':
        preface = '/home/hansb'
    else:
        preface = '/'

    EMEP_VAR_INFO = {
        'concno2': {'units': 'ug m-3', 'data_freq': dfreq},
        'concso2': {'units': 'ug m-3', 'data_freq': dfreq},
        'concco': {'units': 'ug m-3', 'data_freq': dfreq},
        'vmrc2h6': {'units': 'ppb', 'data_freq': dfreq},
        'vmrc2h4': {'units': 'ppb', 'data_freq': dfreq},
        'concpm25': {'units': 'ug m-3', 'data_freq': dfreq},
        'concpm10': {'units': 'ug m-3', 'data_freq': dfreq},
        # - skipping ozone for now
        'concso4': {'units': 'ug m-3', 'data_freq': dfreq},
        'concNtno3': {'units': 'ug N m-3', 'data_freq': dfreq},
        'concNtnh': {'units': 'ug N m-3', 'data_freq': dfreq},
        'concNnh3': {'unnits': 'ug N m-3', 'data_freq': dfreq},
        'concNnh4': {'units': 'ug N m-3', 'data_freq': dfreq},
        'concNhno3': {'units': 'ug N m-3', 'data_freq': dfreq},
        'concNno3pm25': {'units': 'ug N m-3', 'data_freq': dfreq},
        'concNno3pm10': {'units': 'ug N m-3', 'data_freq': dfreq},
        'concsspm25': {'units': 'ug m-3', 'data_freq': dfreq},
        'concss': {'units': 'ug m-3', 'data_freq': dfreq},
        'concCecpm25': {'units': 'ug C m-3', 'data_freq': dfreq},  # not working, lack equivalent variable in pyaerocom with units ug/m3, which is what model gives
        'concCocpm25': {'units': 'ug C m-3', 'data_freq': dfreq},  # not working, captialization problem in pyaerocom
        'conchcho': {'units': 'ug m-3', 'data_freq': dfreq},
        'wetoxs': {'units': 'mg S m-3', 'data_freq': dfreq},  # crashes due to model units "mgS/m2" not accepted by cf_units
        'wetrdn': {'units': 'mg N m-3', 'data_freq': dfreq},  # crashes due to model units "mgN/m2" not accepted by cf_units
        'wetoxn': {'units': 'mg N m-3', 'data_freq': dfreq},  # crashes, same reason as wetrdn
        # - skipping precipiation for now
        'vmrisop': {'units': 'ppb', 'data_freq': dfreq},
        'concglyoxal': {'units': 'ug m-3', 'data_freq': dfreq},
    }
    CALCULATE_HOW = {
        'concNtnh': {'req_vars': ['concnh3', 'concnh4'],
                     'function': der.calc_concNtnh},
        'concco': {'req_vars': ['vmrco'],
                   'function': der.conc_from_vmr_STP},
        'concNtno3': {'req_vars': ['conchno3', 'concno3f', 'concno3c'],
                      'function': der.calc_concNtno3},
        'concNnh3': {'req_vars': ['concnh3'],
                     'function': der.calc_concNnh3},
        'concNnh4': {'req_vars': ['concnh4'],
                     'function': der.calc_concNnh4},
        'concNhno3': {'req_vars': ['conchno3'],
                      'function': der.calc_concNhno3},
        'concNno3pm25': {'req_vars': ['concno3f', 'concno3c'],  # NB: fine before coarse!
                         'function': der.calc_concNno3pm25},
        'concNno3pm10': {'req_vars': ['concno3f', 'concno3c'],
                         'function': der.calc_concNno3pm10},
        'conchcho': {'req_vars': ['vmrhcho'],
                     'function': der.conc_from_vmr_STP},
        'concglyoxal': {'req_vars': ['vmrglyoxal'],
                        'function': der.conc_from_vmr_STP}
    }

    def get_modelfile(year, data_freq):
        if year < 2017 and year >= 1999:
            folder = f'{preface}/lustre/storeB/project/fou/kl/emep/ModelRuns/2019_REPORTING/TRENDS/{year}'
        elif year == 2017:
            folder = f'{preface}/lustre/storeB/project/fou/kl/emep/ModelRuns/2019_REPORTING/EMEP01_L20EC_rv4_33.2017'
        elif year == 2018:
            #folder = f'{preface}/lustre/storeB/project/fou/kl/emep/ModelRuns/2020_REPORTING/EMEP01_rv4_35_2018_emepCRef2'
            folder = f'{preface}/lustre/storeB/project/fou/kl/emep/ModelRuns/2021_REPORTING/TRENDS/2018'
        elif year == 2019:
            folder = f'{preface}/lustre/storeB/project/fou/kl/emep/ModelRuns/2020_REPORTING/EMEP01_rv4_35_2019_tnoCRef2'
            #folder = f'{preface}/lustre/storeB/project/fou/kl/emep/ModelRuns/2021_REPORTING/TRENDS/2019'
        else:
            raise ValueError(f'Location of model data for year {year} in not known')
        if data_freq not in ['hour', 'day', 'month']:
            raise ValueError('data_freq must be "hour", "day" or "month"')
        return os.path.join(folder, f'Base_{data_freq}.nc')

    mdata = read_model(var, get_modelfile, start_yr, stop_yr, EMEP_VAR_INFO, CALCULATE_HOW)
    print(mdata)
