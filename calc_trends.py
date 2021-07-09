#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 15 11:16:33 2021

@author: jonasg
"""
import os, socket, tqdm
import numpy as np
import pandas as pd
import pyaerocom as pya
from pyaerocom.trends_helpers import SEASONS

from helper_functions import (delete_outdated_output, clear_output,
                              get_first_last_year)
from read_mods import read_model
import derive_cubes as der

from variables import ALL_EBAS_VARS

SEASONS = ['all'] + list(SEASONS)

EBAS_LOCAL = '/home/jonasg/MyPyaerocom/data/obsdata/EBASMultiColumn/data'
EBAS_ID = 'EBASMC'

DEFAULT_RESAMPLE_CONSTRAINTS = dict(monthly     =   dict(daily      = 21, weekly = 3),
                                    daily       =   dict(hourly     = 18))

RELAXED_RESAMPLE_CONSTRAINTS = dict(monthly     =   dict(daily      = 4, weekly = 2),
                                    daily       =   dict(hourly     = 18))

DEFAULT_RESAMPLE_HOW = 'mean'

PERIODS = [(2000, 2019, 14),
           (2000, 2010, 7),
           (2010, 2019, 7),
           (2005, 2019, 10)]

EBAS_VARS = [
            # 'concno2',
            # 'concso2',
            # 'concco',
            # 'vmrc2h6',
            # 'vmrc2h4',
            # 'concpm25',
             'concpm10',
            # 'concso4',
            #'concNtno3',
            # 'concNtnh',
            # 'concNnh3',
            # 'concNnh4',
            # 'concNhno3',
            # 'concNno3pm25',
            # 'concNno3pm10',
            # 'concsspm25',
            # 'concsspm10',
            # 'concCecpm25',
            # 'concCocpm25',
            # 'conchcho',
            # 'wetoxs',
            # 'wetrdn',
            # 'wetoxn',
            # 'pr',
            # 'vmrisop',
            # 'concglyoxal',
            # 'conchcho',
            ]
EBAS_BASE_FILTERS = dict(set_flags_nan   = True,
                         #data_level      = 2
                           framework       = ['EMEP*', 'ACTRIS*'])

OBS_OUTPUT_DIR = 'obs_output'
MODEL_OUTPUT_DIR = 'mod_output'
#OBS_OUTPUT_DIR = '/home/eivindgw/testdata/obs_output'  #!!!!!! for testing
#MODEL_OUTPUT_DIR = '/home/eivindgw/testdata/mod_output'  #!!!!!!! for testing

dfreq = 'day'
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

HOSTNAME = socket.gethostname()

if "pc53" in HOSTNAME:
    preface = '/home/hansb'
else:
    preface = '/'


def get_modelfile(year, data_freq):
    if year < 2017 and year >= 1999:
        folder = f'{preface}/lustre/storeB/project/fou/kl/emep/ModelRuns/2019_REPORTING/TRENDS/{year}'
    elif year == 2017:
        folder = f'{preface}/lustre/storeB/project/fou/kl/emep/ModelRuns/2019_REPORTING/EMEP01_L20EC_rv4_33.2017'
    elif year == 2018:
        folder = f'{preface}/lustre/storeB/project/fou/kl/emep/ModelRuns/2020_REPORTING/EMEP01_rv4_35_2018_emepCRef2'
    elif year == 2019:
        folder = f'{preface}/lustre/storeB/project/fou/kl/emep/ModelRuns/2020_REPORTING/EMEP01_rv4_35_2019_tnoCRef2'
    else:
        raise ValueError(f'Location of model data for year {year} in not known')
    if data_freq not in ['hour', 'day', 'month']:
        raise ValueError('data_freq must be "hour", "day" or "month"')
    return os.path.join(folder, f'Base_{data_freq}.nc')


if __name__ == '__main__':
    if not os.path.exists(OBS_OUTPUT_DIR):
        os.mkdir(OBS_OUTPUT_DIR)
    if not os.path.exists(MODEL_OUTPUT_DIR):
        os.mkdir(MODEL_OUTPUT_DIR)

    if os.path.exists(EBAS_LOCAL):
        data_dir = EBAS_LOCAL
    else:
        # try use lustre...
        data_dir = None

    # clear outdated output variables
    delete_outdated_output(OBS_OUTPUT_DIR, ALL_EBAS_VARS)
    delete_outdated_output(MODEL_OUTPUT_DIR, ALL_EBAS_VARS)

    start_yr, stop_yr = get_first_last_year(PERIODS)
    #start_yr = '2015'; stop_yr = '2020'  #!!!!!!!!!! for testing
    print(start_yr, stop_yr)

    oreader = pya.io.ReadUngridded(EBAS_ID, data_dirs=data_dir)

    for var in EBAS_VARS:
        print('var=', var)
        if var not in ALL_EBAS_VARS:
            raise ValueError('invalid variable ', var, '. Please register'
                             'in variables.py')
        # delete former output for that variable if it exists
        clear_output(OBS_OUTPUT_DIR, var)
        clear_output(MODEL_OUTPUT_DIR, var)
        sitemeta = []
        obs_trendtab = []
        mod_trendtab = []

        data = oreader.read(vars_to_retrieve=var)
        data = data.apply_filters(**EBAS_BASE_FILTERS)
        #data = data.apply_filters(station_name='Birkenes II')
        mdata = read_model(var, get_modelfile, start_yr, stop_yr, EMEP_VAR_INFO, CALCULATE_HOW)

        #remove:
        # sitedata = data.to_station_data_all(var, start=int(start_yr)-1, stop=int(stop_yr)+1,
        #                                     resample_how=DEFAULT_RESAMPLE_HOW,
        #                                     min_num_obs=DEFAULT_RESAMPLE_CONSTRAINTS)
        coldata = pya.colocation.colocate_gridded_ungridded(
                    mdata, data, ts_type='monthly', start=start_yr, stop=stop_yr,
                    colocate_time=True, resample_how=DEFAULT_RESAMPLE_HOW,
                    min_num_obs=DEFAULT_RESAMPLE_CONSTRAINTS
                    )

        #loop over stations in colcated data
        sitelist = list(coldata.data.station_name.values)
        for site in tqdm.tqdm(sitelist, desc=var):
            tst = 'monthly'

            obs_site = coldata.data.sel(station_name=site).isel(data_source=0).to_series()
            mod_site = coldata.data.sel(station_name=site).isel(data_source=1).to_series()
            obs_ts = obs_site.loc[start_yr:stop_yr]
            mod_ts = mod_site.loc[start_yr:stop_yr]
            if len(obs_ts) == 0 or np.isnan(obs_ts).all(): # skip
                continue
            obs_subdir = os.path.join(OBS_OUTPUT_DIR, f'data_{var}')
            mod_subdir = os.path.join(MODEL_OUTPUT_DIR, f'data_{var}')

            sitedata_for_meta = data.to_station_data(
                site, var, start=int(start_yr)-1, stop=int(stop_yr)+1,
                resample_how=DEFAULT_RESAMPLE_HOW,
                min_num_obs=DEFAULT_RESAMPLE_CONSTRAINTS
            )

            site_id = sitedata_for_meta.station_id
            os.makedirs(obs_subdir, exist_ok=True)
            os.makedirs(mod_subdir, exist_ok=True)
            fname = f'data_{var}_{site_id}_{tst}.csv'

            obs_siteout = os.path.join(obs_subdir, fname)
            obs_ts.to_csv(obs_siteout)

            mod_siteout = os.path.join(mod_subdir, fname)
            mod_ts.to_csv(mod_siteout)

            unit = sitedata_for_meta.get_unit(var)
            sitemeta.append([var,
                             site_id,
                             sitedata_for_meta.station_name,
                             sitedata_for_meta.latitude,
                             sitedata_for_meta.longitude,
                             sitedata_for_meta.altitude,
                             unit,
                             tst,
                             sitedata_for_meta.framework,
                             sitedata_for_meta.var_info[var]['matrix']
                             ])

            # if tst == 'daily':
            #     site = site.resample_time(
            #         var_name=var,
            #         ts_type='monthly',
            #         min_num_obs=DEFAULT_RESAMPLE_CONSTRAINTS,
            #         how=DEFAULT_RESAMPLE_HOW)
            #     tst = 'monthly'

            te = pya.trends_engine.TrendsEngine


            for (start, stop, min_yrs) in PERIODS:
                for seas in SEASONS:
                    obs_trend = te.compute_trend(obs_ts, tst, start, stop, min_yrs,
                                             seas)

                    obs_row = [var, site_id, obs_trend['period'], obs_trend['season'],
                           obs_trend[f'slp_{start}'], obs_trend[f'slp_{start}_err'],
                           obs_trend[f'reg0_{start}'], obs_trend['m'], obs_trend['m_err'],
                           obs_trend['n'], obs_trend['pval'], unit]

                    obs_trendtab.append(obs_row)

                    mod_trend = te.compute_trend(mod_ts, tst, start, stop, min_yrs,
                                             seas)

                    mod_row = [var, site_id, mod_trend['period'], mod_trend['season'],
                           mod_trend[f'slp_{start}'], mod_trend[f'slp_{start}_err'],
                           mod_trend[f'reg0_{start}'], mod_trend['m'], mod_trend['m_err'],
                           mod_trend['n'], mod_trend['pval'], unit]

                    mod_trendtab.append(mod_row)

                    fname = f'{var}_{site_id}_{start}-{stop}_{seas}_yearly.csv'
                    try:
                        obs_trend['data'].to_csv(os.path.join(obs_subdir, fname))
                        mod_trend['data'].to_csv(os.path.join(mod_subdir, fname))
                    except AttributeError:
                        pass

        metadf = pd.DataFrame(sitemeta,
                              columns=['var',
                                       'station_id',
                                       'station_name',
                                       'latitude',
                                       'longitude',
                                       'altitude',
                                       'unit',
                                       'freq',
                                       'framework',
                                       'matrix'
                                       ])

        metaout = os.path.join(OBS_OUTPUT_DIR, f'sitemeta_{var}.csv')

        metadf.to_csv(metaout)

        obs_trenddf = pd.DataFrame(obs_trendtab,
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

        mod_trenddf = pd.DataFrame(mod_trendtab,
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

        obs_trendout = os.path.join(OBS_OUTPUT_DIR, f'trends_{var}.csv')
        obs_trenddf.to_csv(obs_trendout)

        mod_trendout = os.path.join(MODEL_OUTPUT_DIR, f'trends_{var}.csv')
        mod_trenddf.to_csv(mod_trendout)
        print('Processing of variable %s done.' % var)
