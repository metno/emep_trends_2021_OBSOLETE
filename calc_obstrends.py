#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 15 11:16:33 2021

@author: jonasg
"""
import os, tqdm
import numpy as np
import pandas as pd
import pyaerocom as pya
from pyaerocom.trends_helpers import SEASONS

from helper_functions import (delete_outdated_output, clear_obs_output,
                              get_first_last_year)

from variables import ALL_EBAS_VARS

EBAS_LOCAL = '/home/jonasg/MyPyaerocom/data/obsdata/EBASMultiColumn/data'
EBAS_ID = 'EBASMC'

DEFAULT_RESAMPLE_CONSTRAINTS = dict(monthly     =   dict(daily      = 21),
                                    daily       =   dict(hourly     = 18))

DEFAULT_RESAMPLE_HOW = 'mean'

PERIODS = [(2000, 2019, 14),
           (2000, 2010, 7),
           (2010, 2019, 7)]

EBAS_VARS = [
# =============================================================================
#     'vmrno2',
#             'vmrno',
#             #'vmrox',
#             'vmrso2',
#             'vmrco',
#             'vmrc2h6',
#             'vmrc2h4',
#             'concpm25',
#             'concpm10',
#             #'conco3',
#             'concso4',
#             'concNtno3',
#             'concNtnh',
#             'concNnh3',
#             'concNnh4',
#             'concNhno3',
#             'concNno3pm25',
#             'concNno3pm10',
#             'concsspm25',
#             'concsspm10',
#             'concCecpm25',
#             'concCocpm25',
#             'conchcho',
#             'wetoxs',
#             'wetrdn',
#             'wetoxn',
# =============================================================================
            'pr'
                 ]
EBAS_BASE_FILTERS = dict(set_flags_nan   = True,
                         data_level      = 2)

OUTPUT_DIR = 'obs_output'

if __name__ == '__main__':
    if not os.path.exists(OUTPUT_DIR):
        os.mkdir(OUTPUT_DIR)

    if os.path.exists(EBAS_LOCAL):
        data_dir = EBAS_LOCAL
    else:
        # try use lustre...
        data_dir = None

    # clear outdated output variables
    delete_outdated_output(OUTPUT_DIR, ALL_EBAS_VARS)

    start_yr, stop_yr = get_first_last_year(PERIODS)

    oreader = pya.io.ReadUngridded(EBAS_ID, data_dirs=data_dir)


    for var in EBAS_VARS:
        if not var in ALL_EBAS_VARS:
            raise ValueError('invalid variable ', var, '. Please register'
                             'in variables.py')
        # delete former output for that variable if it exists
        clear_obs_output(OUTPUT_DIR, var)
        sitemeta = []
        trendtab = []

        data = oreader.read(vars_to_retrieve=var)
        data = data.apply_filters(**EBAS_BASE_FILTERS)
        #data = data.apply_filters(station_name='Birkenes II')

        sitedata = data.to_station_data_all(var,
                                            resample_how=DEFAULT_RESAMPLE_HOW,
                                            min_num_obs=DEFAULT_RESAMPLE_CONSTRAINTS)
        tst = 'monthly'
        for site in tqdm.tqdm(sitedata['stats'], desc=var):
            try:
                site = site.resample_time(
                    var_name=var,
                    ts_type=tst,
                    min_num_obs=DEFAULT_RESAMPLE_CONSTRAINTS,
                    how=DEFAULT_RESAMPLE_HOW)
            except pya.exceptions.TemporalResolutionError:
                continue # lower res than monthly

            ts = site[var].loc[start_yr:stop_yr]
            if len(ts) == 0 or np.isnan(ts).all(): # skip
                continue
            subdir = os.path.join(OUTPUT_DIR, f'data_{var}')

            site_id = site.station_id
            os.makedirs(subdir, exist_ok=True)
            fname = f'data_{var}_{site_id}_{tst}.csv'

            siteout = os.path.join(subdir, fname)
            ts.to_csv(siteout)
            unit = site.get_unit(var)
            sitemeta.append([var,
                             site_id,
                             site.station_name,
                             site.latitude,
                             site.longitude,
                             site.altitude,
                             unit,
                             tst])

            te = pya.trends_engine.TrendsEngine


            for (start, stop, min_yrs) in PERIODS:
                for seas in SEASONS:
                    trend = te.compute_trend(ts, tst, start, stop, min_yrs,
                                             seas)

                    row = [var, site_id, trend['period'], trend['season'],
                           trend[f'slp_{start}'], trend[f'slp_{start}_err'],
                           trend[f'reg0_{start}'], trend['m'], trend['m_err'],
                           trend['n'], trend['pval'], unit]

                    trendtab.append(row)

        metadf = pd.DataFrame(sitemeta,
                              columns=['var',
                                       'station_id',
                                       'station_name',
                                       'latitude',
                                       'longitude',
                                       'altitude',
                                       'unit',
                                       'freq'])

        metaout = os.path.join(OUTPUT_DIR, f'sitemeta_{var}.csv')

        metadf.to_csv(metaout)

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







