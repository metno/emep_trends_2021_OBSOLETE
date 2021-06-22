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

EBAS_LOCAL = '/home/jonasg/MyPyaerocom/data/obsdata/EBASMultiColumn/data'
EBAS_ID = 'EBASMC'

DEFAULT_RESAMPLE_CONSTRAINTS = dict(monthly     =   dict(daily      = 21),
                                    daily       =   dict(hourly     = 18))

DEFAULT_RESAMPLE_HOW = 'mean'

PERIODS = [(2000,2020, 14),
           (2000,2010, 7),
           (2010,2020, 7),]

EBAS_VARS = ['concso2',
             'conco3',
             'concso4',
             'concoc',
            'concCec',
            'conctc',
            'concss',
            'concnh3',
            'concnh4',
            'concNhno3',
            'concNtno3',
            'concNtnh',
            'concno2',
            'concpm10',
            'concpm25',
            'sc550dryaer',
            'ac550aer'
            ]

EBAS_BASE_FILTERS = dict(set_flags_nan   = True,
                         data_level      = 2)

OUTPUT_DIR = 'output'

def get_first_last_year(periods):
    first=2100
    last=1900
    for st, end, _ in periods:
        if st < first:
            first = st
        if end > last:
            last = end
    return str(first-1), str(last+1)

if __name__ == '__main__':
    if not os.path.exists(OUTPUT_DIR):
        os.mkdir(OUTPUT_DIR)

    if os.path.exists(EBAS_LOCAL):
        data_dir = EBAS_LOCAL
    else:
        # try use lustre...
        data_dir = None

    start_yr, stop_yr = get_first_last_year(PERIODS)

    oreader = pya.io.ReadUngridded(EBAS_ID, data_dirs=data_dir)


    for var in EBAS_VARS:
        sitemeta = []

        data = oreader.read(vars_to_retrieve=var)
        data = data.apply_filters(**EBAS_BASE_FILTERS)
        #data = data.apply_filters(station_name='Birkenes II')

        sitedata = data.to_station_data_all(var,
                                            resample_how=DEFAULT_RESAMPLE_HOW,
                                            min_num_obs=DEFAULT_RESAMPLE_CONSTRAINTS)
        tst = 'monthly'
        print(var)
        for site in tqdm.tqdm(sitedata['stats']):

            site = site.resample_time(var, tst,
                                      min_num_obs=DEFAULT_RESAMPLE_CONSTRAINTS,
                                      how=DEFAULT_RESAMPLE_HOW)
            ts = site[var].loc[start_yr:stop_yr]
            if len(ts) == 0 or np.isnan(ts).all(): # skip
                continue
            subdir = os.path.join(OUTPUT_DIR, f'data_{var}')
            os.makedirs(subdir, exist_ok=True)
            fname = f'data_{var}_{site.station_id}_{tst}.csv'

            siteout = os.path.join(subdir, fname)
            ts.to_csv(siteout)
            sitemeta.append([var,
                             site.station_id,
                             site.station_name,
                             site.latitude,
                             site.longitude,
                             site.altitude,
                             site.get_unit(var),
                             tst])

            te = pya.trends_engine.TrendsEngine
            resulttab = []


            for (start, stop, min_yrs) in PERIODS:
                for seas in SEASONS:
                    trend = te.compute_trend(ts, tst, start, stop, min_yrs,
                                             seas)

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






