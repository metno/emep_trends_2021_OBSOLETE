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

EBAS_LOCAL = '/home/jonasg/MyPyaerocom/data/obsdata/EBASMultiColumn/data'
EBAS_ID = 'EBASMC'

DEFAULT_RESAMPLE_CONSTRAINTS = dict(yearly     =   dict(daily      = 270),
                                    daily       =   dict(hourly     = 18))

RESAMPLE_HOW = dict(daily       =   dict(hourly='max'))

PERECENTILES = [10, 50, 75, 95, 98, 99]
def get_rs_how(percentile):
    rs_how = {**RESAMPLE_HOW}
    rs_how['yearly'] = dict(daily=f'{percentile}percentile')
    return rs_how


PERIODS = [(2000, 2019, 14),
           (2000, 2010, 7),
           (2010, 2019, 7)]

EBAS_VARS = ['conco3']

EBAS_BASE_FILTERS = dict(set_flags_nan   = True,
                         data_level      = 2)

OUTPUT_DIR = 'obs_output'

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
        trendtab = []

        data = oreader.read(vars_to_retrieve=var)
        data = data.apply_filters(**EBAS_BASE_FILTERS)
        #data = data.apply_filters(station_name='Birkenes II')

        sitedata = data.to_station_data_all(var,
                                            resample_how=RESAMPLE_HOW,
                                            min_num_obs=DEFAULT_RESAMPLE_CONSTRAINTS)

        for site in tqdm.tqdm(sitedata['stats'], desc=var):
            tst = 'daily'
            try:
                site = site.resample_time(
                    var_name=var,
                    ts_type=tst,
                    min_num_obs=DEFAULT_RESAMPLE_CONSTRAINTS,
                    how=RESAMPLE_HOW)
            except pya.exceptions.TemporalResolutionError:
                continue # lower res than monthly

            ts = site[var].loc[start_yr:stop_yr]
            if len(ts) == 0 or np.isnan(ts).all(): # skip
                continue

            subdir = os.path.join(OUTPUT_DIR, f'data_{var}')

            site_id = site.station_id
            os.makedirs(subdir, exist_ok=True)
            fname = f'{var}_{site_id}_{tst}.csv'

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

            tst = 'yearly'
            for percentile in PERECENTILES:
                rs_how = get_rs_how(percentile)
                try:
                    site = site.resample_time(
                        var_name=var,
                        ts_type=tst,
                        min_num_obs=DEFAULT_RESAMPLE_CONSTRAINTS,
                        how=rs_how)
                except pya.exceptions.TemporalResolutionError:
                    continue # lower res than monthly

                ts = site[var]
                if len(ts) == 0 or np.isnan(ts).all(): # skip
                    continue

                te = pya.trends_engine.TrendsEngine


                for (start, stop, min_yrs) in PERIODS:

                    trend = te.compute_trend(ts, tst, start, stop, min_yrs,
                                             'all')

                    row = [var, site_id, trend['period'], trend['season'],
                           trend[f'slp_{start}'], trend[f'slp_{start}_err'],
                           trend[f'reg0_{start}'], trend['m'], trend['m_err'],
                           trend['n'], trend['pval'], unit, percentile]

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
                                       'unit',
                                       'percentile'
                                       ])

        trendout = os.path.join(OUTPUT_DIR, f'trends_{var}.csv')

        trenddf.to_csv(trendout)







