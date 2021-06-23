#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 23 10:17:17 2021

@author: jonasg
"""
import os, shutil, glob

def delete_outdated_output(outdir, varlist):

    files = glob.glob(f'{outdir}/sitemeta*.csv')
    for file in files:
        fname = os.path.basename(file)
        var = fname.split('_')[-1].split('.csv')[0]
        if not var in varlist:
            clear_obs_output(outdir, var)

def clear_obs_output(outdir, var):

    files = glob.glob(f'{outdir}/*{var}*.csv')
    if len(files) > 0:
        print(f'delete output for {var} in {outdir}')
    for file in files:
        os.remove(file)

    datadir = os.path.join(outdir, f'data_{var}')
    if os.path.exists(datadir):
        shutil.rmtree(datadir)

def get_first_last_year(periods):
    first=2100
    last=1900
    for st, end, _ in periods:
        if st < first:
            first = st
        if end > last:
            last = end
    return str(first-1), str(last+1)

