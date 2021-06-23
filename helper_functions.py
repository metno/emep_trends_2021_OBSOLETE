#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 23 10:17:17 2021

@author: jonasg
"""
import os, shutil, glob

def clear_obs_output(outdir, var):
    files = glob.glob(f'{outdir}/*{var}*.csv')
    for file in files:
        os.remove(file)
    datadir = os.path.join(outdir, f'data_{var}')
    if os.path.exists(datadir):
        shutil.rmtree(datadir)

