#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Config file for AeroCom PhaseIII test project
"""
from pyaerocom.io.aux_read_cubes import (add_cubes, subtract_cubes,
                                         divide_cubes, multiply_cubes,
                                         compute_angstrom_coeff_cubes)
from pyaerocom.molmasses import *


def mmr_from_vmr(cube):
    """
    Convvert gas volume/mole mixing ratios into mass mixing ratios.
    Parameters
    ----------
    cube : iris.cube.Cube
        A cube containing gas vmr data to be converted into mmr.
    Returns
    -------
    cube_out : iris.cube.Cube
        Cube containing mmr data.
    """
    var_name = cube.var_name
    M_dry_air = get_molmass('air_dry')
    M_variable = get_molmass(var_name)

    cube_out = (M_variable/M_dry_air)*cube
    return cube_out


def conc_from_vmr_STP(cube):
    R = 287.058 #R for dry air

    standard_T = 293
    standard_P = 101300

    mmr_cube = mmr_from_vmr(cube)
    rho = R*standard_T/standard_P

    cube_out = rho*mmr_cube
    return cube_out

M_N = 14.006
M_O = 15.999
M_H = 1.007

def calc_O3_drydep_velocity(dryo3,vmro3):
    conco3 = conc_from_vmr_STP(vmro3.cube)
    dryvelo3 = divide_cubes(dryo3.cube, conco3)*(100/86400)
    dryvelo3.attributes['ts_type'] = vmro3.ts_type
    dryvelo3.units = 'cm s-1'
    return dryvelo3

def calc_O3_drydep_velocity_stub(dryo3,vmro3):
    pass

def calc_concnh3(vmrnh3):
    if vmrnh3.units == '1e-9':
        vmrnh3.units = 'ppb'
    assert vmrnh3.units == 'ppb'

    in_ts_type = vmrnh3.ts_type

    concnh3 = conc_from_vmr_STP(vmrnh3.cube)
    concnh3.units = 'ug/m3'
    concnh3 *= M_N / (M_N + M_H * 3)
    concnh3.units = 'ug N m-3'

    concnh3.attributes['ts_type']=in_ts_type

    return concnh3

def calc_concnh4(concnh4):
    if concnh4.units == 'ug m-3' or concnh4.units == 'ug/m**3':
        concnh4.units = 'ug/m3'
    assert concnh4.units == 'ug/m3'

    in_ts_type = concnh4.ts_type
    concnh4 = concnh4.cube
    concnh4 *= M_N / (M_N + M_H * 4)
    concnh4.units = 'ug N m-3'

    concnh4.attributes['ts_type']=in_ts_type

    return concnh4

def calc_conchno3(vmrhno3):
    if vmrhno3.units == '1e-9':
        vmrhno3.units == 'ppb'
    assert vmrhno3.units == 'ppb'

    in_ts_type = vmrhno3.ts_type
    conchno3 = conc_from_vmr_STP(vmrhno3.cube)
    conchno3.units = 'ug/m3'
    conchno3 *= M_N / (M_H + M_N + M_O * 3)
    conchno3.attributes['ts_type']=in_ts_type
    conchno3.units = 'ug N m-3'

    return conchno3

def calc_concglyoxal(vmrglyoxal):
    in_ts_type = vmrglyoxal.ts_type
    concglyoxal = conc_from_vmr_STP(vmrglyoxal.cube)
    concglyoxal.units = 'ug/m3'
    concglyoxal.attributes['ts_type']=in_ts_type
    concglyoxal.units = 'ug m-3'

    return concglyoxal

def calc_conchcho(vmrhcho):
    if vmrhcho.units == '1e-9':
        vmrhcho.units == 'ppb'
    # assert vmrhcho.units == 'ppb'

    in_ts_type = vmrhcho.ts_type
    conchcho = conc_from_vmr_STP(vmrhcho.cube)
    conchcho.units = 'ug/m3'
    conchcho.attributes['ts_type']=in_ts_type
    conchcho.units = 'ug m-3'

    return conchcho

def calc_fine_concno310(concno3f):
    return calc_concno310(concno3f=concno3f,concno3c=None)

def calc_concno310(concno3c, concno3f):
    if concno3c != None:
        if concno3c.units == 'ug m-3' or concno3c.units == 'ug/m**3':
            concno3c.units = 'ug/m3'
        assert concno3c.units == 'ug/m3'
    assert concno3f.units == 'ug/m3'

    in_ts_type = concno3f.ts_type
    if concno3c != None:
        concno310 = add_cubes(concno3f.cube, concno3c.cube)
    else:
        concno310 = concno3f.cube

    concno310 *= M_N / (M_N + M_O * 3)
    concno310.attributes['ts_type']=in_ts_type
    concno310.units = 'ug N m-3'
    return concno310

def calc_concno325(concno3f):
    assert concno3f.units == 'ug/m3'
    in_ts_type = concno3f.ts_type
    concno325 = concno3f.cube
    concno325 *= M_N / (M_N + M_O * 3)

    concno325.attributes['ts_type']=in_ts_type
    concno325.units = 'ug N m-3'
    return concno325

def calc_fine_conctno3(concno3f, vmrhno3):
    return calc_conctno3(concno3f=concno3f, concno3c=None, vmrhno3=vmrhno3)

def calc_concNtno3(conchno3,concno3f,concno3c):
    if concno3c != None:
        if concno3c.units == 'ug m-3':
            concno3c.units = 'ug/m3'
        assert concno3c.units == 'ug/m3'


    # in_ts_type = conchno3.ts_type
    if concno3c != None:
        concno3 = add_cubes(concno3f, concno3c)
    else:
        concno3 = concno3f

    concno3 *= M_N / (M_N + M_O * 3)
    conchno3 *= M_N / (M_H + M_N + M_O * 3)
    conctno3 = add_cubes(concno3,conchno3)
    # conctno3.attributes['ts_type']=in_ts_type
    conctno3.units = 'ug N m-3'
    return conctno3


def calc_concNtnh(concnh3,concnh4):
    if concnh4.units == 'ug m-3' or concnh4.units == 'ug/m**3':
        concnh4.units = 'ug/m3'
    assert concnh4.units == 'ug/m3'
    assert concnh3.units == 'ug/m3'

    # concnh3.units = 'ug/m3'
    concnh3 *= M_N / (M_N + M_H * 3)
    in_ts_type = concnh4.ts_type
    concnh4 = concnh4
    concnh4 *= M_N / (M_N + M_H * 4)
    conctnh = add_cubes(concnh3, concnh4)
    # conctnh.attributes['ts_type']=in_ts_type
    conctnh.units = 'ug N m-3'
    return conctnh

def calc_concec(concecff,concecbb):
    concec = add_cubes(concecff.cube,concecbb.cube)
    concec.attributes['ts_type'] = concecff.ts_type
    concec.units = 'ug C m-3'
    return concec

def calc_conccoc(concom25):
    conccoc = concom25.cube
    conccoc = conccoc/1.4
    conccoc.attributes['ts_type'] = concom25.ts_type
    conccoc.units = 'ug C m-3'
    return conccoc

def calc_concCoc25(concCoc25):
    # assert 'C' in str(concCoc25.units)
    # assert 'ug' in str(concCoc25.units)
    in_ts_type = concCoc25.ts_type
    concCoc25 = concCoc25.cube
    concCoc25.attributes['ts_type'] = in_ts_type
    concCoc25.units = 'ug C m-3'
    return concCoc25

def calc_concss25corr(concss10,concss25):
    concss25_from10 = concss10.cube
    concss25_from10 = concss25_from10*0.173

    concss25corr = add_cubes(concss25.cube,concss25_from10)
    concss25corr.attributes['ts_type'] = concss25.ts_type
    concss25corr.units = 'ug m-3'
    return concss25corr

def calc_concsspm25(concss25):
    in_ts_type = concss25.ts_type
    concsspm25 = concss25.cube
    concsspm25.attributes['ts_type'] = in_ts_type
    concsspm25.units = 'ug m-3'
    return concsspm25
    
def calc_precip(pr):
    ts_type = pr.ts_type
    precip = pr.cube
    precip.attributes['ts_type'] = ts_type
    precip.units = 'mm'
    return precip

from pyaerocom import GriddedData

def calc_aod_from_species_contributions(*gridded_objects):

    data = gridded_objects[0].cube

    assert str(data.units) == '1'

    for obj in gridded_objects[1:]:
        assert str(obj.units) == '1'
        data = add_cubes(data, obj.cube)

    return data




# FUNS = {'add_cubes'           : add_cubes,
#         'subtract_cubes'      : subtract_cubes,
#         'divide_cubes'        : divide_cubes,
#         'multiply_cubes'      : multiply_cubes,
#         'calc_ae'             : compute_angstrom_coeff_cubes,
#         'calc_conctno3'       : calc_conctno3,
#         'calc_fine_conctno3'  : calc_fine_conctno3,
#         'calc_conctnh'        : calc_conctnh,
#         'calc_concnh3'        : calc_concnh3,
#         'calc_concnh4'        : calc_concnh4,
#         'calc_conchno3'       : calc_conchno3,
#         'calc_concno310'       : calc_concno310,
#         'calc_fine_concno310'  : calc_fine_concno310,
#         'calc_concno325'      : calc_concno325,
#         'calc_concec'               : calc_concec,
#         'calc_concss25corr'   : calc_concss25corr,
#         'calc_concsspm25'     : calc_concsspm25,
#         'calc_conccoc'        : calc_conccoc,
#         'calc_concCoc25'      : calc_concCoc25,
#         'calc_precip'         : calc_precip,
#         'calc_concglyoxal'    : calc_concglyoxal,
#         'calc_conchcho'       : calc_conchcho,
#         'calc_O3_drydep_velocity':calc_O3_drydep_velocity,
#         'calc_aod_from_species_contributions' : calc_aod_from_species_contributions}
