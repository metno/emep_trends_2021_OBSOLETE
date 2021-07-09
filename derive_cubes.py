"""
Module for calculation of derived variables from EMEP variables
"""
from pyaerocom.io.aux_read_cubes import add_cubes
from pyaerocom.molmasses import get_molmass

# Molar masses of Nitrogen, Oxygen and Hydrogen
M_N = 14.006
M_O = 15.999
M_H = 1.007


def mmr_from_vmr(cube):
    """
    Convert gas volume/mole mixing ratios into mass mixing ratios

    cube.var_name is used to look up the molar mass of the variable.
    The air is assumed to be dry.

    NB: Units are not verified by this function, since the applied factor
    is dimensionless. var_name and units will not be preserved in the
    returned cube. Make sure to update these afterwards with the correct values.

    Parameters
    ----------
    cube : iris.cube.Cube
        A cube containing gas vmr data to be converted into mmr

    Returns
    -------
    cube_out : iris.cube.Cube
        Cube containing mmr data. NB: Will lack proper var_name and units
        attributes
    """
    var_name = cube.var_name
    M_dry_air = get_molmass('air_dry')
    M_variable = get_molmass(var_name)

    cube_out = (M_variable/M_dry_air) * cube
    return cube_out


def conc_from_vmr_STP(cube):
    """
    Convert from ppb to ug/m3 using standard temperaure and pressure

    cube.var_name is used to look up molar mass of the variable.
    The air is assumed to be dry

    Parameters
    ----------
    cube: iris.cube.Cube
        A cube containing gas in ppb units to be converted to ug/m3.
        Its var_name should start with 'vmr'

    Returns
    iris.cube.Cube
        cube transformed to units of ug/m3. var_name is updated so it start
        with 'conc' instead of 'vmr' and the units are also updated.
    """
    R = 287.058  # gas constant of dry air
    standard_T = 293
    standard_P = 101300

    cube_name = str(cube.var_name)
    assert(cube_name.startswith('vmr'))
    assert cube.units == 'ppb'
    out_cube_name = ''.join(['conc', cube_name[3:]])

    mmr_cube = mmr_from_vmr(cube)

    rho = standard_P / (R*standard_T)  # air density (kg/m3) in standard conditions

    cube_out = rho*mmr_cube
    cube_out.var_name = out_cube_name
    cube_out.units = 'ug m-3'
    return cube_out


def calc_concNtnh(concnh3, concnh4):
    """
    Calculate total reduced nitrogen in ug N m-3 from nh3 and nh4 in ug/m3

    Parameters
    ----------
    concnh3 : iris.cube.Cube
        NH3 concentration in units og ug/m3
    concnh4 : iris.cube.Cube
        NH4+ concentration in units of ug/m3

    Returns
    -------
    iris.cube.Cube
        Total nitrate concentration in ug N m-3,
        i.e. converting NH3 and NH4 to ug N m-3 and then adding them
    """
    assert concnh4.units == 'ug/m3'
    assert concnh3.units == 'ug/m3'

    nh3_fac = M_N / (M_N + M_H * 3)
    nh4_fac = M_N / (M_N + M_H * 4)
    concNtnh = add_cubes(concnh3*nh3_fac, concnh4*nh4_fac)
    concNtnh.var_name = 'concNtnh'
    concNtnh.units = 'ug N m-3'
    return concNtnh


def calc_concNtno3(conchno3, concno3f, concno3c):
    """
    Calculate total nitrate in ug N m-3 from ug m-3 of HNO3, NO3f and NO3c

    Parameters
    ----------
    conchno3 : iris.cube.Cube
        HNO3 concentration in ug/m3
    concno3f : iris.cube.Cube
        NO3- concentration in fine particles in ug/m3
    concno3c : iris.cube.Cube
        NO3- concentration in coarse particles in ug/m3

    Returns
    -------
    iris.cube.Cube
        Total nitrate concentration in ug N m-3
    """
    assert conchno3.units == 'ug/m3'
    assert concno3f.units == 'ug/m3'
    assert concno3c.units == 'ug/m3'

    hno3_fac = M_N / (M_N + M_H + M_O * 3)
    no3_fac = M_N / (M_N + M_O * 3)
    concno3 = add_cubes(concno3f, concno3c)
    concNtno3 = add_cubes(conchno3*hno3_fac, concno3*no3_fac)
    concNtno3.var_name = 'concNtno3'
    concNtno3.units = 'ug N m-3'
    return concNtno3


def calc_concNnh3(concnh3):
    """
    Convert NH3 concentration from ug/m3 to ug N m-3

    Parameters
    ----------
    concnh3 : iris.cube.Cube
        NH3 concentration in units of ug/m3

    Returns
    -------
    iris.cube.Cube
        NH3 concentration in units of ug N m-3
    """
    assert concnh3.units == 'ug/m3'

    fac = M_N / (M_N + 3*M_H)
    concNnh3 = concnh3*fac
    concNnh3.var_name = 'concNnh3'
    concNnh3.units = 'ug N m-3'
    return concNnh3


def calc_concNnh4(concnh4):
    """
    Convert NH4+ concentration from ug/m3 to ug N m-3

    Parameters
    ----------
    concnh4 : iris.cube.Cube
        NH4+ concentration in units of ug/m3

    Returns
    -------
    iris.cube.Cube
        NH4+ concentration in units of ug N m-3
    """
    assert concnh4.units == 'ug/m3'

    fac = M_N / (M_N + 4*M_H)
    concNnh4 = concnh4*fac
    concNnh4.var_name = 'concNnh4'
    concNnh4.units = 'ug N m-3'
    return concNnh4


def calc_concNhno3(conchno3):
    """
    Convert HNO3 concentration from ug/m3 to ug N m-3

    Parameters
    ----------
    conchno3 : iris.cube.Cube
        HNO3 concentration in units of ug/m3

    Returns
    -------
    iris.cube.Cube
        HNO3 concentration in units of ug N m-3
    """
    assert conchno3.units == 'ug/m3'

    fac = M_N / (M_N + M_H + 3*M_O)
    concNhno3 = conchno3*fac
    concNhno3.var_name = 'concNhno3'
    concNhno3.units = 'ug N m-3'
    return concNhno3


def calc_concNno3pm25(concno3f, concno3c):
    """
    Calculate total nitrate in PM2.5 in ug N m-3 from fine and coarse in ug/m3

    Parameters
    ----------
    concno3f : iris.cube.Cube
        NO3- concentration in fine particles in ug/m3.
        All of this is assumed to be in particles smaller than 2.5 um
    concno3c : iris.cube.Cube
        NO3- concentration in coarse particles in ug/m3.
        It is assumed that 13.4 % of this mass is in particles smaller than
        2.5 um

    Returns
    -------
    iris.cube.Cube
        NO3- concentration in particles smaller than 2.5 um, in ug N m-3
    """
    assert concno3f.units == 'ug/m3'
    assert concno3c.units == 'ug/m3'

    fac = M_N / (M_N + 3*M_O)
    frac_no3c_pm25 = 0.134
    concno3pm25 = add_cubes(concno3f, concno3c*frac_no3c_pm25)
    concNno3pm25 = concno3pm25*fac
    concNno3pm25.var_name = 'concNno3pm25'
    concNno3pm25.units = 'ug N m-3'
    return concNno3pm25


def calc_concNno3pm10(concno3f, concno3c):
    """
    Calculate total nitrate in PM10 in ug N m-3 from fine and coarse in ug/m3

    Parameters
    ----------
    concno3f : iris.cube.Cube
        NO3- concentration in fine particles in ug/m3.
        All of this is assumed to be in particles smaller than 10 um
    concno3c : iris.cube.Cube
        NO3- concentration in coarse particles in ug/m3.
        All of this is assumed to be in particles smaller than 10 um
    """
    assert concno3f.units == 'ug/m3'
    assert concno3c.units == 'ug/m3'

    fac = M_N / (M_N + 3*M_O)
    concno3pm10 = add_cubes(concno3f, concno3c)
    concNno3pm10 = concno3pm10*fac
    concNno3pm10.var_name = 'concNno3pm10'
    concNno3pm10.units = 'ug N m-3'
    return concNno3pm10
