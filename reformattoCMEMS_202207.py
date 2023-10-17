import argparse
import os.path
import shutil
from datetime import datetime as dt
from datetime import timedelta

import numpy as np

from product_info import ProductInfo
import subprocess
from dataset_selection import DatasetSelection

parser = argparse.ArgumentParser(description='Reformat and upload 2DBS')
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
parser.add_argument('-check', "--check_param", help="Check params mode.", action="store_true")
parser.add_argument("-noupload", "--no_upload", help="No upload mode, only reformat.", action="store_true")
parser.add_argument("-noreformat", "--no_reformat", help="No reformat mode, only upload.", action="store_true")
parser.add_argument("-m", "--mode", help="Mode.", type=str, choices=['NRT', 'DT', 'MY'])
parser.add_argument("-r", "--region", help="Region.", type=str, choices=['BAL', 'MED', 'BLK', 'BS'])
parser.add_argument("-l", "--level", help="Level.", type=str, choices=['l3', 'l4'])
parser.add_argument("-d", "--dataset_type", help="Dataset.", type=str,
                    choices=['reflectance', 'plankton', 'optics', 'transp', 'pp'])
parser.add_argument("-s", "--sensor", help="Sensor.", type=str,
                    choices=['multi', 'olci', 'gapfree_multi', 'multi_climatology', 'cci'])
parser.add_argument("-sd", "--start_date", help="Start date (yyyy-mm-dd)")
parser.add_argument("-ed", "--end_date", help="Start date (yyyy-mm-dd)")
parser.add_argument("-pname", "--name_product", help="Product name")
parser.add_argument("-pfreq", "--frequency_product",
                    help="Select datasets of selected product (-pname) with this frequency", choices=['d', 'm', 'c'])
parser.add_argument("-dname", "--name_dataset", help="Product name")
parser.add_argument("-csize", "--size_file", help="Output file with size information. Files are deleted ")
parser.add_argument("-csizeopt", "--size_opt", help="Options to check file size without reformat",
                    choices=['olci_rrs', 'olci_plankton', 'olci_transp', 'olci_m_rrs', 'olci_m_plankton',
                             'olci_m_transp', 'multi_rrs', 'multi_plankton', 'multi_transp', 'multi_optics',
                             'multi_gapfree', 'multi_m_plankton'])

args = parser.parse_args()


def main():
    print('[INFO] STARTED REFORMAT AND UPLOAD')

    ##DATASETS SELECTION
    name_products, name_datasets = get_datasets()
    n_datasets = len(name_products)
    if n_datasets == 0:
        print(f'[ERROR] No datasets selected')
        return
    if not check_datasets(name_products, name_datasets):
        return
    if args.verbose:
        print(f'[INFO] Number of selected datasets: {n_datasets}')
        for idataset in range(n_datasets):
            print(f'[INFO]  {name_products[idataset]}/{name_datasets[idataset]}')

    # DATE SELECTION
    start_date, end_date = get_dates()
    if start_date is None or end_date is None:
        return
    if args.verbose:
        print(f'[INFO] Start date: {start_date} End date: {end_date}')

    if args.check_param:
        return

    pinfo = ProductInfo()
    for idataset in range(n_datasets):
        pinfo.set_dataset_info(name_products[idataset], name_datasets[idataset])
        if args.verbose:
            print(f'[INFO] Working with dataset: {name_products[idataset]}/{name_datasets[idataset]}')
        if pinfo.dinfo['frequency'] == 'd':
            if not args.size_opt:
                make_reformat_daily_dataset(pinfo, start_date, end_date, args.verbose)
            if args.size_file:
                file_size = args.size_file
                opt = None
                if args.size_opt:
                    opt = args.size_opt
                if args.verbose:
                    print(f'[INFO] Checking size...')
                df = pinfo.check_size_file_orig(start_date, end_date, opt, args.verbose)
                df.to_csv(file_size, sep=';')
                if not args.size_opt:
                    if args.verbose:
                        print(f'[INFO] Deleting...')
                    pinfo.delete_list_file_path_orig(start_date, end_date, args.verbose)
        if pinfo.dinfo['frequency'] == 'm':
            if not args.size_opt:
                make_reformat_monthly_dataset(pinfo, start_date, end_date, args.verbose)
            if args.size_file:
                file_size = args.size_file
                opt = None
                if args.size_opt:
                    opt = args.size_opt
                if args.verbose:
                    print(f'[INFO] Checking size...')
                if opt is not None:
                    df = pinfo.check_size_file_orig_monthly(start_date, end_date, opt, args.verbose)
                # else: NO IMPPLMENTED
                #     df = pinfo.check_size_file_orig(start_date,end_date,opt,args.verbose)
                df.to_csv(file_size, sep=';')
                if not args.size_opt:
                    if args.verbose:
                        print(f'[INFO] Deleting...')
                    pinfo.delete_list_file_path_orig(start_date, end_date, args.verbose)

        if pinfo.dinfo['frequency'] == 'c':

            if pinfo.get_region()=='MED' or pinfo.get_region()=='BLK':

                make_reformat_daily_dataset(pinfo,start_date,end_date,args.verbose)
            else:
                make_reformat_clima_dataset(pinfo, start_date, end_date, args.verbose)


##DATASET SELECTION
def get_datasets():
    # name_products = []
    # name_datasets = []
    dsel = DatasetSelection(args.mode)
    if args.name_product and args.name_dataset:
        name_products = [args.name_product]
        name_datasets = [args.name_dataset]
    elif args.name_product and not args.name_dataset:
        name_products, name_datasets = dsel.get_list_product_datasets_from_product_nane(args.name_product)
    elif not args.name_product and args.name_dataset:
        name_products, name_datasets = dsel.get_list_product_datasets_from_dataset_nane(args.name_dataset)
    else:
        dsel = DatasetSelection(args.mode)
        region, sensor, dataset_type, frequency, level = get_params_selection_dataset()
        dsel.set_params(region, level, dataset_type, sensor, frequency)
        name_products, name_datasets = dsel.get_list_product_datasets_from_params()

    return name_products, name_datasets


def get_params_selection_dataset():
    region = None
    sensor = None
    dataset_type = None
    frequency = None
    level = None
    if args.region:
        region = args.region
    if args.sensor:
        sensor = args.sensor
    if args.dataset_type:
        dataset_type = args.dataset_type
    if args.frequency_product:
        frequency = args.frequency_product
    if args.level:
        level = args.level
    return region, sensor, dataset_type, frequency, level


##DATASETS CHECKING
def check_datasets(name_products, name_datasets):
    mode_check = args.mode
    if args.mode == 'MYINT':
        mode_check = 'MY'
    if args.mode == 'DT':
        mode_check = 'NRT'
    pinfo = ProductInfo()
    for idataset in range(len(name_products)):
        valid = pinfo.set_dataset_info(name_products[idataset], name_datasets[idataset])
        if not valid:
            return False
        mode_here = pinfo.dinfo['mode']
        if mode_here != mode_check:
            print(
                f'[ERROR] Dataset {name_datasets[idataset]} is {mode_here},but script was launched in mode {mode_check}')
            return False
        region, sensor, dataset_type, frequency, level = get_params_selection_dataset()
        if region is not None:
            region_here = pinfo.dinfo['region']
            if region_here != region:
                print(f'[ERROR] Dataset region is {region_here} but {region} was given in the script')
                return False
        if sensor is not None:
            sensor_here = pinfo.dinfo['sensor'].lower()
            if sensor_here != sensor:
                print(f'[ERROR] Dataset sensor is {sensor_here} but {sensor} was given in the script')
                return False
        if dataset_type is not None:
            dataset_type_here = pinfo.dinfo['dataset'].lower()
            if dataset_type_here != dataset_type:
                print(f'[ERROR] Dataset dataset_type is {dataset_type_here} but {dataset_type} was given in the script')
                return False
        if frequency is not None:
            frequency_here = pinfo.dinfo['frequency'].lower()
            if frequency_here != frequency:
                print(f'[ERROR] Dataset frequency is {frequency_here} but {frequency} was given in the script')
                return False
        if level is not None:
            level_here = pinfo.dinfo['level'].lower()
            if level_here != level:
                print(f'[ERROR] Dataset level is {level_here} but {level} was given in the script')
                return False

    return True


##DATES SELECTION
def get_dates():
    start_date = None
    end_date = None
    if not args.start_date and not args.end_date:
        print(f'[ERROR] Start date(-sd) is not given.')
        return start_date, end_date
    start_date_p = args.start_date
    if args.end_date:
        end_date_p = args.end_date
    else:
        end_date_p = start_date_p
    start_date = get_date_from_param(start_date_p)
    end_date = get_date_from_param(end_date_p)
    if start_date is None:
        print(
            f'[ERROR] Start date {start_date_p} is not in the correct format. It should be YYYY-mm-dd or integer (relative days')
        return None, None
    if end_date is None:
        print(
            f'[ERROR] End date {end_date_p} is not in the correct format. It should be YYYY-mm-dd or integer (relative days')
        return None, None
    if start_date > end_date:
        print(f'[ERROR] End date should be greater or equal than start date')
        return None, None
    return start_date, end_date


def get_date_from_param(dateparam):
    datefin = None
    try:
        ndays = int(dateparam)
        datefin = dt.now().replace(hour=12, minute=0, second=0, microsecond=0) + timedelta(days=ndays)
    except:
        try:
            datefin = dt.strptime(dateparam, '%Y-%m-%d')
        except:
            pass
    return datefin


def main_deprecated():
    print('[INFO]STARTED REFORMAT')
    pinfo = ProductInfo()
    do_multiple_datasets = False

    if args.region:
        region = args.region
        if region == 'BS':
            region = 'BLK'

    if args.mode and args.region and args.level and args.dataset_type and args.sensor:
        pinfo.set_dataset_info_fromparam(args.mode, region, args.level, args.dataset_type, args.sensor)
    elif args.name_product and args.name_dataset:
        pinfo.set_dataset_info(args.name_product, args.name_dataset)
    elif args.name_product and not args.name_dataset:
        pinfo.set_product_info(args.name_product)
        do_multiple_datasets = True

    if args.start_date and args.end_date and not do_multiple_datasets:
        start_date = dt.strptime(args.start_date, '%Y-%m-%d')
        end_date = dt.strptime(args.end_date, '%Y-%m-%d')
        if pinfo.dinfo['frequency'] == 'd':
            if not args.size_opt:
                make_reformat_daily_dataset(pinfo, start_date, end_date, args.verbose)
            if args.size_file:
                file_size = args.size_file
                opt = None
                if args.size_opt:
                    opt = args.size_opt
                if args.verbose:
                    print(f'[INFO] Checking size...')
                df = pinfo.check_size_file_orig(start_date, end_date, opt, args.verbose)
                df.to_csv(file_size, sep=';')
                if not args.size_opt:
                    if args.verbose:
                        print(f'[INFO] Deleting...')
                    pinfo.delete_list_file_path_orig(start_date, end_date, args.verbose)

        if pinfo.dinfo['frequency'] == 'm':
            if not args.size_opt:
                make_reformat_monthly_dataset(pinfo, start_date, end_date)
            if args.size_file:
                file_size = args.size_file
                opt = None
                if args.size_opt:
                    opt = args.size_opt
                if args.verbose:
                    print(f'[INFO] Checking size...')
                if opt is not None:
                    df = pinfo.check_size_file_orig_monthly(start_date, end_date, opt, args.verbose)
                # else: NO IMPPLMENTED
                #     df = pinfo.check_size_file_orig(start_date,end_date,opt,args.verbose)
                df.to_csv(file_size, sep=';')
                if not args.size_opt:
                    if args.verbose:
                        print(f'[INFO] Deleting...')
                    pinfo.delete_list_file_path_orig(start_date, end_date, args.verbose)

    if args.start_date and args.end_date and do_multiple_datasets:

        start_date = dt.strptime(args.start_date, '%Y-%m-%d')
        end_date = dt.strptime(args.end_date, '%Y-%m-%d')

        for dname in pinfo.pinfo:
            pinfo_here = ProductInfo()
            pinfo_here.set_dataset_info(args.name_product, dname)
            make = True
            if args.frequency_product and args.frequency_product != pinfo_here.dinfo['frequency']:
                make = False
            if pinfo_here.dinfo['frequency'] == 'd' and make:
                make_reformat_daily_dataset(pinfo_here, start_date, end_date, args.verbose)

            if pinfo_here.dinfo['frequency'] == 'm' and make:
                make_reformat_monthly_dataset(pinfo_here, start_date, end_date, args.verbose)


def make_reformat_daily_dataset(pinfo, start_date, end_date, verbose):
    date_work = start_date
    while date_work <= end_date:
        if verbose:
            print('----------------------------------------------------')
            print(f'[INFO] Reformating file for date: {date_work}')
        cmd = pinfo.get_reformat_cmd(date_work)
        if cmd is None:
            date_work = date_work + timedelta(hours=24)
            continue
        if verbose:
            print(f'[INFO] CMD: {cmd}')

        prog = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        out, err = prog.communicate()
        if out:
            outstr = out.decode(("utf-8"))
            ierror = outstr.find('ERROR')
            if ierror >= 0:
                print(f'[CMD ERROR] {outstr[ierror:]}')
        if err:
            print(f'[CMD ERROR]{err}')

        preformat = pinfo.check_path_reformat()
        if preformat is not None:
            file_orig = pinfo.get_file_path_orig(None, date_work)
            file_dest = pinfo.get_file_path_orig_reformat_name(date_work)
            if os.path.exists(file_orig) and file_dest is not None:
                if verbose:
                    print(f'[INFO] Moving reformated file {file_orig} to path reformat {file_dest}')
                shutil.copy2(file_orig, file_dest)
                os.remove(file_orig)

        date_work = date_work + timedelta(hours=24)


def make_reformat_monthly_dataset(pinfo, start_date, end_date, verbose):
    year_ini = start_date.year
    year_fin = end_date.year
    for year in range(year_ini, year_fin + 1):
        mini = 1
        mfin = 12
        if year == start_date.year:
            mini = start_date.month
        if year == end_date.year:
            mfin = end_date.month
        for month in range(mini, mfin + 1):
            date_here = dt(year, month, 15)
            if verbose:
                print('----------------------------------------------------')
                print(f'[INFO] Reformating file for year: {year} month: {month}')
            cmd = pinfo.get_reformat_cmd(date_here)
            if verbose:
                print(f'[INFO] CMD: {cmd}')
            prog = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE)
            out, err = prog.communicate()
            if out:
                outstr = out.decode(("utf-8"))
                ierror = outstr.find('ERROR')
                if ierror >= 0:
                    print(f'[CMD ERROR] {outstr[ierror:]}')
            if err:
                print(f'[CMD ERROR]{err}')


def make_reformat_clima_dataset(pinfo, start_date, end_date, verbose):
    date_work = start_date
    while date_work <= end_date:
        if verbose:
            print('----------------------------------------------------')
            print(f'[INFO] Reformatting climatology file for date: {date_work}')

        input_file_nor = pinfo.get_file_path_orig_climatology(None, date_work, False, True)
        if input_file_nor is None:
            print(f'[ERROR] File {input_file_nor} does not exist. Skipping date...')
            date_work = date_work + timedelta(hours=24)
            continue
        file_out = pinfo.get_file_path_orig_climatology(None, date_work, True, False)
        if pinfo.get_dtype() == 'plankton':
            create_reformatted_climatology(pinfo,input_file_nor, file_out, date_work,'CHL')

        date_work = date_work + timedelta(hours=24)


def create_reformatted_climatology(pinfo,input_path, file_out, date_here,variable_base):
    from netCDF4 import Dataset
    if args.verbose:
        print(f'[INFO] Started output climatology file: {file_out}')

    if variable_base == 'CHL':
        variable_base_long = 'Chlorophyll-a Concentration'
        variable_base_standard = 'mass_concentration_of_chlorophyll_a_in_sea_water'
        units = 'mg/m3'
        minvalue = 0.0
        maxvalue = 1000.0
    try:
        dataset = Dataset(input_path)
    except:
        print(f'[ERROR] {input_path} does not exist')
        return False
    variables_in = ['NOFOBS', 'WEIGHTED_MEAN', 'WEIGHTED_STD', 'MEDIANA']
    if pinfo.get_region().upper()=='ARC':
        variables_in = ['NOFOBS','W_AVG','W_STD','MEDIAN']
    
    for var in variables_in:
        if var not in dataset.variables:
            print(f'[ERROR] Variable: {var} is not available in input file: {input_path}')
            return False
    variables_out = ['count', 'weighted_mean', 'weighted_std', 'median']
    desc_out = ['Number of observations', 'Weighted mean values', 'Weighted Standard Deviation Values', 'Median values']

    isarctic = pinfo.get_region() == 'ARC'

    ##grid variables
    if isarctic:
        file_grid = pinfo.get_dinfo_param('grid_file')
        if file_grid is None:
            print(f'[ERROR] File grid parameterer is not available in the JSON dataset in PRODUCT_INFO')
            return False
        if not os.path.exists(file_grid):
            print(f'[ERROR] File grid {file_grid} does not exist')
            return False
        try:
            dgrid = Dataset(file_grid)
            lat_array = np.array(dgrid.variables['lat'])
            lon_array = np.array(dgrid.variables['lon'])
            y_array = np.array(dgrid.variables['y'])
            x_array = np.array(dgrid.variables['x'])
            ny = len(y_array)
            nx = len(x_array)
            stereographic = dgrid.variables['stereographic']
            atts = stereographic.ncattrs()
            atts_values = {}
            for at in atts:
                atts_values[at] = stereographic.getncattr(at)

            dgrid.close()
        except:
            print(f'[ERROR] Error reading file grid: {file_grid}')
            return False

    else: ##MED, BLK
        lat_array = np.array(dataset.variables['LAT'])
        min_lat = np.min(lat_array)
        max_lat = np.max(lat_array)
        lon_array = np.array(dataset.variables['LON'])
        min_lon = np.min(lon_array)
        max_lon = np.max(lon_array)
        limits = [min_lat, max_lat, min_lon, max_lon]
        nlat = len(lat_array)
        nlon = len(lon_array)

    try:
        dout = Dataset(file_out, 'w', format='NETCDF4')
    except PermissionError:
        if args.verbose:
            print(f'[ERROR] Permission error')
        return False



    if isarctic:
        dout = create_clima_dimensions_arc(dout,ny,nx)
        dout = create_grid_variables(dout,lat_array,lon_array,y_array,x_array,atts_values)
    else:
        dout = create_clima_dimensions(dout, nlat, nlon)
        dout = create_lat_lon_variables(dout, lat_array, lon_array, limits)
    dout = create_time_variable(dout,date_here)
    for ivar  in range(len(variables_in)):
        var_in = variables_in[ivar]
        var_out = f'{variable_base}_{variables_out[ivar]}'
        if args.verbose:
            print(f'[INFO] Variable: {var_in} -> {var_out}')
        sname = f'{variable_base_standard}'
        lname = f'Climatology - {desc_out[ivar]} of {variable_base_long}'
        array = np.array(dataset.variables[var_in])


        if var_in=='NOFOBS':
            sname = f'{variable_base_standard} number_of_observations'
            dout = create_variable(dout, var_out, 'i4', sname, lname, 0, 32767, None, array,isarctic)
        else:
            dout = create_variable(dout,var_out,'f4',sname,lname,minvalue,maxvalue,units,array,isarctic)
    global_atts = pinfo.get_global_atts()
    date_now = dt.utcnow()

    if 'creation_date' in global_atts.keys():
        global_atts['creation_date'] = date_now.strftime('%a %b %d %Y')
    if 'creation_time' in global_atts.keys():
        global_atts['creation_time'] = date_now.strftime('%H:%M:%S UTC')
    if 'site_name' in global_atts.keys():
        global_atts['site_name'] = pinfo.get_region().upper()
    if 'title' in global_atts.keys():
        global_atts['title'] = pinfo.dataset_name
    if 'cmems_product_id' in global_atts.keys():
        global_atts['cmems_product_id'] = pinfo.product_name
    if 'westernmost_longitude' in global_atts.keys():
        if isarctic:
            global_atts['westernmost_longitude'] = '-180.0'
        else:
            global_atts['westernmost_longitude'] = f'{min_lon:.1f}'
    if 'easternmost_longitude' in global_atts.keys():
        if isarctic:
            global_atts['easternmost_longitude'] = '180.0'
        else:
            global_atts['easternmost_longitude'] = f'{max_lon:.1f}'
    if 'southernmost_latitude' in global_atts.keys():
        if isarctic:
            global_atts['southernmost_latitude'] =  '65.0'
        else:
            global_atts['southernmost_latitude'] = f'{min_lat:.1f}'
    if 'northernmost_latitude' in global_atts.keys():
        if isarctic:
            global_atts['northernmost_latitude'] = '90.0'
        else:
            global_atts['northernmost_latitude'] = f'{max_lat:.1f}'

    for at in global_atts:
        dout.setncattr(at,global_atts[at])

    dataset.close()
    dout.close()

def create_variable(dout,name,dtype,sname,lname,minvalue,maxvalue,units,array,isarctic):
    if isarctic:
        variable = dout.createVariable(name, dtype, ('time', 'y', 'x'), fill_value=-999, zlib=True, shuffle=True,
                                       complevel=6)
        variable.grid_mapping = 'stereographic'
        variable.comment = 'OC-CC v6 - Gaussian Processor Regressor (GPR) Algorithm'
    else:
        variable = dout.createVariable(name, dtype, ('time', 'lat', 'lon'), fill_value=-999, zlib=True,shuffle=True, complevel=6)
    variable.standard_name = sname
    variable.long_name = lname
    variable.valid_min = minvalue
    variable.valid_max = maxvalue
    variable.missing_value = -999.0
    variable.type = 'surface'
    if units is not None:
        variable.units = units
    if isarctic and not name.endswith('_count'):
        array[array!=-999.0] = np.power(10,array[array!=-999.0])
    variable[:] = array[:]
    return dout

def create_clima_dimensions(dout, nlat, nlon):
    dout.createDimension('lat', nlat)  # ny
    dout.createDimension('lon', nlon)  # nx
    dout.createDimension('time', 1)
    return dout
def create_clima_dimensions_arc(dout, ny, nx):
    dout.createDimension('y', ny)  # ny
    dout.createDimension('x', nx)  # nx
    dout.createDimension('time', 1)
    return dout

def create_lat_lon_variables(dout, lat_array, lon_array, limits):
    # latitude
    satellite_latitude = dout.createVariable('lat', 'f4', ('lat',), zlib=True, shuffle=True, complevel=6)
    satellite_latitude.units = "degrees_north"
    satellite_latitude.standard_name = "latitude"
    satellite_latitude.long_name = "latitude"
    satellite_latitude.valid_min = limits[0]
    satellite_latitude.valid_max = limits[1]
    satellite_latitude[:] = lat_array[:]

    # longitude
    satellite_longitude = dout.createVariable('lon', 'f4', ('lon',), zlib=True, shuffle=True, complevel=6)
    satellite_longitude.units = "degrees_east"
    satellite_longitude.standard_name = "longitude"
    satellite_longitude.long_name = "longitude"
    satellite_longitude.valid_min = limits[2]
    satellite_longitude.valid_max = limits[3]
    satellite_longitude[:] = lon_array[:]

    return dout


def create_grid_variables(dout, lat_array, lon_array, y_array,x_array,stereographicAtts):
    # latitude
    satellite_latitude = dout.createVariable('lat', 'f4', ('y','x'), zlib=True, shuffle=True, complevel=6)
    satellite_latitude.units = "degrees_north"
    satellite_latitude.standard_name = "latitude"
    satellite_latitude.long_name = "latitude"
    satellite_latitude.comment = "Spherical latidude from 65 to 90 degrees north"
    satellite_latitude[:] = lat_array[:]

    # longitude
    satellite_longitude = dout.createVariable('lon', 'f4', ('y','x'), zlib=True, shuffle=True, complevel=6)
    satellite_longitude.units = "degrees_east"
    satellite_longitude.standard_name = "longitude"
    satellite_longitude.long_name = "longitude"
    satellite_longitude.comment = "Spherical longitude from -180 to 180 degrees east"
    satellite_longitude[:] = lon_array[:]

    #y
    yvar = dout.createVariable('y','f4',('y',),zlib=True, shuffle=True, complevel=6)
    yvar.units = "metres"
    yvar.standard_name = "projection_y_coordinate"
    yvar.axis = "Y"
    yvar[:] = y_array[:]

    #x
    xvar = dout.createVariable('x', 'f4', ('x',), zlib=True, shuffle=True, complevel=6)
    xvar.units = "metres"
    xvar.standard_name = "projection_x_coordinate"
    xvar.axis = "X"
    xvar[:] = x_array[:]

    #stererographic
    stereographic = dout.createVariable('stereographic', 'i4')
    for at in stereographicAtts:
        stereographic.setncattr(at,stereographicAtts[at])

    return dout

def create_time_variable(dout,date_here):
    # time
    time = dout.createVariable('time', 'i8', ('time',), zlib=True, shuffle=True, complevel=6)
    time.long_name = 'reference time'
    time.standard_name = 'time'
    time.axis = 'T'
    time.calendar = 'Gregorian'
    time.units = 'seconds since 1981-01-01 00:00:00'
    nseconds = (date_here-dt(1981,1,1,0,0,0)).total_seconds()
    time[0] = nseconds

    return dout


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
