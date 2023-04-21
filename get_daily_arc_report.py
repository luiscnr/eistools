import argparse
from datetime import datetime as dt
from datetime import timedelta

import numpy as np

from product_info import ProductInfo
import os
import warnings

warnings.filterwarnings("ignore")
parser = argparse.ArgumentParser(description='Daily reports')
parser.add_argument("-m", "--mode", help="Mode.", type=str, required=True, choices=['NRT', 'DT'])
parser.add_argument("-sd", "--date", help="Date.")
args = parser.parse_args()


def main():
    if args.date:
        date = get_date_from_param(args.date)
    else:
        date = dt.now().replace(hour=12, minute=0, second=0, microsecond=0)

    if date is None:
        print(
            f'[ERROR] Date {args.date} is not in the correct format. It should be YYYY-mm-dd or integer (relative days')
        return
    name_products, name_datasets, dates_processing = get_list_products_datasets(args.mode, date)

    sep = ['----------------------------------------------------------------------------------------------------------']

    # DOWNLOAD
    date_processing = dates_processing[0]
    lines = []
    status_downloaded, lines_download, downloaded_files = get_lines_download(args.mode, date_processing)
    lines = [*lines, *lines_download, *sep]

    # RESAMPLING
    status_resampling, lines_resampling = get_lines_resampling(args.mode, date_processing, downloaded_files)
    lines = [*lines, *lines_resampling, *sep]

    # INTEGRATION
    status_integration, lines_integration = get_lines_integration(args.mode, date_processing)
    lines = [*lines, *lines_integration, *sep]

    for line in lines:
        print(line)


def get_list_products_datasets(mode, date):
    pinfo = ProductInfo()
    name_products = []
    name_datasets = []
    dates = []
    regions = ['ARC']
    levels = ['L3']
    sensors = ['olci']
    if mode == 'NRT':
        date_nrt = date - timedelta(days=1)
    if mode == 'DT':
        date_dt = date - timedelta(days=8)

    # DAILY PRODUCTS
    for region in regions:
        for level in levels:
            for sensor in sensors:
                name_p, name_d = pinfo.get_list_datasets_params('NRT', region, level, None, sensor, 'd')
                if mode == 'NRT':
                    dates_d = [date_nrt] * len(name_p)
                if mode == 'DT':
                    dates_d = [date_dt] * len(name_p)
                name_products = [*name_products, *name_p]
                name_datasets = [*name_datasets, *name_d]
                dates = [*dates, *dates_d]

    return name_products, name_datasets, dates


def get_lines_download(mode, date):
    lines = ['DOWNLOAD']
    downloadedFiles = []
    dir_base = '/store/COP2-OC-TAC/arc/sources'
    str_date = date.strftime('%Y%m%d')
    dir_date = os.path.join(dir_base, str_date)
    if os.path.exists(dir_date):
        lines.append(f'[INFO] Source path:{dir_date}')
    else:
        lines.append(f'[ERROR] Source path: {dir_date} does not exist')
        lines.append(f'[STATUS] FAIL')
        return 0, lines, downloadedFiles
    flist = os.path.join(dir_date, 'eum_filelist.txt')
    if os.path.exists(dir_date):
        lines.append(f'[INFO] File list:{flist}')
    else:
        # if file doesn't exist, some problem has occurred
        lines.append(f'[ERROR] File list was not created. ')
        lines.append(f'[STATUS] FAIL')
        return 0, lines, downloadedFiles
    timeliness = '_NR_'
    if mode == 'DT':
        timeliness = '_NT_'
    wce = f'{str_date}'
    nexpected = 0
    ndownloaded = 0
    missingFiles = []

    f1 = open(flist)
    for line in f1:
        name = line.strip()
        if name.find(wce) > 0 and name.find(timeliness) > 0 and name.find('OL_2_WFR') > 0:
            nexpected = nexpected + 1
            fdownloaded = os.path.join(dir_date, f'{name}.zip')
            if os.path.exists(fdownloaded):
                downloadedFiles.append(name)
                ndownloaded = ndownloaded + 1
            else:
                missingFiles.append(name)
    f1.close()

    if nexpected == 0 and ndownloaded == 0:
        ##situation when granules were not found
        lines.append(f'[WARNING] No granules found in the Arctic area')
        lines.append(f'[WARNING] Granules downloaded: 0')
        lines.append(f'[STATUS] WARNING')
        return 2, lines, downloadedFiles

    lines.append(f'[INFO] #Granules found in the Arctic area: {nexpected}')
    lines.append(f'[INFO] #Granules downloaded: {ndownloaded}')
    if ndownloaded == nexpected:
        lines.append('[STATUS] OK')
        return 1, lines, downloadedFiles
    elif ndownloaded == 0:
        lines.append('[ERROR] 0 granules were downloaded')
        lines.append('[STATUS] FAIL')
        return 0, lines, downloadedFiles
    elif ndownloaded < nexpected:
        lines.append('[WARNING] Some expected granules are missing')
        lines.append('[STATUS] WARNING')
        return -1, lines, downloadedFiles


def get_lines_resampling(mode, date, downloadedFiles):
    lines = ['RESAMPLING']
    dir_base = '/store/COP2-OC-TAC/arc/resampled'
    str_year = date.strftime('%Y')
    str_month = date.strftime('%m')
    str_day = date.strftime('%d')
    dir_date = os.path.join(dir_base, str_year, str_month, str_day)
    if os.path.exists(dir_date):
        lines.append(f'[INFO] Resampling path:{dir_date}')
    else:
        lines.append(f'[ERROR] Resampling path: {dir_date} does not exist')
        lines.append(f'[STATUS] FAIL')
        return 0, lines
    nfiles = len(downloadedFiles)
    if nfiles == 0:
        lines.append(f'[WARNING] No granules available for resampling')
        lines.append(f'[STATUS] WARNING')
        return 2, lines

    nresampled = 0
    for name in downloadedFiles:
        name_resampled = f'{name.strip()[:-5]}_resampled.nc'
        file_resampled = os.path.join(dir_date, name_resampled)
        if os.path.exists(file_resampled):
            nresampled = nresampled + 1
    lines.append(f'[INFO] #Granules available in the Arctic area: {nfiles}')
    lines.append(f'[INFO] #Granules resampled (with >10000 valid pixels): {nresampled}')
    if nresampled > 0:
        lines.append('[STATUS] OK')
        return 1, lines
    elif nresampled == 0:
        lines.append('[ERROR] No granules were resampled')
        lines.append('[STATUS] FAIL')
        return 0, lines


def get_lines_integration(mode, date):
    lines = ['RESAMPLING']
    dir_base = '/store/COP2-OC-TAC/arc/integrated'
    str_year = date.strftime('%Y')
    str_day = date.strftime('%j')
    str_date = f'{str_year}{str_day}'
    dir_date = os.path.join(dir_base, str_year, str_day)
    if os.path.exists(dir_date):
        lines.append(f'[INFO] Integration path:{dir_date}')
    else:
        lines.append(f'[ERROR] Integration path: {dir_date} does not exist')
        lines.append(f'[STATUS] FAIL')
        return 0, lines

    freflectance = os.path.join(dir_date, f'O{str_date}_rrs-arc-fr.nc')
    status_reflectance = 1
    if os.path.exists(freflectance):
        lines.append(f'[INFO] Reflectance file: {freflectance}')
        bands = ['RRS400', 'RRS412_5', 'RRS442_5', 'RRS490', 'RRS510', 'RRS560', 'RRS620', 'RRS665', 'RRS673_75',
                 'RRS681_25', 'RRS708_75']
        isvalid, lines_file = get_check_netcdf_file(freflectance, 'RRS442_5', bands)
        lines = [*lines, *lines_file]
        if isvalid:
            lines.append(f'[STATUS] OK')
        else:
            status_reflectance = 0
            lines.append(f'[STATUS] FAIL')
    else:
        lines.append(f'[INFO] Reflectance file: {freflectance} does not exist')
        lines.append(f'[STATUS] FAIL')
        status_reflectance = 0

    fkd = os.path.join(dir_date, f'O{str_date}_transp-arc-fr.nc')
    status_transp = 1
    if os.path.exists(fkd):
        lines.append(f'[INFO] Transparence file: {fkd}')
        lines.append(f'[STATUS] OK')
    else:
        lines.append(f'[INFO] Transparence file: {fkd} does not exist')
        lines.append(f'[STATUS] FAIL')
        status_transp = 0

    status = 1
    if status_reflectance == 0 or status_transp == 0:
        status = 0
    return status, lines


def get_check_netcdf_file(file_nc, band_valid, bands):
    from netCDF4 import Dataset
    lines = []
    try:
        dataset = Dataset(file_nc)
    except:
        lines.append(f'[ERROR] File {file_nc} is not a valid NetCDF4 file')
        return False, lines

    if band_valid not in dataset.variables:
        lines.append(f'[ERROR] Band: {band_valid} is not available in file {file_nc}')
        dataset.close()
        return False, lines

    var_valid = dataset.variables[band_valid]
    nvalid, avg_v, min_v, max_v = compute_statistics(var_valid)
    if nvalid == -1:
        dataset.close()
        lines.append(f'[ERROR] Band: {band_valid} in file {file_nc} is not valid')
        return False, lines
    else:
        lines.append(f'[INFO] Number of valid pixels: {nvalid}')

    if nvalid == 0:
        dataset.close()
        return True, lines

    for band in bands:
        if band not in dataset.variables:
            lines.append(f'[ERROR] Band: {band_valid} is not available in file {file_nc}')
            dataset.close()
            return False, lines
        if band == band_valid:
            nvalh = nvalid
            avgh = avg_v
            minh = min_v
            maxh = max_v
        else:
            variable = dataset.variables[band]
            nvalh, avgh, minh, maxh = compute_statistics(variable)

        if nvalh > 0:
            lineband = f'[INFO]->{band}: Avg: {avgh} Min: {minh} Max: {maxh}'
            lines.append(lineband)
        elif nvalh == 0:
            lineband = f'[INFO]->{band}: No valid pixels'
            lines.append(lineband)
        elif nvalh == -1:
            lines.append(f'[ERROR] Band: {band} in file {file_nc} is not valid')
            dataset.close()
            return False, lines

    dataset.close()

    return True, lines


def compute_statistics(variable):
    #print(variable)
    width = variable.shape[1]
    height = variable.shape[2]
    ystep = 1000
    xstep = 1000
    import numpy.ma as ma
    min_values = []
    max_values = []
    avg_values = []
    nvalid_all = 0
    for y in range(0, height, ystep):
        for x in range(0, width, xstep):
            try:
                limits = get_limits(y, x, ystep, xstep, height, width)
                #print(limits)
                array_lim = ma.array(variable[0, limits[0]:limits[1], limits[2]:limits[3]])
                #print(array_lim.shape)
                nvalid = ma.count(array_lim)
                #print('AQUI',nvalid)
                nvalid_all = nvalid_all + nvalid
                if nvalid > 0:
                    min_values.append(ma.min(array_lim))
                    max_values.append(ma.max(array_lim))
                    avg_values.append(ma.mean(array_lim))
            except:
                return -1, -1, -1, -1
    if nvalid_all == 0:
        return nvalid_all, -1, -1, -1
    else:
        avgv = ma.mean(ma.array([avg_values]))
        minv = ma.min(ma.array([min_values]))
        maxv = ma.max(ma.array([max_values]))
        return nvalid_all, avgv, minv, maxv


def get_limits(y, x, ystep, xstep, ny, nx):
    yini = y
    xini = x
    yfin = y + ystep
    xfin = x + xstep
    if yfin > ny:
        yfin = ny
    if xfin > nx:
        xfin = nx

    limits = [yini, yfin, xini, xfin]
    return limits


def get_lines_dataset(name_product, name_dataset, date):
    lines = []
    datestr = date.strftime('%Y-%m-%d')
    pinfo = ProductInfo()
    pinfo.set_dataset_info(name_product, name_dataset)

    lines.append('====================================================================================================')
    lines.append(f'PRODUCT: {name_product}')
    lines.append(f'DATASET: {name_dataset}')
    lines.append(f'DATE: {datestr}')
    lines.append(f'REGION: {pinfo.get_region()}')
    lines.append(f'SENSOR: {pinfo.get_sensor()}')
    lines.append(f'DATASET TYPE: {pinfo.get_dtype()}')
    lines.append(f'LEVEL: {pinfo.get_level()}')
    lines.append(f'FREQUENCY: {pinfo.get_frequency()}')


def get_list_products_datasets(mode, date):
    pinfo = ProductInfo()
    name_products = []
    name_datasets = []
    dates = []
    regions = ['ARC']
    levels = ['L3']
    sensors = ['olci']
    if mode == 'NRT':
        date_nrt = date - timedelta(days=1)
    if mode == 'DT':
        date_dt = date - timedelta(days=8)

    # DAILY PRODUCTS
    for region in regions:
        for level in levels:
            for sensor in sensors:
                name_p, name_d = pinfo.get_list_datasets_params('NRT', region, level, None, sensor, 'd')
                if mode == 'NRT':
                    dates_d = [date_nrt] * len(name_p)
                if mode == 'DT':
                    dates_d = [date_dt] * len(name_p)
                name_products = [*name_products, *name_p]
                name_datasets = [*name_datasets, *name_d]
                dates = [*dates, *dates_d]

    return name_products, name_datasets, dates


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


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
