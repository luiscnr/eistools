import argparse
from datetime import datetime as dt
from datetime import timedelta
import check_202207 as checkftp

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
    date_processing = dates_processing[0]
    lines_ini = get_lines_ini(date, dates_processing[0])

    sep = ['----------------------------------------------------------------------------------------------------------']

    # DOWNLOAD
    lines = []
    status_downloaded, lines_download, downloaded_files = get_lines_download(args.mode, date_processing)
    lines = [*lines, *lines_download, *sep]

    # RESAMPLING
    status_resampling, lines_resampling = get_lines_resampling(date_processing, downloaded_files)
    lines = [*lines, *lines_resampling, *sep]

    # INTEGRATION
    status_integration, lines_integration = get_lines_integration(date_processing)
    lines = [*lines, *lines_integration, *sep]

    # PROCESSING
    status_processing, lines_processing = get_lines_processing(date_processing)
    lines = [*lines, *lines_processing, *sep]

    # UPLOAD
    status_upload, lines_upload = get_lines_upload(name_products, name_datasets, dates_processing)
    lines = [*lines, *lines_upload, *sep]

    # GLOBAL STATUS
    if status_integration == 0 or status_processing == 0 or status_resampling == 0 or status_downloaded == 0 or status_upload == 0:
        global_status = 'ERROR'
    elif status_integration == 1 and status_processing == 1 and status_resampling == 1 and status_downloaded == 1 and status_upload == 1:
        global_status = 'OK'
    else:
        global_status = 'WARNING'
    line_status = [f'GLOBAL STATUS: {global_status}']
    lines = [*lines_ini, *line_status, *sep, *lines]

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


def get_lines_ini(report_date, processing_date):
    report_date_str = report_date.strftime('%Y-%m-%d')
    processing_date_str = processing_date.strftime('%Y-%m-%d')
    lines = ['ARCTIC DAILY TECHNICAL REPORT']
    lines.append(f'MODE: {args.mode}')
    lines.append(f'PROCESSING DATE: {processing_date_str}')
    lines.append(f'CMD REPORT:sh /store/COP2-OC-TAC/arc/operational_code/lancia_check_date.sh {args.mode} {report_date_str}')
    lines.append(' ')

    return lines


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
    if os.path.exists(flist):
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

    nmissing = len(missingFiles)
    corruptedFiles = []
    if nmissing>0:
        fcorrupted = os.path.join(dir_date,'CorruptedFiles.txt')
        if os.path.exists(fcorrupted):
            f2 = open(fcorrupted,'r')
            for line in f2:
                name = line.strip()
                if name.find(wce) > 0 and name.find(timeliness) > 0 and name.find('OL_2_WFR') > 0:
                    corruptedFiles.append(name)
            f2.close()
    ncorrupted = len(corruptedFiles)
    n_nodownloaded = nmissing-ncorrupted


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
        lines.append(f'[WARNING] {nmissing} expected granules are missing:')
        lines.append(f'[WARNING] -> No downloaded: {n_nodownloaded}')
        lines.append(f'[WARNING] -> Corrupted: {ncorrupted}')
        lines.append('[STATUS] WARNING')

    if ncorrupted>0:
        lines.append('')
        lines.append('Corrupted granules: ')
        for granule in corruptedFiles:
            lines.append(granule)

        return -1, lines, downloadedFiles


def get_lines_resampling(date, downloadedFiles):
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
        lines.append('[WARNING] No granules were resampled')
        lines.append('[STATUS] WARNING')
        return 2, lines


def get_lines_integration(date):
    lines = ['INTEGRATION']
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
        bands = ['KD490']
        isvalid, lines_file = get_check_netcdf_file(fkd, 'KD490', bands)
        lines = [*lines, *lines_file]
        if isvalid:
            lines.append(f'[STATUS] OK')
        else:
            status_transp = 0
            lines.append(f'[STATUS] FAIL')

    else:
        lines.append(f'[INFO] Transparence file: {fkd} does not exist')
        lines.append(f'[STATUS] FAIL')
        status_transp = 0

    status = 1
    if status_reflectance == 0 or status_transp == 0:
        status = 0
    return status, lines


def get_lines_processing(date):
    lines = ['PROCESSING']
    dir_base = '/store/COP2-OC-TAC/arc/integrated'
    str_year = date.strftime('%Y')
    str_day = date.strftime('%j')
    str_date = f'{str_year}{str_day}'
    dir_date = os.path.join(dir_base, str_year, str_day)
    if os.path.exists(dir_date):
        lines.append(f'[INFO] Processing path:{dir_date}')
    else:
        lines.append(f'[ERROR] Processing path: {dir_date} does not exist')
        lines.append(f'[STATUS] FAIL')
        return 0, lines

    fplankton = os.path.join(dir_date, f'O{str_date}_plankton-arc-fr.nc')
    status = 1
    if os.path.exists(fplankton):
        lines.append(f'[INFO] Plankton file: {fplankton}')
        bands = ['CHL']
        isvalid, lines_file = get_check_netcdf_file(fplankton, 'CHL', bands)
        lines = [*lines, *lines_file]
        if isvalid:
            lines.append(f'[STATUS] OK')
        else:
            status = 0
            lines.append(f'[STATUS] FAIL')
    else:
        lines.append(f'[INFO] Plankton file: {fplankton} does not exist')
        lines.append(f'[STATUS] FAIL')
        status = 0

    return status, lines


def get_lines_upload(products, datasets, dates):
    lines = ['UPLOAD']
    status = 1
    for idx in range(len(products)):
        pinfo = ProductInfo()
        pinfo.set_dataset_info(products[idx], datasets[idx])
        date = dates[idx]
        upload_mode = args.mode
        if args.mode == 'DT':
            pinfomy = pinfo.get_pinfomy_equivalent()
            if not pinfomy is None:
                upload_mode = 'MYINT'
        if args.mode == 'NRT' and date < pinfo.get_last_nrt_date():
            upload_mode = 'DT'
            pinfomy = pinfo.get_pinfomy_equivalent()
            if not pinfomy is None:
                upload_mode = 'MYINT'
        from s3buckect import S3Bucket
        sb = S3Bucket()
        sb.star_client()
        if upload_mode == 'MYINT':
            sb.update_params_from_pinfo(pinfomy)
            bucket_name, key, isuploaded = sb.check_daily_file('MYINT',pinfomy,date,False)
            #rpath, remote_file_name, isuploaded = checkftp.check_dailyfile_du('MYINT', pinfomy, date, False)
        else:
            #sb.update_params_from_pinfo(pinfo)
            bucket_name, key, isuploaded = sb.check_daily_file(upload_mode, pinfo, date, False)
            #rpath, remote_file_name, isuploaded = checkftp.check_dailyfile_du(upload_mode, pinfo, date, False)
        sb.close_client()
        lines.append(f'[INFO]{products[idx]}/{datasets[idx]}')
        lines.append(f'[INFO]  ->Upload mode:  {upload_mode.lower()}')
        lines.append(f'[INFO]  ->Path: {bucket_name}-->{key}')
        if isuploaded:
            lines.append('[INFO]  ->Upload completed')
        else:
            lines.append('[ERROR]  ->Upload failed')
            status = 0
    if status == 1:
        lines.append(f'[STATUS] OK')
    else:
        lines.append(f'[STATUS] FAIL')

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
            if band.startswith('RRS'):
                lineband = f'[INFO]  ->{band}: Avg: {avgh:.4f} Min: {minh:.4f} Max: {maxh:.4f}'
            else:
                lineband = f'[INFO]  ->{band}: Avg: {avgh:.2f} Min: {minh:.2f} Max: {maxh:.2f}'
            lines.append(lineband)
        elif nvalh == 0:
            lineband = f'[INFO]  ->{band}: No valid pixels'
            lines.append(lineband)
        elif nvalh == -1:
            lines.append(f'[ERROR] Band: {band} in file {file_nc} is not valid')
            dataset.close()
            return False, lines

    dataset.close()

    return True, lines


def compute_statistics(variable):
    # print(variable)
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
                array_lim = ma.array(variable[0, limits[0]:limits[1], limits[2]:limits[3]])
                nvalid = ma.count(array_lim)
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
