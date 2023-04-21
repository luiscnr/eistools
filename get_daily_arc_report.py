import argparse
from datetime import datetime as dt
from datetime import timedelta
from product_info import ProductInfo
import os

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
    status_downloaded, lines_download, downloaded_files = get_lines_download(args.mode,date_processing)
    lines = [*lines, *lines_download,*sep]


    # RESAMPLING
    status_resampling, lines_resampling = get_lines_resampling(args.mode,date_processing,downloaded_files)
    lines = [*lines, *lines_resampling, *sep]

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
        #if file doesn't exist, some problem has occurred
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
    dir_date = os.path.join(dir_base,str_year,str_month,str_day)
    if os.path.exists(dir_date):
        lines.append(f'[INFO] Source path:{dir_date}')
    else:
        lines.append(f'[ERROR] Source path: {dir_date} does not exist')
        lines.append(f'[STATUS] FAIL')
        return 0, lines
    nfiles = len(downloadedFiles)
    if nfiles==0:
        lines.append(f'[WARNING] No granules available for resampling')
        lines.append(f'[STATUS] WARNING')
        return 2, lines

    nresampled = 0
    for name in downloadedFiles:
        name_resampled = f'{name.strip()[:-5]}_resampled.nc'
        file_resampled = os.path.join(dir_date,name_resampled)
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
