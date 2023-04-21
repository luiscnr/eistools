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


    # DOWNLOAD
    date_processing = dates_processing[0]
    lines = []
    status, lines_download = get_lines_download(args.mode,date_processing)
    lines = [*lines, *lines_download]
    for line in lines:
        print(line)


    #
    # lines_download = get_lines_download(args.mode, date)
    # lines = [*lines, *lines_download]
    # ndatasets = len(name_datasets)
    # completed_array = [False] * ndatasets
    # missing_array = [''] * ndatasets
    # processed_array = [0] * ndatasets
    # uploaded_array = [False] * ndatasets
    #
    # # checking
    # ncompleted = 0
    # nprocessed = 0
    # nuploaded = 0
    # for idx in range(len(name_products)):
    #     lines_dataset, iscompleted, isprocessed, isuploaded, missing_str = get_lines_dataset(name_products[idx],name_datasets[idx],dates[idx])


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
    dir_base = '/store/COP2-OC-TAC/arc/sources'
    str_date = date.strftime('%Y%m%d')
    dir_date = os.path.join(dir_base, str_date)
    if os.path.exists(dir_date):
        lines.append(f'[INFO] Source path:{dir_base}')
    else:
        lines.append(f'[ERROR] Source path: {dir_base} does not exist')
        lines.append(f'[STATUS] FAIL')
        return 0, lines
    flist = os.path.join(dir_date, 'eum_filelist.txt')
    if os.path.exists(dir_date):
        lines.append(f'[INFO] File list:{flist}')
    else:
        #if file doesn't exist, some problem has occurred
        lines.append(f'[ERROR] File list was not created. ')
        lines.append(f'[STATUS] FAIL')
        return 0, lines
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
        # print(name)
        # print(wce)
        # print(timeliness)
        if name.find(wce) > 0 and name.find(timeliness) > 0 and name.find('OL_2_WFR') > 0:
            nexpected = nexpected + 1
            fdownloaded = os.path.join(dir_date, name,'.zip')
            print(fdownloaded)
            if os.path.exists(fdownloaded):
                ndownloaded = ndownloaded + 1
            else:
                missingFiles.append(name)
    f1.close()

    if nexpected == 0 and ndownloaded == 0:
        ##situation when granules were not found
        lines.append(f'[WARNING] No granules found in the Arctic area')
        lines.append(f'[WARNING] Granules downloaded: 0')
        lines.append(f'[STATUS] WARNING')
        return 1, lines

    lines.append(f'[INFO] #Granules found in the Arctic area: {nexpected}')
    lines.append(f'[INFO] #Granules downloaded: {ndownloaded}')
    if ndownloaded == nexpected:
        lines.append('[STATUS] OK')
        return 2, lines
    elif ndownloaded == 0:
        lines.append('[ERROR] 0 granules were downloaded')
        lines.append('[STATUS] FAIL')
        return 0, lines
    elif ndownloaded < nexpected:
        lines.append('[WARNING] Some expected granules are missing')
        lines.append('[STATUS] WARNING')
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
