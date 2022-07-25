import argparse
from datetime import datetime as dt
from datetime import timedelta
from product_info import ProductInfo
import check_202207 as checkftp
from source_info import SourceInfo
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

    name_products, name_datasets, dates = get_list_products_datasets(args.mode, date)
    start_reproc_file(date)
    lines = []
    ndatasets = len(name_datasets)
    completed_array = [False] * ndatasets
    missing_array = [''] * ndatasets
    processed_array = [False] * ndatasets
    uploaded_array = [False] * ndatasets

    # checking
    ncompleted = 0
    nprocessed = 0
    nuploaded = 0
    for idx in range(len(name_products)):
        # print(name_products[idx], name_datasets[idx], dates[idx])
        lines_dataset, iscompleted, isprocessed, isuploaded, missing_str = get_lines_dataset(name_products[idx],name_datasets[idx],dates[idx])
        if iscompleted:
            ncompleted = ncompleted + 1
            completed_array[idx] = iscompleted
            missing_array[idx] = missing_str
        if isprocessed:
            nprocessed = nprocessed + 1
            processed_array[idx] = isprocessed
        if isuploaded:
            nuploaded = nuploaded + 1
            uploaded_array[idx] = isuploaded

        lines = [*lines, *lines_dataset]

    start_lines = get_start_lines(date, ndatasets, ncompleted, nprocessed, nuploaded)
    lines = [*start_lines, *lines]
    print_email_lines(lines)


def get_start_lines(date, ndatasets, ncompleted, nprocessed, nuploaded):
    lines = []
    datestr = date.strftime('%Y-%m-%d')
    lines.append(f'DAILY TECHNICAL REPORT')
    lines.append(f'MODE: {args.mode}')
    lines.append(f'DATE: {datestr}')
    lines.append(f'TOTAL NUMBER OF DATASETS: {ndatasets}')
    status = 'OK'
    if ncompleted < ndatasets:
        status = 'FAILED'
    lines.append(f'COMPLETED DATASETS (NON DEGRADED): {ncompleted}/{ndatasets} -> {status}')
    status = 'OK'
    if nprocessed < ndatasets:
        status = 'FAILED'
    lines.append(f'PROCESSED DATASETS: {nprocessed}/{ndatasets} -> {status}')
    status = 'OK'
    if nuploaded < ndatasets:
        status = 'FAILED'
    lines.append(f'UPLOADED DATASETS: {nuploaded}/{ndatasets} -> {status}')
    lines.append('')
    return lines


def start_reproc_file(date):
    datestr = date.strftime('%Y%m%d')
    freproc = get_reproc_filename(date)
    lines = ['#!/bin/bash', '#bash script for reprocessing', f'#mode: {args.mode}', f'#date: {datestr}']
    with open(freproc, 'w') as f:
        for line in lines:
            f.write(line)
            f.write('\n')


def get_reproc_filename(date):
    path_base = '/home/gosuser/OCTACManager/daily_checking/REPROC_FILES/PENDING'
    datestr = date.strftime('%Y%m%d')
    freproc = os.path.join(path_base, f'reproc_{args.mode}_{datestr}')
    return freproc


def get_lines_dataset(name_product, name_dataset, date):
    lines = []
    datestr = date.strftime('%Y-%m-%d')
    pinfo = ProductInfo()
    pinfo.set_dataset_info(name_product, name_dataset)

    lines.append('*************************************************************************************************')
    lines.append(f'PRODUCT: {name_product}')
    lines.append(f'DATASET: {name_dataset}')
    lines.append(f'DATE: {datestr}')
    lines.append(f'REGION: {pinfo.get_region()}')
    lines.append(f'SENSOR: {pinfo.get_sensor()}')
    lines.append(f'DATASET TYPE: {pinfo.get_dtype()}')
    lines.append(f'LEVEL: {pinfo.get_level()}')
    lines.append(f'FREQUENCY: {pinfo.get_frequency()}')

    # 1: SOURCES CHECK
    lines.append('-------------------------------------------------------------------------------------------')
    lines.append('SOURCES')
    sources = pinfo.get_sources()
    lines_sources, iscompleted, missing_str = get_lines_sources(pinfo, sources, date)
    lines = [*lines, *lines_sources]
    if iscompleted:
        lines.append('Status: OK')
    else:
        lines.append('Status: FAILED')

    # 2: PROCESSING CHECK
    lines.append('-------------------------------------------------------------------------------------------')
    lines.append('PROCESSING')
    lines_processing, isprocessed = get_lines_processing(pinfo, date)
    lines = [*lines, *lines_processing]
    if isprocessed:
        lines.append('Status: OK')
    else:
        lines.append('Status: FAILED')

    # 3: UPLOAD CHECK
    upload_mode = args.mode
    if args.mode == 'DT':
        pinfomy = pinfo.get_pinfomy_equivalent()
        if not pinfomy is None:
            upload_mode = 'MYINT'
    if upload_mode == 'MYINT':
        rpath, remote_file_name, isuploaded = checkftp.check_dailyfile_du('MYINT', pinfomy, date, False)
    else:
        rpath, remote_file_name, isuploaded = checkftp.check_dailyfile_du(args.mode, pinfo, date, False)
    lines.append('-------------------------------------------------------------------------------------------')
    lines.append('DU Upload')
    lines.append(f'Upload mode:  {upload_mode.lower()}')
    lines.append(f'Remote path: {rpath}')
    lines.append(f'Remote file name: {remote_file_name}')
    if isuploaded:
        lines.append('Status: OK')
    else:
        lines.append('Status: FAILED')

    return lines, iscompleted, isprocessed, isuploaded, missing_str


def get_lines_processing(pinfo, date):
    isprocessed = False
    path_jday,nTot,nAva,missing_files = pinfo.check_processed_files(date)
    if nTot==-1:
        lines = [f'Processed files path: {path_jday}','NO IMPLEMENTED']
        isprocessed = True
        return lines, isprocessed
    session_id = pinfo.get_session_id(args.mode,date)
    if session_id is None:
        lines = ['Warning: Session ID was not found']
    else:
        sinfo = SourceInfo('202207')
        sinfo.sessionid = session_id
        lines = [f'Session ID: {session_id}', f'Processing folder: {sinfo.get_processing_folder()}',
                 f'Session folder: {sinfo.get_session_folder()}', f'Log file:  {sinfo.get_log_file()}']

    lines.append(f'Processed files path: {path_jday}')
    lines.append(f'Processed files: {nAva}/{nTot}')
    if nAva == nTot:
        isprocessed = True
    else:
        if not os.path.exists(path_jday):
            lines.append(f'Path: {path_jday} does not exists')
        for name_file in missing_files:
            lines.append(f'File: {name_file} is not available')


    return lines, isprocessed


def get_lines_sources(pinfo, sources, date):
    lines = []
    missing_str = None
    iscompleted = False
    if sources is None:
        lines = ['No implemented']
        iscompleted = True
        return lines, iscompleted, missing_str
    sinfo = SourceInfo('202207')
    slist = sources.split(',')
    ncompleted = 0
    for s in slist:
        source = s.strip()
        lines_source, source_valid = sinfo.check_source(source, args.mode, pinfo.get_region(), date)
        if source_valid:
            ncompleted = ncompleted + 1
        else:
            if missing_str is None:
                missing_str = s.strip()
            else:
                missing_str = f'{missing_str},{s.strip}'
        if len(lines) == 0:
            lines = lines_source
        else:
            lines = [*lines, *lines_source]
        lines.append('##############')

    if ncompleted == len(slist):
        iscompleted = True
    return lines, iscompleted, missing_str


def get_list_products_datasets(mode, date):
    pinfo = ProductInfo()
    name_products = []
    name_datasets = []
    dates = []
    regions = ['BAL', 'MED', 'BLK']
    levels = ['L3', 'L4']
    sensors = ['olci', 'multi', 'gapfree_multi']
    if mode == 'NRT':
        date_nrt = date - timedelta(days=1)
    if mode == 'DT':
        date_dt_multi = date - timedelta(days=20)
        date_dt_interp = date - timedelta(days=24)
        date_dt_olci = date - timedelta(days=8)

    # DAILY PRODUCTS
    for region in regions:
        for level in levels:
            for sensor in sensors:
                name_p, name_d = pinfo.get_list_datasets_params('NRT', region, level, None, sensor, 'd')
                if mode == 'NRT':
                    dates_d = [date_nrt] * len(name_p)
                if mode == 'DT':
                    if sensor == 'multi':
                        dates_d = [date_dt_multi] * len(name_p)
                    elif sensor == 'olci':
                        dates_d = [date_dt_olci] * len(name_p)
                    elif sensor == 'gapfree_multi':
                        dates_d = [date_dt_interp] * len(name_p)
                name_products = [*name_products, *name_p]
                name_datasets = [*name_datasets, *name_d]
                dates = [*dates, *dates_d]

    return name_products, name_datasets, dates


def print_email_lines(lines):
    for line in lines:
        print(line)


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
