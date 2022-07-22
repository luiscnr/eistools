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

    # checking
    ncompleted = 0
    nprocessed = 0
    nuploaded = 0
    for idx in range(len(name_products)):
        # print(name_products[idx], name_datasets[idx], dates[idx])
        lines_dataset, aresources, isuploaded = get_lines_dataset(name_products[idx], name_datasets[idx], dates[idx])
        if aresources:
            ncompleted = ncompleted + 1
        isprocessed = True  # no implemented
        if isprocessed:
            nprocessed = nprocessed + 1
        if isuploaded:
            nuploaded = nuploaded + 1

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
    lines.append(f'PROCESSED DATASETS: {nprocessed}/{ndatasets} * NO IMPLEMENTED YET')
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
    path_base = '/home/gosuser/OCTACManager/daily_checking/REPROC_FILES'
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

    lines.append('------------------')
    lines.append('SOURCES')
    sources = pinfo.get_sources()
    # print(pinfo.product_name, pinfo.dataset_name)
    lines_sources, aresources = get_lines_sources(pinfo, sources, date)
    lines = [*lines, *lines_sources]
    if aresources:
        lines.append('Status: OK')
    else:
        lines.append('Status: FAILED')

    upload_mode = args.mode
    if args.mode == 'DT':
        pinfomy = pinfo.get_pinfomy_equivalent()
        if not pinfomy is None:
            upload_mode = 'MYINT'
    if upload_mode == 'MYINT':
        rpath, remote_file_name, isuploaded = checkftp.check_dailyfile_du('MYINT', pinfomy, date, False)
    else:
        rpath, remote_file_name, isuploaded = checkftp.check_dailyfile_du(args.mode, pinfo, date, False)
    lines.append('------------------')
    lines.append('DU Upload')
    lines.append(f'Upload mode:  {upload_mode.lower()}')
    lines.append(f'Remote path: {rpath}')
    lines.append(f'Remote file name: {remote_file_name}')
    if isuploaded:
        lines.append('Status: OK')
    else:
        lines.append('Status: FAILED')

    return lines, aresources, isuploaded


def get_lines_sources(pinfo, sources, date):
    lines = []
    isvalid = False
    if sources is None:
        lines = ['STATUS: No implemented']
        isvalid = True
        return lines, isvalid
    sinfo = SourceInfo('202207')
    slist = sources.split(',')
    nvalid = 0
    for s in slist:
        source = s.strip()
        lines_source, source_valid = sinfo.check_source(source, args.mode, pinfo.get_region(), date)
        if source_valid:
            nvalid = nvalid + 1
        if len(lines) == 0:
            lines = lines_source
        else:
            lines = [*lines, *lines_source]
        lines.append('##############')

    if nvalid == len(slist):
        isvalid = True
    return lines, isvalid


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
