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
        # print(name_products[idx],name_datasets[idx],dates[idx],'-------------------------------------------------------')
        lines_dataset, iscompleted, isprocessed, isuploaded, missing_str = get_lines_dataset(name_products[idx],
                                                                                             name_datasets[idx],
                                                                                             dates[idx])
        if iscompleted:
            ncompleted = ncompleted + 1
            completed_array[idx] = iscompleted
        else:
            missing_array[idx] = missing_str
        if isprocessed:
            nprocessed = nprocessed + 1
            processed_array[idx] = isprocessed
        if isuploaded:
            nuploaded = nuploaded + 1
            uploaded_array[idx] = isuploaded

        lines = [*lines, *lines_dataset]

    save_attach_info_file(lines)
    start_lines = get_start_lines(date, ndatasets, ncompleted, nprocessed, nuploaded)
    lines_mail = start_lines

    if ncompleted == ndatasets and nprocessed == ndatasets and nuploaded == ndatasets:
        reproc_file = get_reproc_filename(date)
        if os.path.exists(reproc_file):
            os.remove(reproc_file)

    # IF EVERYTHING IS OK, SCRIPT FINISHES HERE
    if ncompleted < ndatasets or nprocessed < ndatasets or nuploaded < ndatasets:
        pinfo = ProductInfo()
        # PROBLEMS DETECTED
        lines_mail.append('')
        lines_mail.append('IDENTIFIED PROBLEMS')
        lines_mail.append('===================')
        lines_mail.append('')
        for idx in range(len(name_products)):
            pinfo.set_dataset_info(name_products[idx], name_datasets[idx])
            if not completed_array[idx] or not processed_array[idx] or not uploaded_array[idx]:
                lines_mail.append(f'{name_products[idx]}/{name_datasets[idx]}')
                if not completed_array[idx]:
                    lines_mail.append(f'     MISSING SOURCES ({missing_array[idx]})')
                if not processed_array[idx]:
                    lines_mail.append(f'     INCOMPLETED PROCESSING')
                if not uploaded_array[idx]:
                    lines_mail.append(f'     DATASET WAS NOT UPLOADED')

        # CMD REPROC FILE
        start_reproc_file(date)
        cmdlines = ['ins=$(date +%Y%m%d%H%M%S)']
        name_r = get_reproc_filename(date)[:-3]
        name_r = name_r.split('/')[-1]
        path_o = '/home/gosuser/OCTACManager/daily_checking/REPROC_FILES/LAUNCHED/'
        cmdlines.append(f'cp {get_reproc_filename(date)} {path_o}{name_r}_$ins.sh')
        for idx in range(len(name_products)):
            pinfo.set_dataset_info(name_products[idx], name_datasets[idx])
            if not completed_array[idx]:
                missing_sources_str = missing_array[idx]
                missing_sources = missing_sources_str.split(',')
                sinfo = SourceInfo('202207')
                for source in missing_sources:
                    sinfo.start_source(source.strip())
                    cmd = get_specific_cmd(sinfo.get_cmd(), '202207', dates[idx], pinfo.get_region(), args.mode)
                    cmdlines.append(cmd)
                cmd = get_specific_cmd(pinfo.get_reprocessing_cmd(), '202207', dates[idx], pinfo.get_region(),
                                       args.mode)
                cmdlines.append(cmd)
                cmd = get_upload_cmd(pinfo, dates[idx])
                cmdlines.append(cmd)
            if not processed_array[idx]:
                cmd = get_specific_cmd(pinfo.get_reprocessing_cmd(), '202207', dates[idx], pinfo.get_region(),
                                       args.mode)
                cmdlines.append(cmd)
                cmd = get_upload_cmd(pinfo, dates[idx])
                cmdlines.append(cmd)

            if not uploaded_array[idx]:
                cmd = get_upload_cmd(pinfo, dates[idx])
                cmdlines.append(cmd)

        cmd = get_report_cmd(date)
        cmdlines.append(cmd)

        cmdlines = reorganize_cmd_lines(cmdlines)

        append_lines_to_reproc_file(date, cmdlines)
        lines_mail.append('')
        lines_mail.append('PROPOSED REPROCESSING')
        lines_mail.append('=====================')
        lines_mail.append('')
        lines_mail.append(f'CMD: sh {get_reproc_filename(date)}')
        lines_mail.append('')
        for linecmd in cmdlines:
            lines_mail.append(linecmd)

    # lines_mail = [*start_lines, *cmdlines]
    print_email_lines(lines_mail)


def get_start_lines(date, ndatasets, ncompleted, nprocessed, nuploaded):
    lines = []
    datestr = date.strftime('%Y-%m-%d')
    lines.append(f'DAILY TECHNICAL REPORT')
    lines.append(f'MODE: {args.mode}')
    lines.append(f'DATE: {datestr}')
    cmd = f'CMD REPORT: sh /home/gosuser/OCTACManager/daily_checking/send_report_email_NRT_202207.sh {datestr}'
    lines.append(cmd)
    lines.append(f'TOTAL NUMBER OF DATASETS: {ndatasets}')
    status = 'OK'
    generalstatus = 'OK'
    if ncompleted < ndatasets:
        status = 'FAILED'
        generalstatus = 'FAILED'
    lines.append(f'COMPLETED DATASETS (NON DEGRADED): {ncompleted}/{ndatasets} -> {status}')
    status = 'OK'
    if nprocessed < ndatasets:
        status = 'FAILED'
        generalstatus = 'FAILED'
    lines.append(f'PROCESSED DATASETS: {nprocessed}/{ndatasets} -> {status}')
    status = 'OK'
    if nuploaded < ndatasets:
        status = 'FAILED'
        generalstatus = 'FAILED'
    lines.append(f'UPLOADED DATASETS: {nuploaded}/{ndatasets} -> {status}')
    lines.append('')
    lines.append(F'GENERAL STATUS: {generalstatus}')
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


def append_lines_to_reproc_file(date, lines):
    freproc = get_reproc_filename(date)
    with open(freproc, 'a') as f:
        for line in lines:
            f.write(line)
            f.write('\n')


def get_reproc_filename(date):
    path_base = '/home/gosuser/OCTACManager/daily_checking/REPROC_FILES/PENDING'
    datestr = date.strftime('%Y%m%d')
    freproc = os.path.join(path_base, f'reproc_{args.mode}_{datestr}.sh')
    return freproc


def save_attach_info_file(lines):
    if args.mode == 'NRT':
        finfo = '/home/gosuser/OCTACManager/daily_checking/REPROC_FILES/NRTProduct.info'
    if args.mode == 'DT':
        finfo = '/home/gosuser/OCTACManager/daily_checking/REPROC_FILES/DTProduct.info'
    with open(finfo, 'w') as f:
        for line in lines:
            f.write(line)
            f.write('\n')


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
    if args.mode == 'NRT' and date < pinfo.get_last_nrt_date():
        upload_mode = 'DT'
        pinfomy = pinfo.get_pinfomy_equivalent()
        if not pinfomy is None:
            upload_mode = 'MYINT'

    if upload_mode == 'MYINT':
        rpath, remote_file_name, isuploaded = checkftp.check_dailyfile_du('MYINT', pinfomy, date, False)
    else:
        rpath, remote_file_name, isuploaded = checkftp.check_dailyfile_du(upload_mode, pinfo, date, False)
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
    path_jday, nTot, nAva, missing_files = pinfo.check_processed_files(date)
    if nTot == -1:
        lines = [f'Processed files path: {path_jday}', 'NO IMPLEMENTED']
        isprocessed = True
        return lines, isprocessed
    session_id = pinfo.get_session_id(args.mode, date)
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


def get_specific_cmd(cmd, eis, date, region, mode):
    region = region.upper()
    if region == 'BLK':
        region = 'BS'
    mode = mode.upper()
    cmd = cmd.replace('$EIS$', eis)
    cmd = cmd.replace('$REG$', region)
    cmd = cmd.replace('$MODE$', mode)
    cmd = cmd.replace('$DATE$', date.strftime('%Y-%m-%d'))
    return cmd


def get_upload_cmd(pinfo, date):
    datestr = date.strftime('%Y-%m-%d')
    region = pinfo.get_region()
    level = pinfo.get_level().lower()
    sensor = pinfo.get_sensor().lower()
    dtype = pinfo.get_dtype().lower()
    cmd = f'sh /home/gosuser/Processing/OC_PROC_EIS202207/uploaddu/upload2DBS_202207.sh -m {args.mode} -r {region} -l {level} -s {sensor} -d {dtype} -sd {datestr}'
    return cmd


def get_report_cmd(date):
    datestr = date.strftime('%Y-%m-%d')
    cmd = f'sh /home/gosuser/OCTACManager/daily_checking/send_report_email_{args.mode}_202207.sh {datestr}'
    return cmd


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


def reorganize_cmd_lines(cmd_lines):
    new_cmd_lines = [cmd_lines[0]]
    for idx in range(1, len(cmd_lines), 1):
        lines_prev = cmd_lines[:idx]
        line_now = cmd_lines[idx]
        if not line_now in lines_prev:
            new_cmd_lines.append(line_now)
    return new_cmd_lines


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
