import argparse
import shutil
from datetime import datetime as dt
from datetime import timedelta
from product_info import ProductInfo
import calendar
import pandas as pd
import os
from configparser import RawConfigParser
from ftplib import FTP
import json
import reformattoCMEMS_202207 as reformat
import uploadtoDBS_202207 as upload

parser = argparse.ArgumentParser(description='Check upload')
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
parser.add_argument("-m", "--mode", help="Mode.", type=str, required=True, choices=['NRT', 'DT', 'MY'])
parser.add_argument("-r", "--region", help="Region.", type=str, choices=['BAL', 'MED', 'BLK'])
parser.add_argument("-l", "--level", help="Level.", type=str, choices=['l3', 'l4'])
parser.add_argument("-d", "--dataset_type", help="Dataset.", type=str,
                    choices=['reflectance', 'plankton', 'optics', 'transp'])
parser.add_argument("-s", "--sensor", help="Sensor.", type=str,
                    choices=['multi', 'olci', 'gapfree_multi', 'multi_climatology'])
parser.add_argument("-sd", "--start_date", help="Start date (yyyy-mm-dd)")
parser.add_argument("-ed", "--end_date", help="Start date (yyyy-mm-dd)")
parser.add_argument("-pname", "--name_product", help="Product name")
parser.add_argument("-pfreq", "--frequency_product",
                    help="Select datasets of selected product (-pname) with this frequency", choices=['d', 'm', 'c'])
parser.add_argument("-dname", "--name_dataset", help="Product name")

args = parser.parse_args()


def do_check():
    print('check')
    ftpc = FTPCheck('MY')
    rpathbase = '/Core/OCEANCOLOUR_BAL_BGC_L3_MY_009_133/cmems_obs-oc_bal_bgc-transp_my_l3-multi-1km_P1D'
    lines = []
    for y in range(1997, 2023, 1):
        for m in range(1, 13, 1):
            print(y, m)
            datehere = dt(y, m, 15)
            yearstr = datehere.strftime('%Y')
            monthstr = datehere.strftime(('%m'))
            path = f'{rpathbase}/{yearstr}/{monthstr}'
            ftpc.go_subdir(rpathbase)
            sizemonth = 0
            for d in range(1, 32, 1):
                try:
                    datehere = dt(y, m, d)
                    dateherestr = datehere.strftime('%Y%m%d')
                    fname = f'{path}/{dateherestr}_cmems_obs-oc_bal_bgc-transp_my_l3-multi-1km_P1D.nc'
                    size = ftpc.get_file_size(fname)
                    if size >= 0:
                        sizemonth = sizemonth + size
                except:
                    pass
            if sizemonth > 0:
                line = f'{y};{m};{sizemonth}'
                lines.append(line)

    file_out = '/mnt/c/DATA_LUIS/OCTAC_WORK/EisMarch2023/transp_multi_133.csv'
    with open(file_out, 'w') as f:
        for line in lines:
            f.write(line)
            f.write('\n')

    return True


def do_check2():
    print('check')
    ftpc = FTPCheck('MY')
    dataset = 'cmems_obs-oc_bal_bgc-plankton_my_l4-multi-1km_P1M'
    rpathbase = f'/Core/OCEANCOLOUR_BAL_BGC_L4_MY_009_134/{dataset}'
    lines = []
    for y in range(2016, 2022, 1):
        datehere = dt(y, 1, 15)
        yearstr = datehere.strftime('%Y')
        path = f'{rpathbase}/{yearstr}'
        ftpc.go_subdir(path)
        for m in range(1, 13, 1):
            last_day = calendar.monthrange(y, m)[1]
            print(y, m, last_day)
            dateini = dt(y, m, 1)
            datefin = dt(y, m, last_day)
            dateinistr = dateini.strftime('%Y%m%d')
            datefinstr = datefin.strftime('%Y%m%d')
            fname = f'{path}/{dateinistr}-{datefinstr}_{dataset}.nc'
            print(fname)
            sizemonth = ftpc.get_file_size(fname)
            print(sizemonth)
            if sizemonth > 0:
                line = f'{y};{m};{sizemonth}'
                lines.append(line)

    file_out = '/mnt/c/DATA_LUIS/OCTAC_WORK/EisMarch2023/planckton_multi_134.csv'
    with open(file_out, 'w') as f:
        for line in lines:
            f.write(line)
            f.write('\n')

    return True


def do_check3():
    print('STARTED CHECK 3 MODE')
    dir_dest = '/dst04-data1/OC/OLCI/daily_3.01/'
    dir_orig = '/store/COP2-OC-TAC/BAL_Evolutions/BAL_REPROC'
    start_date = dt(2016, 4, 1)
    end_date = dt(2022, 11, 22)
    date_here = start_date
    while date_here <= end_date:
        yearstr = date_here.strftime('%Y')
        jjjstr = date_here.strftime('%j')
        datestr = date_here.strftime(('%Y%j'))
        dir_dest_day = os.path.join(dir_dest, yearstr, jjjstr)
        # chl
        namechl = f'O{datestr}-chl-bal-fr.nc'
        file_orig = os.path.join(dir_orig, yearstr, jjjstr, namechl)
        file_dest = os.path.join(dir_dest_day, namechl)
        if os.path.exists(file_orig) and os.path.exists(dir_dest_day) and not os.path.exists(file_dest):
            print(f'Date: {date_here} Copying: {file_orig} to {dir_dest_day}')
            shutil.copy(file_orig, file_dest)

        # kd490
        namekd = f'O{datestr}-kd490-bal-fr.nc'
        file_orig = os.path.join(dir_orig, yearstr, jjjstr, namekd)
        file_dest = os.path.join(dir_dest_day, namekd)
        if os.path.exists(file_orig) and os.path.exists(dir_dest_day) and not os.path.exists(file_dest):
            print(f'Date: {date_here} Copying: {file_orig} to {dir_dest_day}')
            shutil.copy(file_orig, file_dest)

        date_here = date_here + timedelta(hours=24)

    return True


def do_check4():
    print('STARTED CHECK 4 MODE')
    start_date = dt(2022, 1, 1)
    end_date = dt(2022, 11, 22)
    date_here = start_date
    dir_dest = '/dst04-data1/OC/OLCI/dailybal_onns'
    dir_orig = '/dst04-data1/OC/OLCI/daily_3.01'
    lines = []
    while date_here <= end_date:
        yearstr = date_here.strftime('%Y')
        jjjstr = date_here.strftime('%j')
        dir_dest_jday = os.path.join(dir_dest, yearstr, jjjstr)
        line = f'mkdir {dir_dest_jday}'
        lines.append(line)
        dir_orig_jday = os.path.join(dir_orig, yearstr, jjjstr)
        line = f'cp -r {dir_orig_jday}/*bal* {dir_dest_jday}'
        lines.append(line)
        line = f'cp -r {dir_orig_jday}/*BAL* {dir_dest_jday}'
        lines.append(line)
        line = f'rm -f {dir_orig_jday}/*bal*'
        lines.append(line)
        line = f'rm -f {dir_orig_jday}/*BAL*'
        lines.append(line)

        date_here = date_here + timedelta(hours=24)

    fout = '/mnt/c/DATA_LUIS/OCTAC_WORK/EiSNovember2022/UPLOAD/movebalonns.txt'
    with open(fout, 'w') as f:
        for line in lines:
            f.write(line)
            f.write('\n')

    return True


def do_check5():
    print('check5')
    ftpc = FTPCheck('MY')
    rpathbase = '/Core/OCEANCOLOUR_BAL_BGC_L3_MY_009_133/cmems_obs-oc_bal_bgc-plankton_my_l3-olci-300m_P1D'
    # limit a-b 15/05/2018
    start_date = dt(2021, 1, 1)
    end_date = dt(2022, 9, 30)
    datehere = start_date
    lines = []
    yearprev = -1
    monthprev = -1
    while datehere <= end_date:
        if datehere.year != yearprev or datehere.month != monthprev:
            print(datehere.year, datehere.month)
            yearprev = datehere.year
            monthprev = datehere.month
            yearstr = datehere.strftime('%Y')
            monthstr = datehere.strftime(('%m'))
            path = f'{rpathbase}/{yearstr}/{monthstr}'
            ftpc.go_subdir(path)
        dateherestr = datehere.strftime('%Y%m%d')

        file = f'{dateherestr}_cmems_obs-oc_bal_bgc-plankton_my_l3-olci-300m_P1D.nc'
        existe = ftpc.check_file(file)
        if not existe:
            lines.append(dateherestr)
        datehere = datehere + timedelta(days=1)

    fout = '/mnt/c/DATA_LUIS/OCTAC_WORK/POLYMER_PROCESSING/NOAVAILABLE/dates2021-2022.csv'
    with open(fout, 'w') as f:
        for line in lines:
            f.write(line)
            f.write('\n')

    return True


def do_check6():
    print('check6')
    # fdates = '/mnt/c/DATA_LUIS/OCTAC_WORK/POLYMER_PROCESSING/NOAVAILABLE/dates2016.csv'
    fdates = '/store/COP2-OC-TAC/BAL_Evolutions/NotAv/dates2021-2022.csv'
    fout = '/store/COP2-OC-TAC/BAL_Evolutions/NotAv/check2021-2022.csv'
    f1 = open(fdates, 'r')
    linesout = []
    for line in f1:
        dateherestr = line.strip()
        print(dateherestr)
        datehere = dt.strptime(dateherestr, '%Y%m%d')
        yearstr = datehere.strftime('%Y')
        jjjstr = datehere.strftime('%j')
        # check polymer
        dirpolymer = f'/store/COP2-OC-TAC/BAL_Evolutions/POLYMER/{yearstr}/{jjjstr}'
        flist = os.listdir(dirpolymer)
        npolymer = len(flist)
        # check water
        dirwater = f'/store/COP2-OC-TAC/BAL_Evolutions/POLYMER_WATER/{yearstr}/{jjjstr}'
        flist = os.listdir(dirwater)
        nwater = len(flist)
        # check reproc
        dirreproc = f'/store/COP2-OC-TAC/BAL_Evolutions/BAL_REPROC/{yearstr}/{jjjstr}'
        prenamea = f'Oa{yearstr}{jjjstr}'
        prenamea_split = f'Oa{yearstr}{jjjstr}-'
        namea = f'Oa{yearstr}{jjjstr}--bal-fr.nc'
        prenameb = f'Ob{yearstr}{jjjstr}'
        prenameb_split = f'Ob{yearstr}{jjjstr}-'
        nameb = f'Ob{yearstr}{jjjstr}--bal-fr.nc'
        prenamemerge = f'O{yearstr}{jjjstr}'
        nresamplea = 0
        nresampleb = 0
        msa = 0
        msb = 0
        nsplita = 0
        nsplitb = 0
        nmerge = 0
        for name in os.listdir(dirreproc):
            if name == namea:
                msa = 1
            elif name == nameb:
                msb = 1
            elif name.startswith(prenamea):
                if name.startswith(prenamea_split):
                    nsplita = nsplita + 1
                else:
                    nresamplea = nresamplea + 1
            elif name.startswith(prenameb):
                if name.startswith(prenameb_split):
                    nsplitb = nsplitb + 1
                else:
                    nresampleb = nresampleb + 1
            elif name.strip(prenamemerge):
                nmerge = nmerge + 1
        lineout = f'{dateherestr};{npolymer};{nwater};{nresamplea};{nresampleb};{msa};{msb};{nsplita};{nsplitb};{nmerge}'
        linesout.append(lineout)

    f1.close()

    print('Saving...')
    with open(fout, 'w') as f:
        line = 'date;npolymer;nwater;nresamplea;nresampleb;msa;msb;nsplita;nsplitb;nmerge'
        f.write(line)
        f.write('\n')
        for line in linesout:
            f.write(line)
            f.write('\n')

    print('DONE')

    return True


def do_check7():
    print('docheck7 prepare sh.txt to correct bal missings')
    # finput = '/mnt/c/DATA_LUIS/OCTAC_WORK/POLYMER_PROCESSING/NOAVAILABLE/check_all.csv'
    # fout = '/mnt/c/DATA_LUIS/OCTAC_WORK/POLYMER_PROCESSING/NOAVAILABLE/correct_all.sh.txt'

    finput = '/store/COP2-OC-TAC/BAL_Evolutions/NotAv/check_all.csv'
    # fout = '/store/COP2-OC-TAC/BAL_Evolutions/NotAv/correct_all_polymer.sh.txt'
    fout = '/store/COP2-OC-TAC/BAL_Evolutions/NotAv/correct_all_upload.sh.txt'

    # linesoutput = ['source /home/gosuser/load_miniconda3.source', 'conda activate OC_202209',
    #                'cd /home/gosuser/Processing/OC_PROC_EIS202211/s3olciProcessing/aceasy', '']

    linesoutput = ['']

    f1 = open(finput, 'r')
    for line in f1:

        vals = line.strip().split(';')
        if vals[0] == 'date':
            continue
        datehere = dt.strptime(vals[0], '%Y%m%d')
        yearstr = datehere.strftime('%Y')
        jjjstr = datehere.strftime('%j')
        dateherestr = datehere.strftime('%Y-%m-%d')
        npolymer = int(vals[1])
        nwater = int(vals[2])
        nsplita = int(vals[7])
        nsplitb = int(vals[8])
        if npolymer == nwater and npolymer > 0:
            file_end = f'/store/COP2-OC-TAC/BAL_Evolutions/BAL_REPROC/{yearstr}/{jjjstr}/CMEMS2_O{yearstr}{jjjstr}-rrs-bal-fr.nc'
            if os.path.exists(file_end):
                lr, lo, lp, lt = get_lines_upload(dateherestr)
                linesoutput.append(lr)
                linesoutput.append(lo)
                linesoutput.append(lp)
                linesoutput.append(lt)
            continue
            linebase = f'python /home/gosuser/Processing/OC_PROC_EIS202211/s3olciProcessing/aceasy/main.py -ac BALALL -c /home/gosuser/Processing/OC_PROC_EIS202211/s3olciProcessing/CONFIG -i /store/COP2-OC-TAC/BAL_Evolutions/POLYMER_WATER -o /store/COP2-OC-TAC/BAL_Evolutions/BAL_REPROC  -sd {dateherestr} -ed {dateherestr} -v'
            if nsplita == 28 and nsplitb == 0:
                lineout = linebase.replace('CONFIG', 'aceasy_config_merge.ini')
                linesoutput.append(lineout)
                lineout = linebase.replace('CONFIG', 'aceasy_config_reformat.ini')
                linesoutput.append(lineout)
            elif nsplita == 0 and nsplitb == 28:
                lineout = linebase.replace('CONFIG', 'aceasy_config_merge.ini')
                linesoutput.append(lineout)
                lineout = linebase.replace('CONFIG', 'aceasy_config_reformat.ini')
                linesoutput.append(lineout)
            else:
                lineout = f'rm /store/COP2-OC-TAC/BAL_Evolutions/BAL_REPROC/{yearstr}/{jjjstr}/*'
                linesoutput.append(lineout)
                lineout = linebase.replace('CONFIG', 'aceasy_config.ini')
                linesoutput.append(lineout)
                lineout = linebase.replace('CONFIG', 'aceasy_config_ms.ini')
                linesoutput.append(lineout)
                lineout = linebase.replace('CONFIG', 'aceasy_config_merge.ini')
                linesoutput.append(lineout)
                lineout = linebase.replace('CONFIG', 'aceasy_config_reformat.ini')
                linesoutput.append(lineout)
        if npolymer == 0 and nwater == 0:
            continue
            dir_trim = f'/store/COP2-OC-TAC/BAL_Evolutions/POLYMER_TRIM/{yearstr}/{jjjstr}'
            dowithtrim = False
            if os.path.isdir(dir_trim) and os.path.exists(dir_trim):
                files = os.listdir(dir_trim)
                if len(files) >= 0:
                    dowithtrim = True

            if dowithtrim:
                lineout = f'python /home/Luis.Gonzalezvilas/aceasy/main.py -ac POLYMER -c /home/Luis.Gonzalezvilas/aceasy/aceasy_config_vm.ini -i /store/COP2-OC-TAC/BAL_Evolutions/POLYMER_TRIM -o /store/COP2-OC-TAC/BAL_Evolutions/POLYMER -tp /home/Luis.Gonzalezvilas/TEMPDATA/unzip_folder -sd {dateherestr} -ed {dateherestr} -v'
                linesoutput.append(lineout)
                lineout = f'python /home/Luis.Gonzalezvilas/aceasy/main.py -ac BALMLP -c /home/Luis.Gonzalezvilas/aceasy/aceasy_config_vm.ini -i /store/COP2-OC-TAC/BAL_Evolutions/POLYMER -o /store/COP2-OC-TAC/BAL_Evolutions/POLYMER_WATER -sd {dateherestr} -ed {dateherestr} -v'
                linesoutput.append(lineout)

        if nwater < npolymer:
            print('CASO ESPECIAL->', datehere)
    f1.close()

    print('Saving...')
    with open(fout, 'w') as f:
        for line in linesoutput:
            f.write(line)
            f.write('\n')

    print('DONE')

    return True


def main():
    print('[INFO] STARTED REFORMAT AND UPLOAD')

    if do_check7():
        return

    ##DATASETS SELECTION
    pinfo = ProductInfo()
    name_products = []
    name_datasets = []
    n_datasets = 0

    if args.region:
        region = args.region
        if region == 'BS':
            region = 'BLK'

    if args.mode and args.region and args.level:
        # pinfo.set_dataset_info_fromparam(args.mode, args.region, args.level, args.dataset_type, args.sensor)
        dataset_type = None
        sensor = None
        mode_search = args.mode
        frequency = None
        if args.mode == 'DT':
            mode_search = 'NRT'
        if args.dataset_type:
            dataset_type = args.dataset_type
        if args.sensor:
            sensor = args.sensor
        if args.frequency_product:
            frequency = args.frequency_product
        name_products, name_datasets = pinfo.get_list_datasets_params(mode_search, region, args.level, dataset_type,
                                                                      sensor, frequency)
        n_datasets = len(name_products)

    elif args.name_product and args.name_dataset:
        name_products.append(args.name_product)
        name_datasets.append(args.name_dataset)
        n_datasets = 1
        # pinfo.set_dataset_info(args.name_product, args.name_dataset)
    elif args.name_product and not args.name_dataset:
        name_products, name_datasets = pinfo.get_list_datasets(args.name_product)
        n_datasets = len(name_products)
        # pinfo.set_product_info(args.name_product)
        do_multiple_datasets = True

    if n_datasets == 0:
        print(f'[ERROR] No datasets selected')
        return

    if args.verbose:
        print(f'[INFO] Number of selected datasets: {n_datasets}')
        for idataset in range(n_datasets):
            print(f'[INFO]  {name_products[idataset]}/{name_datasets[idataset]}')

    ##DATES SELECTION
    if not args.start_date and not args.end_date:
        print(f'[ERROR] Start date(-sd) is not given.')
        return
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
        return
    if end_date is None:
        print(
            f'[ERROR] End date {end_date_p} is not in the correct format. It should be YYYY-mm-dd or integer (relative days')
        return
    if start_date > end_date:
        print(f'[ERROR] End date should be greater or equal than start date')
        return
    if args.verbose:
        print(f'[INFO] Start date: {start_date} End date: {end_date}')

    if args.check_param:
        return

    # if args.start_date and args.end_date and not do_multiple_datasets:
    for idataset in range(n_datasets):
        # start_date = dt.strptime(args.start_date, '%Y-%m-%d')
        # end_date = dt.strptime(args.end_date, '%Y-%m-%d')
        pinfo.set_dataset_info(name_products[idataset], name_datasets[idataset])
        if args.verbose:
            print(f'[INFO] Working with dataset: {name_products[idataset]}/{name_datasets[idataset]}')
        if pinfo.dinfo['frequency'] == 'd':
            check_daily(pinfo, start_date, end_date, args.verbose)


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


def get_lines_upload(dateherestr):
    lr = f'/usr/local/anaconda/anaconda3/bin/python /home/gosuser/Processing/OC_PROC_EIS202207_NRTNASA_EDS/uploaddu/eistools/reformat_uploadtoDBS_202207.py -m MY -pname OCEANCOLOUR_BAL_BGC_L3_MY_009_133  -dname  cmems_obs-oc_bal_bgc-reflectance_my_l3-olci-300m_P1D -sd {dateherestr} -ed {dateherestr} -v -noreformat'
    lo = f'/usr/local/anaconda/anaconda3/bin/python /home/gosuser/Processing/OC_PROC_EIS202207_NRTNASA_EDS/uploaddu/eistools/reformat_uploadtoDBS_202207.py -m MY -pname OCEANCOLOUR_BAL_BGC_L3_MY_009_133  -dname  cmems_obs-oc_bal_bgc-optics_my_l3-olci-300m_P1D -sd {dateherestr} -ed {dateherestr} -v -noreformat'
    lp = f'/usr/local/anaconda/anaconda3/bin/python /home/gosuser/Processing/OC_PROC_EIS202207_NRTNASA_EDS/uploaddu/eistools/reformat_uploadtoDBS_202207.py -m MY -pname OCEANCOLOUR_BAL_BGC_L3_MY_009_133  -dname  cmems_obs-oc_bal_bgc-plankton_my_l3-olci-300m_P1D -sd {dateherestr} -ed {dateherestr} -v -noreformat'
    lt = f'/usr/local/anaconda/anaconda3/bin/python /home/gosuser/Processing/OC_PROC_EIS202207_NRTNASA_EDS/uploaddu/eistools/reformat_uploadtoDBS_202207.py -m MY -pname OCEANCOLOUR_BAL_BGC_L3_MY_009_133  -dname  cmems_obs-oc_bal_bgc-transp_my_l3-olci-300m_P1D -sd {dateherestr} -ed {dateherestr} -v -noreformat'
    return lr, lo, lp, lt


# IMPORTANT: PINFO MUST CONTAIN PRODUCT NAME AND DATASET NAME
def check_dailyfile_du(mode, pinfo, date, verbose):
    if verbose:
        print('[INFO] Checking  in the DU: started...')
    ftpcheck = FTPCheck(mode)
    y = date.year
    m = date.month
    rpath = ftpcheck.go_month_subdir(pinfo, y, m)
    if rpath is None:
        if verbose:
            print(f'[ERROR] Month subdir for year {y} and month {m} was not found in FTP DU for {pinfo.dataset_name}')
        return None, None, False
    if verbose:
        print(f'[INFO] Remote path: {rpath}')
    remote_name = pinfo.get_remote_file_name(date)
    if mode == 'DT':
        remote_name = remote_name.replace('nrt', 'dt')
    if mode == 'MYINT':
        remote_name = remote_name.replace('my', 'myint')
    if verbose:
        print(f'[INFO] Remote file name: {remote_name}')

    check = ftpcheck.check_file(remote_name)
    if verbose:
        print(f'[INFO] Status: {check}')

    return rpath, remote_name, check


def check_daily(pinfo, start_date, end_date, verbose):
    if verbose:
        print('[INFO] Starting checking...')

    fsummary, finvalid = pinfo.get_dataset_summary()

    values = {}
    if os.path.exists(fsummary):
        fp = open(fsummary)
        values = json.load(fp)
        fp.close()

    listnan = []
    sdate_nan = None
    edate_nan = None
    if os.path.exists(finvalid):
        fp = open(finvalid, 'r')
        for line in fp:
            fname = line.strip().split('/')[-1]
            date = dt.strptime(fname[0:8], '%Y%m%d')
            if sdate_nan is None and edate_nan is None:
                sdate_nan = date
                edate_nan = date
            else:
                if date < sdate_nan:
                    sdate_nan = date
                if date > edate_nan:
                    edate_nan = date
        fp.close()

    for y in range(start_date.year, end_date.year + 1):
        for m in range(start_date.month, end_date.month + 1):
            if verbose:
                print(f'[INFO] Checking... Year: {y} Month: {m}')
            day_ini = 1
            day_fin = calendar.monthrange(y, m)[1]
            if m == start_date.month:
                day_ini = start_date.day
            if m == end_date.month:
                day_fin = end_date.day
            if verbose:
                print(f'[INFO] Deleting potential original files...')
            dateini_here = dt(y, m, day_ini)
            datefin_here = dt(y, m, day_fin)
            pinfo.MODE = 'NONE'
            pinfo.delete_list_file_path_orig(dateini_here, datefin_here, False)
            ftpcheck = FTPCheck(args.mode)
            rpath = ftpcheck.go_month_subdir(pinfo, y, m)
            key = f'{y}{m}'
            month_complete = False
            if key in values.keys():
                ndays = (values[key]['EndDay'] - values[key]['StartDay']) + 1
                if ndays == values[key]['NFiles']:
                    if values[key]['EndDay'] == calendar.monthrange(y, m)[1]:
                        month_complete = True
                    else:
                        day_ini = values[key]['EndDay'] + 1
                        values[key]['EndDay'] = day_fin
                else:
                    day_ini = 1
                    if m == start_date.month:
                        day_ini = start_date.day
                    values[key]['StartDay'] = day_ini
                    values[key]['EndDay'] = day_fin
                    values[key]['NMissing'] = 0
                    values[key]['NFiles'] = 0
                    values[key]['TotalSize'] = 0
                    values[key]['AvgSize'] = 0
            else:
                values[key] = {
                    'Year': y,
                    'Month': m,
                    'StartDay': day_ini,
                    'EndDay': day_fin,
                    'NMissing': 0,
                    'NFiles': 0,
                    'TotalSize': 0,
                    'AvgSize': 0,
                }
            if month_complete:
                continue
            for d in range(day_ini, day_fin + 1):
                remote_name = pinfo.get_remote_file_name(dt(y, m, d))
                size = ftpcheck.get_file_size(remote_name)
                if size == -1:
                    addnan = True
                    if sdate_nan is not None and edate_nan is not None:
                        if sdate_nan <= dt(y, m, d) <= edate_nan:
                            addnan = False
                    if addnan:
                        remote_path = os.path.join(rpath, remote_name)
                        listnan.append(remote_path)
                    values[key]['NMissing'] = values[key]['NMissing'] + 1
                else:
                    values[key]['NFiles'] = values[key]['NFiles'] + 1
                    values[key]['TotalSize'] = values[key]['TotalSize'] + size
            if values[key]['NFiles'] > 0:
                values[key]['AvgSize'] = values[key]['TotalSize'] / values[key]['NFiles']

    with open(fsummary, 'w') as fp:
        json.dump(values, fp)
    with open(finvalid, 'a') as fp:
        for line in listnan:
            fp.write(line)
            fp.write('\n')

    print(f'[INFO] Completed')


class FTPCheck():

    def __init__(self, mode):
        sdir = os.path.abspath(os.path.dirname(__file__))
        # path2script = "/".join(sdir.split("/")[0:-1])
        path2script = os.path.dirname(sdir)
        credentials = RawConfigParser()
        credentials.read(os.path.join(path2script, 'credentials.ini'))
        if mode == 'MY' or mode == 'MYINT':
            du_server = "my-dev.cmems-du.eu"
        elif mode == 'NRT' or mode == 'DT':
            du_server = "nrt-dev.cmems-du.eu"
        du_uname = credentials.get('normal', 'uname')
        du_passwd = credentials.get('normal', 'passwd')
        self.ftpdu = FTP(du_server, du_uname, du_passwd)

    def go_subdir(self, rpath):
        # print('Changing directory to: ', rpath)
        try:
            self.ftpdu.cwd(rpath)
            return rpath
        except:
            return None

    def go_month_subdir(self, pinfo, year, month):
        dateref = dt(year, month, 15)
        rpath = os.path.join('/Core', pinfo.product_name, pinfo.dataset_name, dateref.strftime('%Y'),
                             dateref.strftime('%m'))

        rpath = self.go_subdir(rpath)

        return rpath

    def check_file(self, fname):
        if fname in self.ftpdu.nlst():
            return True
        else:
            return False

    def get_file_size(self, fname):
        try:
            t = self.ftpdu.size(fname)
            tkb = t / 1024
            tmb = tkb / 1024
            tgb = tmb / 1024
        except:
            tgb = -1
        return tgb


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
