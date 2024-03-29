import argparse
import datetime
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
    print('check, computing monthly sizes')
    ftpc = FTPCheck('MY')
    rpathbase = '/Core/OCEANCOLOUR_GLO_BGC_L3_MY_009_107/c3s_obs-oc_glo_bgc-reflectance_my_l3-multi-4km_P1M'
    # rpathbase = '/Core/OCEANCOLOUR_BAL_BGC_L3_MY_009_107/c3s_obs-oc_glo_bgc-reflectance_my_l3-multi-4km_P1M'
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
                    fname = f'{path}/{dateherestr}_c3s_obs-oc_glo_bgc-reflectance_my_l3-multi-4km_P1D.nc'
                    # print(fname)
                    size = ftpc.get_file_size(fname)
                    if size >= 0:
                        sizemonth = sizemonth + size
                except:
                    pass
            if sizemonth > 0:
                line = f'{y};{m};{sizemonth}'
                lines.append(line)

    file_out = '/mnt/c/DATA_LUIS/OCTAC_WORK/EisMarch2023/reflectance_multi_107.csv'
    with open(file_out, 'w') as f:
        for line in lines:
            f.write(line)
            f.write('\n')

    return True


def do_check_sizes_monthly_ftp():
    print('check 2 computing monthly sizes')
    ftpc = FTPCheck('MY')
    datasets = [
        'cmems_obs-oc_blk_bgc-plankton_my_l4-multi-1km_P1M',
        'cmems_obs-oc_blk_bgc-plankton_my_l4-olci-300m_P1M',
        'cmems_obs-oc_med_bgc-plankton_my_l4-multi-1km_P1M',
        'cmems_obs-oc_med_bgc-plankton_my_l4-olci-300m_P1M',
        'cmems_obs-oc_bal_bgc-plankton_my_l4-multi-1km_P1M',
        'cmems_obs-oc_bal_bgc-plankton_my_l4-olci-300m_P1M'
    ]

    #dataset = 'cmems_obs-oc_bal_bgc-plankton_my_l4-multi-1km_P1M'
    #rpathbase = f'/Core/OCEANCOLOUR_BAL_BGC_L4_MY_009_134/{dataset}'
    for dataset in datasets:
        name_prod = 'OCEANCOLOUR_BAL_BGC_L4_MY_009_134'
        if dataset.find('med')>0:
            name_prod = 'OCEANCOLOUR_MED_BGC_L4_MY_009_144'
        if dataset.find('blk')>0:
            name_prod = 'OCEANCOLOUR_BLK_BGC_L4_MY_009_154'
        rpathbase = f'/Core/{name_prod}/{dataset}'
        lines = []
        start_date = dt(1997,9,4)
        end_date = dt(2022,12,31)
        if dataset.find('olci')>0:
            start_date = dt(2016,4,26)
            end_date = dt(2022,12,30)

        lines = []
        for y in range(start_date.year, end_date.year+1, 1):
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
                #print(fname)
                sizemonth = ftpc.get_file_size(fname)
                #print(sizemonth)
                if sizemonth > 0:
                    line = f'{y};{m};{sizemonth}'
                    lines.append(line)

        file_out = os.path.join('/store/COP2-OC-TAC/EiSNovember2023/size_info',f'{dataset}_size.csv')
        with open(file_out, 'w') as f:
            for line in lines:
                f.write(line)
                f.write('\n')

    return True

def do_check_last_date():
    print('Check last date of upload file')
    #input_file = '/mnt/c/DATA_LUIS/OCTAC_WORK/MED_GREY_LIST/cop_grey_list_L3.txt'
    input_file = '/mnt/c/DATA_LUIS/OCTAC_WORK/MED_GREY_LIST/cop_grey_list_interp_L4.txt'
    #input_file = '/mnt/c/DATA_LUIS/OCTAC_WORK/MED_GREY_LIST/cop_grey_list_monthly_L4.txt'
    ftpc = FTPCheck('MY')
    #dataset = 'cmems_obs-oc_med_bgc-transp_my_l3-multi-1km_P1D' #PRODUCT 142
    dataset = 'cmems_obs-oc_med_bgc-plankton_my_l4-gapfree-multi-1km_P1D' #PRODUDCT 144
    #dataset = 'cmems_obs-oc_med_bgc-plankton_my_l4-multi-1km_P1M' #PRODUCT 144

    #rpathbase = f'/Core/OCEANCOLOUR_MED_BGC_L3_MY_009_143/{dataset}'
    rpathbase = f'/Core/OCEANCOLOUR_MED_BGC_L4_MY_009_144/{dataset}'

    is_monthly = False

    f1 = open(input_file)
    for line in f1:
        datestr = line.strip()
        datehere = dt.strptime(datestr,'%Y-%m-%d')
        yearstr = datehere.strftime('%Y')
        monthstr = datehere.strftime('%m')
        if is_monthly:
            path = f'{rpathbase}/{yearstr}'
        else:
            path = f'{rpathbase}/{yearstr}/{monthstr}'
        ftpc.go_subdir(path)

        if is_monthly:
            last_day = calendar.monthrange(datehere.year, datehere.month)[1]
            dateini = dt(datehere.year, datehere.month, 1)
            datefin = dt(datehere.year, datehere.month, last_day)
            dateinistr = dateini.strftime('%Y%m%d')
            datefinstr = datefin.strftime('%Y%m%d')
            fname = f'{path}/{dateinistr}-{datefinstr}_{dataset}.nc'
        else:
            dateherestr = datehere.strftime('%Y%m%d')
            fname = f'{path}/{dateherestr}_{dataset}.nc'
        datehere_modified = ftpc.get_last_modified(fname)
        if datehere_modified is None:
            print(datehere,'->',datehere_modified)
        else:
            if datehere_modified<dt(2023,1,31,0,0,0):
                print(datehere, '->', datehere_modified)

    f1.close()
    print('DONE')
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
    print('check5, checking missing dates in baltic')
    ftpc = FTPCheck('MY')
    rpathbase = '/Core/OCEANCOLOUR_BAL_BGC_L3_MY_009_133/cmems_obs-oc_bal_bgc-plankton_my_l3-olci-300m_P1D'
    # limit a-b 15/05/2018
    start_date = dt(2016, 4, 26)
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

    fout = '/mnt/c/DATA_LUIS/OCTAC_WORK/POLYMER_PROCESSING/NOAVAILABLE/dates2016-2022_2.csv'
    with open(fout, 'w') as f:
        for line in lines:
            f.write(line)
            f.write('\n')

    return True


def do_check6():
    print('check6, preparing BAL correction for single days')
    # fdates = '/mnt/c/DATA_LUIS/OCTAC_WORK/POLYMER_PROCESSING/NOAVAILABLE/dates2016.csv'
    fdates = '/store/COP2-OC-TAC/BAL_Evolutions/NotAv/dates2016-2022_2.csv'
    fout = '/store/COP2-OC-TAC/BAL_Evolutions/NotAv/check2016-2022_2.csv'
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

    finput = '/store/COP2-OC-TAC/BAL_Evolutions/NotAv/check2016-2022_1.csv'
    # fout = '/store/COP2-OC-TAC/BAL_Evolutions/NotAv/correct_all_polymer.sh.txt'
    # fout = '/store/COP2-OC-TAC/BAL_Evolutions/NotAv/correct_all_upload.sh.txt'
    fout = '/store/COP2-OC-TAC/BAL_Evolutions/NotAv/correct_reformat.sh.txt'

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
        nmerge = int(vals[9])
        # dir_reproc = f'/store/COP2-OC-TAC/BAL_Evolutions/BAL_REPROC/{yearstr}/{jjjstr}'
        # fa = os.path.join(dir_reproc, f'CMEMS2_O{yearstr}{jjjstr}-optics-bal-fr.nc')
        # fb = os.path.join(dir_reproc, f'CMEMS2_O{yearstr}{jjjstr}-rrs-bal-fr.nc')
        # fc = os.path.join(dir_reproc, f'CMEMS2_O{yearstr}{jjjstr}-transp-bal-fr.nc')
        # fd = os.path.join(dir_reproc, f'CMEMS2_O{yearstr}{jjjstr}-plankton-bal-fr.nc')
        # if os.path.exists(fa) and os.path.exists(fb) and os.path.exists(fc) and os.path.exists(fd):
        #     lr, lo, lp, lt = get_lines_upload(dateherestr)
        #     linesoutput.append(lr)
        #     linesoutput.append(lo)
        #     linesoutput.append(lp)
        #     linesoutput.append(lt)

        # daterefob = datetime.datetime(2018,5,8)
        # if datehere>daterefob and nsplita==28 and nmerge==28:
        #     line_s3b = f'sh /home/gosuser/Processing/OC_PROC_EIS202211/s3olciProcessing/s3olci_daily_proc_202211.sh {dateherestr} FR NT BAL S3B SRUN --noupload --nopng -v >> /dev/null 2>&1'
        #     linesoutput.append(line_s3b)

        # if nmerge==28:
        #     lr, lo, lp, lt = get_lines_upload(dateherestr)
        #     linesoutput.append(lr)
        #     linesoutput.append(lo)
        #     linesoutput.append(lp)
        #     linesoutput.append(lt)

        linebase = f'python /home/gosuser/Processing/OC_PROC_EIS202211/s3olciProcessing/aceasy/main.py -ac BALALL -c /home/gosuser/Processing/OC_PROC_EIS202211/s3olciProcessing/CONFIG -i /store/COP2-OC-TAC/BAL_Evolutions/POLYMER_WATER -o /store/COP2-OC-TAC/BAL_Evolutions/BAL_REPROC  -sd {dateherestr} -ed {dateherestr} -v'
        if npolymer == 0:
            # line_trim_delete = f'rm -rf /store/COP2-OC-TAC/BAL_Evolutions/POLYMER_TRIM/{yearstr}/{jjjstr}/*'
            # linesoutput.append(line_trim_delete)
            # line_trim = f'/usr/local/anaconda/anaconda3/bin/python trims3basic.py -s /dst04-data1/OC/OLCI/sources_baseline_2.23 -o /store/COP2-OC-TAC/BAL_Evolutions/POLYMER_TRIM -sd {dateherestr} -ed {dateherestr} -geo BAL -wce EFR -v'
            # linesoutput.append(line_trim)
            # dir_trim = f'/store/COP2-OC-TAC/BAL_Evolutions/POLYMER_TRIM/{yearstr}/{jjjstr}'
            # dopolymer = False
            # if os.path.isdir(dir_trim) and os.path.exists(dir_trim):
            #     files = os.listdir(dir_trim)
            #     if len(files) > 0:
            #         dopolymer = True
            # if dopolymer:
            #     line_polymer = f'python /home/Luis.Gonzalezvilas/aceasy/main.py -ac POLYMER -c /home/Luis.Gonzalezvilas/aceasy/aceasy_config_vm.ini -i /store/COP2-OC-TAC/BAL_Evolutions/POLYMER_TRIM -o /store/COP2-OC-TAC/BAL_Evolutions/POLYMER -tp /home/Luis.Gonzalezvilas/TEMPDATA/unzip_folder -sd {dateherestr} -ed {dateherestr} -v'
            #     linesoutput.append(line_polymer)
            # dir_polymer = f'/store/COP2-OC-TAC/BAL_Evolutions/POLYMER/{yearstr}/{jjjstr}'
            # dowater = False
            # if os.path.isdir(dir_polymer) and os.path.exists(dir_polymer):
            #     files = os.listdir(dir_polymer)
            #     if len(files) > 0:
            #         dowater = True
            # if dowater:
            #     line_water = f'python main.py -ac BALMLP -c /home/Luis.Gonzalezvilas/aceasy/aceasy_config_vm.ini -i /store/COP2-OC-TAC/BAL_Evolutions/POLYMER -o /store/COP2-OC-TAC/BAL_Evolutions/POLYMER_WATER -sd {dateherestr} -ed {dateherestr} -v'
            #     linesoutput.append(line_water)
            dir_water = f'/store/COP2-OC-TAC/BAL_Evolutions/POLYMER_WATER/{yearstr}/{jjjstr}'
            doresto = False
            if os.path.isdir(dir_water) and os.path.exists(dir_water):
                files = os.listdir(dir_water)
                if len(files) > 0:
                    doresto = True
            if doresto:
                # lineout = linebase.replace('CONFIG', 'aceasy_config.ini')
                # linesoutput.append(lineout)
                # lineout = linebase.replace('CONFIG', 'aceasy_config_ms.ini')
                # linesoutput.append(lineout)
                # lineout = linebase.replace('CONFIG', 'aceasy_config_merge.ini')
                # linesoutput.append(lineout)
                lineout = linebase.replace('CONFIG', 'aceasy_config_reformat.ini')
                linesoutput.append(lineout)

        if npolymer > 0:
            continue

        if npolymer == nwater and npolymer > 0:
            continue

            ##upload
            file_end = f'/store/COP2-OC-TAC/BAL_Evolutions/BAL_REPROC/{yearstr}/{jjjstr}/CMEMS2_O{yearstr}{jjjstr}-rrs-bal-fr.nc'
            if os.path.exists(file_end):
                lr, lo, lp, lt = get_lines_upload(dateherestr)
                linesoutput.append(lr)
                linesoutput.append(lo)
                linesoutput.append(lp)
                linesoutput.append(lt)
            # other corrections
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
            dir_trim = f'/store/COP2-OC-TAC/BAL_Evolutions/POLYMER_TRIM/{yearstr}/{jjjstr}'
            dowithtrim = False
            if os.path.isdir(dir_trim) and os.path.exists(dir_trim):
                files = os.listdir(dir_trim)
                if len(files) >= 0:
                    dowithtrim = True

            if dowithtrim:
                # lineout = f'python /home/Luis.Gonzalezvilas/aceasy/main.py -ac POLYMER -c /home/Luis.Gonzalezvilas/aceasy/aceasy_config_vm.ini -i /store/COP2-OC-TAC/BAL_Evolutions/POLYMER_TRIM -o /store/COP2-OC-TAC/BAL_Evolutions/POLYMER -tp /home/Luis.Gonzalezvilas/TEMPDATA/unzip_folder -sd {dateherestr} -ed {dateherestr} -v'
                # linesoutput.append(lineout)
                # lineout = f'python /home/Luis.Gonzalezvilas/aceasy/main.py -ac BALMLP -c /home/Luis.Gonzalezvilas/aceasy/aceasy_config_vm.ini -i /store/COP2-OC-TAC/BAL_Evolutions/POLYMER -o /store/COP2-OC-TAC/BAL_Evolutions/POLYMER_WATER -sd {dateherestr} -ed {dateherestr} -v'
                # linesoutput.append(lineout)
                # check polymer
                dirpolymer = f'/store/COP2-OC-TAC/BAL_Evolutions/POLYMER/{yearstr}/{jjjstr}'
                flist = os.listdir(dirpolymer)
                npolymernew = len(flist)
                # check water
                dirwater = f'/store/COP2-OC-TAC/BAL_Evolutions/POLYMER_WATER/{yearstr}/{jjjstr}'
                flist = os.listdir(dirwater)
                nwaternew = len(flist)
                if 0 < npolymernew == nwaternew:
                    l1, l2, l3, l4, l5 = get_lines_correction(dateherestr, yearstr, jjjstr)
                    linesoutput.append(l1)
                    linesoutput.append(l2)
                    linesoutput.append(l3)
                    linesoutput.append(l4)
                    linesoutput.append(l5)

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


def do_check8():
    print('CHECK 8: Med grey list')
    # input_file = '/mnt/c/DATA_LUIS/OCTAC_WORK/MED_GREY_LIST/cop_grey_list_L3.txt'
    # input_file = '/mnt/c/DATA_LUIS/OCTAC_WORK/MED_GREY_LIST/cop_grey_list_interp_L4.txt'
    input_file = '/mnt/c/DATA_LUIS/OCTAC_WORK/MED_GREY_LIST/cop_grey_list_monthly_L4.txt'

    # output_file = '/mnt/c/DATA_LUIS/OCTAC_WORK/MED_GREY_LIST/upload_grey_list_L3.sh.txt'
    # output_file = '/mnt/c/DATA_LUIS/OCTAC_WORK/MED_GREY_LIST/upload_grey_list_interp_L4.sh.txt'
    output_file = '/mnt/c/DATA_LUIS/OCTAC_WORK/MED_GREY_LIST/upload_grey_list_monthly_L4.sh.txt'

    output_lines = []
    # prename = 'python /home/gosuser/OCTACManager/daily_checking/eistools/reformat_uploadtoDBS_202207.py -m MY -pname OCEANCOLOUR_MED_BGC_L3_MY_009_143 -dname'
    prename = 'python /home/gosuser/OCTACManager/daily_checking/eistools/reformat_uploadtoDBS_202207.py -m MY -pname OCEANCOLOUR_MED_BGC_L4_MY_009_144 -dname'

    # datasets = ['cmems_obs-oc_med_bgc-reflectance_my_l3-multi-1km_P1D','cmems_obs-oc_med_bgc-plankton_my_l3-multi-1km_P1D','cmems_obs-oc_med_bgc-transp_my_l3-multi-1km_P1D','cmems_obs-oc_med_bgc-optics_my_l3-multi-1km_P1D']
    # datasets = ['cmems_obs-oc_med_bgc-plankton_my_l4-gapfree-multi-1km_P1D']
    datasets = ['cmems_obs-oc_med_bgc-plankton_my_l4-multi-1km_P1M']

    f1 = open(input_file)
    for line in f1:
        datestr = line.strip()
        for dataset in datasets:
            output_line = f'{prename} {dataset} -sd {datestr} -ed {datestr} -v'
            output_lines.append(output_line)
        output_lines.append('')
    f1.close()

    f2 = open(output_file, 'w')
    for line in output_lines:
        f2.write(line)
        f2.write('\n')
    f2.close()

    print('DONE')
    return True


def do_check9():
    print('STARTED copy daily files from BAL EVOLUTION to daily')
    output_file = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/copytodaily.sh.txt'
    lines_output = []
    input_dir = '/store/COP2-OC-TAC/BAL_Evolutions/BAL_REPROC'
    output_dir = '/dst04-data1/OC/OLCI/daily_3.01'
    dateref = dt(2021, 1, 1)
    dateend = dt(2022, 11, 30)
    while dateref <= dateend:
        yearstr = dateref.strftime('%Y')
        jjjstr = dateref.strftime('%j')
        datestr = dateref.strftime('%Y%j')
        input_dir_day = os.path.join(input_dir, yearstr, jjjstr)
        output_dir_day = os.path.join(output_dir, yearstr, jjjstr)
        cmd = f'cp {input_dir_day}/O{datestr}*.nc {output_dir_day}'
        lines_output.append(cmd)
        dateref = dateref + timedelta(hours=25)

    f2 = open(output_file, 'w')
    for line in lines_output:
        f2.write(line)
        f2.write('\n')
    f2.close()
    print('DONE')

    return True


def do_check_sizes_daily_ftp():
    ftpc = FTPCheck('MY')

    # datasets = [
    #     'cmems_obs-oc_bal_bgc-optics_my_l3-olci-300m_P1D',
    #     'cmems_obs-oc_bal_bgc-plankton_my_l3-olci-300m_P1D',
    #     'cmems_obs-oc_bal_bgc-reflectance_my_l3-olci-300m_P1D',
    #     'cmems_obs-oc_bal_bgc-transp_my_l3-olci-300m_P1D',
    #     'cmems_obs-oc_bal_bgc-plankton_my_l3-multi-1km_P1D',
    #     'cmems_obs-oc_bal_bgc-reflectance_my_l3-multi-1km_P1D',
    #     'cmems_obs-oc_bal_bgc-transp_my_l3-multi-1km_P1D',
    #     'cmems_obs-oc_med_bgc-optics_my_l3-multi-1km_P1D',
    #     'cmems_obs-oc_med_bgc-plankton_my_l3-olci-300m_P1D',
    #     'cmems_obs-oc_med_bgc-reflectance_my_l3-olci-300m_P1D',
    #     'cmems_obs-oc_med_bgc-transp_my_l3-olci-300m_P1D',
    #     'cmems_obs-oc_med_bgc-plankton_my_l3-multi-1km_P1D',
    #     'cmems_obs-oc_med_bgc-reflectance_my_l3-multi-1km_P1D',
    #     'cmems_obs-oc_med_bgc-transp_my_l3-multi-1km_P1D',
    #     'cmems_obs-oc_blk_bgc-optics_my_l3-multi-1km_P1D',
    #     'cmems_obs-oc_blk_bgc-plankton_my_l3-olci-300m_P1D',
    #     'cmems_obs-oc_blk_bgc-reflectance_my_l3-olci-300m_P1D',
    #     'cmems_obs-oc_blk_bgc-transp_my_l3-olci-300m_P1D',
    #     'cmems_obs-oc_blk_bgc-plankton_my_l3-multi-1km_P1D',
    #     'cmems_obs-oc_blk_bgc-reflectance_my_l3-multi-1km_P1D',
    #     'cmems_obs-oc_blk_bgc-transp_my_l3-multi-1km_P1D'
    # ]
    datasets = [
        'cmems_obs-oc_blk_bgc-plankton_my_l4-gapfree-multi-1km_P1D',
        'cmems_obs-oc_med_bgc-plankton_my_l4-gapfree-multi-1km_P1D'
    ]

    for dataset in datasets:
        name_prod = 'OCEANCOLOUR_BAL_BGC_L3_MY_009_133'
        if dataset.find('med')>0:
            #name_prod = 'OCEANCOLOUR_MED_BGC_L3_MY_009_143'
            name_prod = 'OCEANCOLOUR_MED_BGC_L4_MY_009_144'
        if dataset.find('blk')>0:
            #name_prod = 'OCEANCOLOUR_BLK_BGC_L3_MY_009_153'
            name_prod = 'OCEANCOLOUR_BLK_BGC_L4_MY_009_154'
        rpathbase = f'/Core/{name_prod}/{dataset}'
        lines = []
        start_date = dt(1997,9,4)
        end_date = dt(2022,12,31)
        if dataset.find('olci')>0:
            start_date = dt(2016,4,26)
            end_date = dt(2022,11,30)

        for y in range(start_date.year, end_date.year+1, 1):
            for m in range(1, 13, 1):
                print(f'Dataset: {dataset} {y} {m}')
                datehere = dt(y, m, 15)
                yearstr = datehere.strftime('%Y')
                monthstr = datehere.strftime(('%m'))
                path = f'{rpathbase}/{yearstr}/{monthstr}'
                ftpc.go_subdir(rpathbase)
                sizemonth = 0
                for d in range(1, 32, 1):
                    try:
                        datehere = dt(y, m, d)
                        if datehere<start_date:
                            continue
                        if datehere>end_date:
                            continue

                        dateherestr = datehere.strftime('%Y%m%d')
                        fname = f'{path}/{dateherestr}_{dataset}.nc'
                        size = ftpc.get_file_size(fname)
                        if size >= 0:
                            sizemonth = sizemonth + size
                    except:
                        pass
                if sizemonth > 0:
                    line = f'{y};{m};{sizemonth}'
                    lines.append(line)
        file_out = os.path.join('/store/COP2-OC-TAC/EiSNovember2023/size_info',f'{dataset}_size.csv')

        with open(file_out, 'w') as f:
            for line in lines:
                f.write(line)
                f.write('\n')

    return True



def do_check_sizes():
    folder = '/store/COP2-OC-TAC/arc/multi'
    start_date = dt(2021,10,1)
    end_date = dt(2022,9,30)

    date_work = start_date
    size = 0
    while date_work <= end_date:

        year = date_work.strftime('%Y')
        jjj = date_work.strftime('%j')
        dayfolder = os.path.join(folder,year,jjj)

        fp_size = 0
        fp = os.path.join(dayfolder,f'C{year}{jjj}_chl-arc-4km.nc')
        if os.path.exists(fp):
            fp_stats = os.stat(fp)
            fp_size = fp_stats.st_size / (1024 * 1024 * 1024)

        fr_size = 0
        fr = os.path.join(dayfolder, f'C{year}{jjj}_rrs-arc-4km.nc')
        if os.path.exists(fr):
            fr_stats = os.stat(fr)
            fr_size = fr_stats.st_size / (1024 * 1024 * 1024)

        ft_size = 0
        ft = os.path.join(dayfolder, f'C{year}{jjj}_kd490-arc-4km.nc')
        if os.path.exists(ft):
            ft_stats = os.stat(ft)
            ft_size = ft_stats.st_size / (1024 * 1024 * 1024)


        size = size + fp_size + fr_size + ft_size

        print(date_work,'->',size)
        date_work = date_work + timedelta(hours=24)
    print('FINAL:')
    print(size)

def main():
    print('[INFO] STARTED REFORMAT AND UPLOAD')

    if do_check_sizes_monthly_ftp():
        return
    # if do_check_last_date():
    #     return

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
    # lr = f'/usr/local/anaconda/anaconda3/bin/python /home/gosuser/Processing/OC_PROC_EIS202207_NRTNASA_EDS/uploaddu/eistools/reformat_uploadtoDBS_202207.py -m MY -pname OCEANCOLOUR_BAL_BGC_L3_MY_009_133  -dname  cmems_obs-oc_bal_bgc-reflectance_my_l3-olci-300m_P1D -sd {dateherestr} -ed {dateherestr} -v -noreformat'
    # lo = f'/usr/local/anaconda/anaconda3/bin/python /home/gosuser/Processing/OC_PROC_EIS202207_NRTNASA_EDS/uploaddu/eistools/reformat_uploadtoDBS_202207.py -m MY -pname OCEANCOLOUR_BAL_BGC_L3_MY_009_133  -dname  cmems_obs-oc_bal_bgc-optics_my_l3-olci-300m_P1D -sd {dateherestr} -ed {dateherestr} -v -noreformat'
    # lp = f'/usr/local/anaconda/anaconda3/bin/python /home/gosuser/Processing/OC_PROC_EIS202207_NRTNASA_EDS/uploaddu/eistools/reformat_uploadtoDBS_202207.py -m MY -pname OCEANCOLOUR_BAL_BGC_L3_MY_009_133  -dname  cmems_obs-oc_bal_bgc-plankton_my_l3-olci-300m_P1D -sd {dateherestr} -ed {dateherestr} -v -noreformat'
    # lt = f'/usr/local/anaconda/anaconda3/bin/python /home/gosuser/Processing/OC_PROC_EIS202207_NRTNASA_EDS/uploaddu/eistools/reformat_uploadtoDBS_202207.py -m MY -pname OCEANCOLOUR_BAL_BGC_L3_MY_009_133  -dname  cmems_obs-oc_bal_bgc-transp_my_l3-olci-300m_P1D -sd {dateherestr} -ed {dateherestr} -v -noreformat'
    lr = f'/usr/local/anaconda/anaconda3/bin/python /home/gosuser/Processing/OC_PROC_EIS202207_NRTNASA_EDS/uploaddu/eistools/reformat_uploadtoDBS_202207.py -m MY -pname OCEANCOLOUR_BAL_BGC_L3_MY_009_133  -dname  cmems_obs-oc_bal_bgc-reflectance_my_l3-olci-300m_P1D -sd {dateherestr} -ed {dateherestr} -v'
    lo = f'/usr/local/anaconda/anaconda3/bin/python /home/gosuser/Processing/OC_PROC_EIS202207_NRTNASA_EDS/uploaddu/eistools/reformat_uploadtoDBS_202207.py -m MY -pname OCEANCOLOUR_BAL_BGC_L3_MY_009_133  -dname  cmems_obs-oc_bal_bgc-optics_my_l3-olci-300m_P1D -sd {dateherestr} -ed {dateherestr} -v'
    lp = f'/usr/local/anaconda/anaconda3/bin/python /home/gosuser/Processing/OC_PROC_EIS202207_NRTNASA_EDS/uploaddu/eistools/reformat_uploadtoDBS_202207.py -m MY -pname OCEANCOLOUR_BAL_BGC_L3_MY_009_133  -dname  cmems_obs-oc_bal_bgc-plankton_my_l3-olci-300m_P1D -sd {dateherestr} -ed {dateherestr} -v'
    lt = f'/usr/local/anaconda/anaconda3/bin/python /home/gosuser/Processing/OC_PROC_EIS202207_NRTNASA_EDS/uploaddu/eistools/reformat_uploadtoDBS_202207.py -m MY -pname OCEANCOLOUR_BAL_BGC_L3_MY_009_133  -dname  cmems_obs-oc_bal_bgc-transp_my_l3-olci-300m_P1D -sd {dateherestr} -ed {dateherestr} -v'
    return lr, lo, lp, lt


def get_lines_correction(dateherestr, yearstr, jjjstr):
    linebase = f'python /home/gosuser/Processing/OC_PROC_EIS202211/s3olciProcessing/aceasy/main.py -ac BALALL -c /home/gosuser/Processing/OC_PROC_EIS202211/s3olciProcessing/CONFIG -i /store/COP2-OC-TAC/BAL_Evolutions/POLYMER_WATER -o /store/COP2-OC-TAC/BAL_Evolutions/BAL_REPROC  -sd {dateherestr} -ed {dateherestr} -v'
    l1 = f'rm /store/COP2-OC-TAC/BAL_Evolutions/BAL_REPROC/{yearstr}/{jjjstr}/*'
    l2 = linebase.replace('CONFIG', 'aceasy_config.ini')
    l3 = linebase.replace('CONFIG', 'aceasy_config_ms.ini')
    l4 = linebase.replace('CONFIG', 'aceasy_config_merge.ini')
    l5 = linebase.replace('CONFIG', 'aceasy_config_reformat.ini')
    return l1, l2, l3, l4, l5


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
            du_server = "my.cmems-du.eu"
        elif mode == 'NRT' or mode == 'DT':
            du_server = "nrt.cmems-du.eu"
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

    def get_last_modified(self, fname):
        try:
            cmd = f'MDTM {fname}'
            timestamp = self.ftpdu.voidcmd(cmd)[4:].strip()
            from dateutil import parser
            time = parser.parse(timestamp)
            return time
        except:
            return None



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
