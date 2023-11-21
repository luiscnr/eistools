import argparse
import shutil
from datetime import datetime as dt
from datetime import timedelta
from source_info import SourceInfo
import calendar
import os

parser = argparse.ArgumentParser(description='Check upload')
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
parser.add_argument("-m", "--mode", help="Mode.", type=str, required=True,
                    choices=['COPYAQUA', 'CHECKFTPCONTENTS', 'CHECKGRANULES', 'CHECKSOURCES', 'ZIPGRANULES'])
parser.add_argument("-p", "--path", help="Path for FTPDOWNLOAD")
parser.add_argument("-sd", "--start_date", help="Start date (yyyy-mm-dd)")
parser.add_argument("-ed", "--end_date", help="Start date (yyyy-mm-dd)")

args = parser.parse_args()


def main():
    if args.mode == 'COPYAQUA':
        copy_aqua()
    if args.mode == 'CHECKFTPCONTENTS':
        check_ftp_contents()
    if args.mode == 'CHECKGRANULES':
        check_granules()
    if args.mode == 'CHECKSOURCES':
        check_sources()
    if args.mode == 'ZIPGRANULES':
        check_zip()


def check_zip():
    #MED and BLK
    # source_dir = '/dst04-data1/OC/OLCI/sources_baseline_3.01/2023'
    # output_file = '/mnt/c/DATA_LUIS/TEMPORAL/zip_granules_blk.slurm'
    # file_new_granules = '/mnt/c/DATA_LUIS/TEMPORAL/2023/new_granules_blk_rr.csv'
    #BAL
    download_dir = '/store/COP2-OC-TAC/OLCI_FTP_EUMETSAT/cdrftp.eumetsat.int/cdrftp/olci_l1l2_2023'
    source_dir = '/store/COP2-OC-TAC/BAL_Evolutions/sources/2023'
    file_new_granules = '/mnt/c/DATA_LUIS/TEMPORAL/2023/new_granules_bal.csv'
    dir_sensor = 'OL_1_EFR'
    output_file = '/mnt/c/DATA_LUIS/TEMPORAL/zip_granules_bal.slurm'

    sbatch_lines = [
        '#SBATCH --nodes=1',
        '#SBATCH --ntasks=1',
        '#SBATCH -p octac_rep',
        '#SBATCH --mail-type=BEGIN,END,FAIL',
        '#SBATCH --mail-user=luis.gonzalezvilas@artov.ismar.cnr.it',
        '',
        'source /home/gosuser/load_miniconda3.source',
        'conda activate op_proc_202211v1',
        'cd /store/COP2-OC-TAC/OLCI_FTP_EUMETSAT/eistools',
        ''
    ]

    fout = open(output_file,'w')
    fout.write('#!/bin/bash')
    for line in sbatch_lines:
        fout.write('\n')
        fout.write(line)

    import pandas as pd
    dset = pd.read_csv(file_new_granules,sep=';')
    for index,row in dset.iterrows():

        jjj = str(row['Day'])
        granule = str(row['Granule'])
        dir_platform = granule[0:3]
        dir_input = os.path.join(download_dir,dir_platform,dir_sensor,'2023',jjj)
        file_input = os.path.join(dir_input,f'{granule}.zip')
        dir_output = os.path.join(source_dir,jjj)
        file_output = os.path.join(dir_output, f'{granule}.zip')

        cmd = f'cd {dir_input} && zip -r {granule}.zip {granule} && mv {file_input} {file_output}'
        print(cmd)
        fout.write('\n')
        fout.write(cmd)

    fout.close()


def check_sources():
    dir_orig = '/store/COP2-OC-TAC/OLCI_FTP_EUMETSAT'
    #dir_orig = '/mnt/c/DATA_LUIS/TEMPORAL'

    # arc
    prename = 'ToRemove_'
    dir_sources = '/store/COP2-OC-TAC/arc/sources'
    check_sources_impl(dir_orig, dir_sources, 'arc',prename,'_OL_2_WFR_')

    # med and blk
    # dir_sources = '/dst04-data1/OC/OLCI/sources_baseline_3.01'
    # prename = 'ToRemove_'
    # check_sources_impl(dir_orig,dir_sources,'med',prename,'_OL_2_WFR_')
    # check_sources_impl(dir_orig, dir_sources, 'blk',prename,'_OL_2_WFR_')
    # prename = 'ToRemoveRR_'
    # check_sources_impl(dir_orig, dir_sources, 'med_rr',prename,'_OL_2_WRR_')
    # check_sources_impl(dir_orig, dir_sources, 'blk_rr',prename,'_OL_2_WRR_')
    # dir_sources = '/dst04-data1/OC/OLCI/trimmed_sources'
    # prename = 'ToRemoveTrim_'
    # check_sources_impl(dir_orig, dir_sources, 'med',prename,'_OL_2_WFR_')
    # check_sources_impl(dir_orig, dir_sources, 'blk',prename,'_OL_2_WFR_')
    # prename = 'ToRemoveTrimRR_'
    # check_sources_impl(dir_orig, dir_sources, 'med_rr',prename,'_OL_2_WRR_')
    # check_sources_impl(dir_orig, dir_sources, 'blk_rr',prename,'_OL_2_WRR_')




def check_sources_impl(dir_orig, dir_sources, region, prename,wce):
    file_orig = os.path.join(dir_orig, f'new_granules_{region}.csv')
    list_granules_dict = {}
    list_granules_date = []
    date_prev_str = None
    f1 = open(file_orig, 'r')
    for line in f1:
        jjj = line.split(';')[0].strip()
        if jjj == 'Day':  # FIRST LINE
            continue
        granule = line.split(';')[1].strip()
        date_here_str = f'2023{jjj}'
        if date_prev_str is None:
            list_granules_date = [granule]
            date_prev_str = date_here_str
        else:
            if date_here_str == date_prev_str:
                list_granules_date.append(granule)
            else:
                date_here = dt.strptime(date_prev_str, '%Y%j')
                if region == 'arc':
                    folder_date = os.path.join(dir_sources, date_here.strftime('%Y%m%d'))
                else:
                    folder_date = os.path.join(dir_sources, '2023', date_here.strftime('%j'))
                list_granules_dict[date_prev_str] = {
                    'list': list_granules_date,
                    'folder': folder_date
                }
                list_granules_date = [granule]
                date_prev_str = date_here_str
    date_here = dt.strptime(date_prev_str, '%Y%j')
    if region == 'arc':
        folder_date = os.path.join(dir_sources, date_here.strftime('%Y%m%d'))
    else:
        folder_date = os.path.join(dir_sources, '2023', date_here.strftime('%j'))
    list_granules_dict[date_prev_str] = {
        'list': list_granules_date,
        'folder': folder_date
    }
    f1.close()

    file_remove = os.path.join(dir_orig, f'{prename}{region}.sh')
    fw = open(file_remove, 'w')

    for date_ref in list_granules_dict:
        list = list_granules_dict[date_ref]['list']
        folder = list_granules_dict[date_ref]['folder']
        list_applied = [0]*len(list)
        for name in os.listdir(folder):
            ifind = name.find(wce)
            if not name.startswith('S3'):
                continue
            if ifind<0:
                continue
            index_g = check_grunule_in_list(name, list)
            if index_g>=0:
                list_applied[index_g] = 1
                fout = os.path.join(folder, name)
                fw.write('\n')
                fw.write(f'mv {fout} /store/COP2-OC-TAC/OLCI_FTP_EUMETSAT/2023')

        for i in range(len(list_applied)):
            if list_applied[i]==0:
                print(f'[INFO] Not found: {date_ref} -> {list[i]}')

    fw.close()


def check_granules():
    ##WFR LIST
    file_new_granules = '/mnt/c/DATA_LUIS/TEMPORAL/2023/S3_WFR_Granules.csv'
    # #check_granules_region('arc',file_new_granules)
    # check_granules_region('med',file_new_granules)
    # check_granules_region('blk', file_new_granules)
    check_granules_region('arc',file_new_granules)

    ##WRR LIST
    # file_new_granules = '/mnt/c/DATA_LUIS/TEMPORAL/2023/S3_WRR_Granules.csv'
    # check_granules_region('med_rr', file_new_granules)
    # check_granules_region('blk_rr', file_new_granules)

    ##EFR LIST
    # file_new_granules = '/mnt/c/DATA_LUIS/TEMPORAL/2023/S3_EFR_Granules.csv'
    # check_granules_region('bal', file_new_granules)


def check_granules_region(region, file_new_granules):
    input_path = '/mnt/c/DATA_LUIS/TEMPORAL/2023'
    import pandas as pd

    sbatch_lines = [
    '#SBATCH --nodes=1',
    '#SBATCH --ntasks=1',
    '#SBATCH -p octac_rep',
    '#SBATCH --mail-type=BEGIN,END,FAIL',
    '#SBATCH --mail-user=luis.gonzalezvilas@artov.ismar.cnr.it',
    '',
    'source /home/gosuser/load_miniconda3.source',
    'conda activate op_proc_202211v1',
    'cd /store/COP2-OC-TAC/OLCI_FTP_EUMETSAT/eistools',
    ''
    ]

    output_file = f'{input_path}/new_granules_{region}.csv'
    f1 = open(output_file, 'w')
    f1.write('Day;Granule')

    output_ftp_files = f'{input_path}/ftp_granules_{region}.slurm'
    output_mv_files = f'{input_path}/mv_granules_{region}.slurm'
    output_ftp_folder = '/store/COP2-OC-TAC/OLCI_FTP_EUMETSAT'
    start = 'wget -m --user=cdr_ro --password=LNnh73tfAavaC3YmqXfzafVn  ftp://cdrftp.eumetsat.int'

    if region=='bal':
        input_ftp_folders = {
            'S3A': '/cdrftp/olci_l1l2_2023/S3A/OL_1_EFR/',
            'S3B': '/cdrftp/olci_l1l2_2023/S3B/OL_1_EFR/',
        }
        source_folder = '/store/COP2-OC-TAC/BAL_Evolutions/sources/'
    if region=='med' or region=='blk' or region=='arc':
        input_ftp_folders  = {
            'S3A': '/cdrftp/olci_l1l2_2023/S3A/OL_2_WFR/',
            'S3B': '/cdrftp/olci_l1l2_2023/S3B/OL_2_WFR/',
        }
        if region == 'med' or region == 'blk':
            source_folder = '/dst04-data1/OC/OLCI/sources_baseline_3.01/'
        if region == 'arc':
            source_folder = '/store/COP2-OC-TAC/arc/sources'
    if region=='med_rr' or region=='blk_rr':
        input_ftp_folders = {
            'S3A': '/cdrftp/olci_l1l2_2023/S3A/OL_2_WRR/',
            'S3B': '/cdrftp/olci_l1l2_2023/S3B/OL_2_WRR/',
        }
        source_folder = '/dst04-data1/OC/OLCI/sources_baseline_3.01/'

    fftp = open(output_ftp_files, 'w')
    fmv = open(output_mv_files, 'w')
    fftp.write('#!/bin/bash')
    fmv.write('#!/bin/bash')
    for line in sbatch_lines:
        fftp.write('\n')
        fftp.write(line)
        fmv.write('\n')
        fmv.write(line)


    dset = pd.read_csv(file_new_granules, sep=';')
    date_here_prev = None
    list_granules = []
    for index, row in dset.iterrows():
        day = row['Day']
        granule = row['Granule']
        date_here_str = dt.strptime(f'2023{day}', '%Y%j').strftime('%Y%m%d')
        if date_here_prev is None or date_here_str != date_here_prev:
            flist = os.path.join(input_path, f'eum_filelist_{region}_{date_here_str}.txt')
            list_granules = []
            if os.path.exists(flist):
                fg = open(flist)
                for g in fg:
                    list_granules.append(g)
                fg.close()
            date_here_prev = date_here_str
        if len(list_granules) > 0:
            index_g = check_grunule_in_list(granule, list_granules)
            if index_g>=0:
                f1.write('\n')
                f1.write(f'{day};{granule}')

                platform = granule.split('_')[0]
                input_ftp_folder = input_ftp_folders[platform]
                fftp.write('\n')
                str = f'{start}{input_ftp_folder}2023/{day}/{granule} -P {output_ftp_folder}'
                fftp.write(str)
                fmv.write('\n')
                str = f'mv {output_ftp_folder}/cdrftp.eumetsat.int{input_ftp_folder}2023/{day}/{granule} {source_folder}2023/{day}'
                fmv.write(str)
    f1.close()
    fftp.close()
    fmv.close()

def check_grunule_in_list(granule, list_granules):
    format = '%Y%m%dT%H%M%S'
    start_date_granule = dt.strptime(granule.split('_')[7], format)
    end_date_granule = dt.strptime(granule.split('_')[8], format)
    platform = granule.split('_')[0]
    for indexg in range(len(list_granules)):
        g = list_granules[indexg]
        if g.startswith(platform):
            start_date_g = dt.strptime(g.split('_')[7], format)
            end_date_g = dt.strptime(g.split('_')[8], format)
            overlap = compute_overlap(start_date_granule, end_date_granule, start_date_g, end_date_g)
            if overlap > 0.50:
                return indexg

    return -1


def compute_overlap(sd, ed, sdcheck, edcheck):
    overlap = 0
    total_s = (ed - sd).total_seconds()
    if edcheck < sd:
        return overlap
    if sdcheck > ed:
        return overlap
    if sdcheck <= sd <= edcheck:
        overlap = (edcheck - sd).total_seconds() / total_s
    if sdcheck >= sd and edcheck <= ed:
        overlap = (edcheck - sdcheck).total_seconds() / total_s
    if sdcheck < sd and edcheck > ed:
        overlap = 1
    if sdcheck <= ed <= edcheck:
        overlap = (ed - sdcheck).total_seconds() / total_s
    return overlap


def check_ftp_contents():
    from ftplib import FTP
    ftp_orig = FTP('cdrftp.eumetsat.int', 'cdr_ro', 'LNnh73tfAavaC3YmqXfzafVn')
    rpath = '/cdrftp/olci_l1l2_2023/S3B/OL_1_EFR/2023'
    output_path = '/mnt/c/DATA_LUIS/TEMPORAL/2023/S3B_EFR_Granules.csv'
    f1 = open(output_path, 'w')
    f1.write('Day;Granule')
    for jjj in range(108, 132):
        ftp_orig.cwd(rpath)
        jjjs = str(jjj)
        if jjjs in ftp_orig.nlst():
            print(f'[INFO] Day: {jjjs}')
            jjj_path = f'{rpath}/{jjjs}'

            ftp_orig.cwd(jjj_path)
            for name in ftp_orig.nlst():
                f1.write('\n')
                f1.write(f'{jjjs};{name}')
    f1.close()


def copy_aqua():
    start_date, end_date = get_dates()
    sinfo = SourceInfo('202207')
    sinfo.start_source('AQUA')
    for y in range(start_date.year, end_date.year + 1):
        monthIni = 1
        monthEnd = 12
        if y == start_date.year:
            monthIni = start_date.month
        if y == end_date.year:
            monthEnd = end_date.month
        for m in range(monthIni, monthEnd + 1):

            day_ini = 1
            day_fin = calendar.monthrange(y, m)[1]
            if m == start_date.month and y == start_date.year:
                day_ini = start_date.day
            if m == end_date.month and y == end_date.year:
                day_fin = end_date.day

            for d in range(day_ini, day_fin + 1):
                date_here = dt(y, m, d)
                if args.verbose:
                    print('----------------------------------------------')
                    print(f'[INFO] Copying files for date: {date_here}')
                copy_aqua_impl(sinfo, date_here, 'MED')
                copy_aqua_impl(sinfo, date_here, 'BS')


def copy_aqua_impl(sinfo, date_here, region):
    sinfo.get_last_session_id('NRT', region, date_here)
    proc_folder = sinfo.get_processing_folder()
    if args.verbose:
        print(f'[INFO]   Region: ', region)
        print(f'[INFO]   Session ID: ', sinfo.sessionid)
        print(f'[INFO]   Processing folder: ', proc_folder)
    flist = os.path.join(proc_folder, 'daily_L2_files.list')
    file_list = get_files_aqua_from_list(proc_folder, flist)
    if len(file_list) > 0:
        for f in file_list:
            name = f.split('/')[-1]
            year = date_here.strftime('%Y')
            jday = date_here.strftime('%j')
            fout = f'/store3/OC/MODISA/sources/{year}/{jday}/{name}'
            if not os.path.exists(fout):
                if args.verbose:
                    print(f'[INFO]   Copying {f} to {fout}')
                shutil.copy(f, fout)


def get_files_aqua_from_list(proc_folder, file_list):
    file1 = open(file_list, 'r')
    filelist = []
    for line in file1:
        line = line.strip()
        if line.startswith('AQUA_MODIS'):
            datehere = dt.strptime(line.split('.')[1], '%Y%m%dT%H%M%S')
            datehere = datehere.replace(second=0)
            datehereold = datehere.strftime('%Y%j%H%M%S')
            fname = f'A{datehereold}.L2_LAC_OC.nc'
            filea = os.path.join(proc_folder, fname)
            if os.path.exists(filea):
                filelist.append(filea)
        else:
            filea = os.path.join(proc_folder, line.strip())
            if os.path.exists(filea):
                filelist.append(filea)
    file1.close()
    return filelist


def get_dates():
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


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
