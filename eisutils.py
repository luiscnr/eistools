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
                    choices=['COPYAQUA', 'CHECKFTPCONTENTS', 'CHECKGRANULES','CHECKSOURCES'])
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

def check_sources():
    dir_orig = '/store/COP2-OC-TAC/OLCI_FTP_EUMETSAT'
    #arc
    dir_sources = '/store/COP2-OC-TAC/arc/sources'
    check_sources_impl(dir_orig,dir_sources,'arg')

def check_sources_impl(dir_orig,dir_sources,region):
    file_orig = os.path.join(dir_orig,f'new_granules_{region}.csv')
    list_granules = {}
    list_granules_date = []
    date_prev_str  = None
    f1 = open(file_orig,'r')
    for line in f1:
        jjj = line.split(';')[0].strip()
        granule = line.split(';')[1].strip()
        date_here_str = f'2023{jjj}'
        if date_prev_str is None:
            list_granules_date = [granule]
            date_prev_str = date_here_str
        else:
            if date_here_str==date_prev_str:
                list_granules_date.append(granule)
            else:
                date_here = dt.strptime(date_prev_str, '%Y%j')
                if region=='arc':
                    folder_date = os.path.join(dir_sources,date_here.strftime('%Y%m%d'))
                else:
                    folder_date = os.path.join(dir_sources,'2023',date_here.strftime('%j'))
                list_granules[date_prev_str] = {
                    'list':list_granules,
                    'folder': folder_date
                }
                list_granules_date = [granule]
                date_prev_str = date_here_str
    date_here = dt.strptime(date_prev_str, '%Y%j')
    if region == 'arc':
        folder_date = os.path.join(dir_sources, date_here.strftime('%Y%m%d'))
    else:
        folder_date = os.path.join(dir_sources, '2023', date_here.strftime('%j'))
    list_granules[date_prev_str] = {
        'list': list_granules_date,
        'folder': folder_date
    }
    f1.close()

    file_remove = os.path.join(dir_orig,f'ToRemove_{region}.sh')
    fw = open(file_remove,'w')

    for date_ref in list_granules:
        list = list_granules[date_ref]['list']
        folder = list_granules[date_ref]['folder']
        for name in os.listdir(folder):
            b = check_grunule_in_list(name,list)
            if b:
                fout = os.path.join(folder,name)
                fw.write(f'mv {fout} /store/COP2-OC-TAC/OLCI_FTP_EUMETSAT/2023')
    fw.close()

def check_granules():

    ##WFR LIST
    #file_new_granules = '/mnt/c/DATA_LUIS/TEMPORAL/2023/S3_WFR_Granules.csv'
    #check_granules_region('arc',file_new_granules)
    #check_granules_region('med',file_new_granules)
    #check_granules_region('blk', file_new_granules)

    ##WRR LIST
    #file_new_granules = '/mnt/c/DATA_LUIS/TEMPORAL/2023/S3_WRR_Granules.csv'
    #check_granules_region('med_rr', file_new_granules)
    #check_granules_region('blk_rr', file_new_granules)

    ##ERR LIST
    file_new_granules = '/mnt/c/DATA_LUIS/TEMPORAL/2023/S3_EFR_Granules.csv'
    check_granules_region('bal', file_new_granules)


def check_granules_region(region,file_new_granules):
    input_path = '/mnt/c/DATA_LUIS/TEMPORAL/2023'
    import pandas as pd
    output_file = f'/mnt/c/DATA_LUIS/TEMPORAL/new_granules_{region}.csv'
    f1 = open(output_file, 'w')
    f1.write('Day;Granule')
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
            b = check_grunule_in_list(granule, list_granules)
            if b:
                f1.write('\n')
                f1.write(f'{day};{granule}')
    f1.close()


def check_grunule_in_list(granule, list_granules):
    format = '%Y%m%dT%H%M%S'
    start_date_granule = dt.strptime(granule.split('_')[7], format)
    end_date_granule = dt.strptime(granule.split('_')[8], format)
    platform = granule.split('_')[0]
    for g in list_granules:
        if g.startswith(platform):
            start_date_g = dt.strptime(g.split('_')[7], format)
            end_date_g = dt.strptime(g.split('_')[8], format)
            overlap = compute_overlap(start_date_granule, end_date_granule, start_date_g, end_date_g)
            if overlap>0.99:
               return True

    return False


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
    if sdcheck<sd and edcheck>ed:
        overlap = 1
    if sdcheck <= ed <= edcheck:
        overlap = (ed - sdcheck).total_seconds() / total_s
    return overlap


def check_ftp_contents():
    from ftplib import FTP
    ftp_orig = FTP('cdrftp.eumetsat.int', 'cdr_ro', 'LNnh73tfAavaC3YmqXfzafVn')
    rpath = '/cdrftp/olci_l1l2_2023/S3B/OL_2_WRR/2023'
    output_path = '/mnt/c/DATA_LUIS/TEMPORAL/2023/S3B_WRR_Granules.csv'
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
