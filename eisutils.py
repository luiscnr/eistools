import argparse
import shutil
from datetime import datetime as dt
from datetime import timedelta
from source_info import SourceInfo
import calendar
import os

parser = argparse.ArgumentParser(description='Check upload')
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
parser.add_argument("-m", "--mode", help="Mode.", type=str, required=True, choices=['COPYAQUA'])

parser.add_argument("-sd", "--start_date", help="Start date (yyyy-mm-dd)")
parser.add_argument("-ed", "--end_date", help="Start date (yyyy-mm-dd)")

args = parser.parse_args()


def main():
    if args.mode == 'COPYAQUA':
        copy_aqua()


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
        print(line)
        if line.startswith('AQUA_MODIS'):
            datehere = dt.strptime(line.split('.')[1], '%Y%m%dT%H%M%S')
            datehere = datehere.replace(second=0)
            print(datehere)
            datehereold = datehere.strftime('%Y%j%H%M%s')
            fname = f'A{datehereold}.L2_LAC_OC.nc'
            filea = os.path.join(proc_folder, fname)
            print(filea)
            if os.path.exists(filea):
                filelist.append(filea)
        else:
            filea = os.path.join(proc_folder, line.strip())
            if os.path.exists(filea):
                filelist.append((filea))
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
