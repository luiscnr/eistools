import argparse
from datetime import datetime as dt
from datetime import timedelta
from source_info import SourceInfo
import calendar

parser = argparse.ArgumentParser(description='Check upload')
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
parser.add_argument("-m", "--mode", help="Mode.", type=str, required=True, choices=['COPYAQUA'])

parser.add_argument("-sd", "--start_date", help="Start date (yyyy-mm-dd)")
parser.add_argument("-ed", "--end_date", help="Start date (yyyy-mm-dd)")

args = parser.parse_args()

def main():
    if args.mode=='COPYAQUA':
        copy_aqua()



def copy_aqua():
    start_date, end_date = get_dates()
    sinfo = SourceInfo('202207')
    sinfo.start_source('AQUA')
    for y in range(start_date.year, end_date.year + 1):
        monthIni = 1
        monthEnd = 12
        if y==start_date.year:
            monthIni = start_date.month
        if y==end_date.year:
            monthEnd = end_date.month
        for m in range(monthIni, monthEnd + 1):

            day_ini = 1
            day_fin = calendar.monthrange(y, m)[1]
            if m == start_date.month and y== start_date.year:
                day_ini = start_date.day
            if m == end_date.month and y==end_date.year:
                day_fin = end_date.day

            for d in range(day_ini,day_fin+1):
                date_here = dt(y,m,d)
                if args.verbose:
                    print(f'[INFO] Copying files for date: {date_here}')
                sinfo.get_last_session_id('NRT','BAL',date_here)
                proc_folder = sinfo.get_processing_folder()
                if args.verbose:
                    print(f'[INFO] Processing folder: ',proc_folder)




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