import argparse
import os
from datetime import datetime as dt
from datetime import timedelta
parser = argparse.ArgumentParser(description='Create slurm script for Baltic Sea')
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
parser.add_argument("-m", "--mode", help="Mode.", type=str, required=True,
                    choices=['DOWNLOAD','TRIM','POLYMER','PROCESSING','COMPLETE','UPLOAD'])
parser.add_argument("-p", "--path", help="Path for output")
parser.add_argument("-sd", "--start_date", help="Start date (yyyy-mm-dd)")
parser.add_argument("-ed", "--end_date", help="Start date (yyyy-mm-dd)")
parser.add_argument("-ld", "--list_dates",help="File with list dates as yyyy-mm-dd")

args = parser.parse_args()


def main():
    print(f'[STARTED]')
    path = '/store/COP2-OC-TAC/BAL_Evolutions/slurmscripts_rep'
    if args.path:
        path = args.path
    if not os.path.exists(path):
        print(f'[ERROR] Output path: {path} does not exist')
        return
    mode = args.mode
    file_out = os.path.join(path,f'lancia_bal_multiple_{mode.lower()}.sh')

    list_dates = []
    if args.list_dates and os.path.exists(args.list_dates):
        start_date, end_date = get_dates()
        f1 = open(args.list_dates,'r')
        for line in f1:
            try:
                date_here = dt.strptime(line.strip(),'%Y-%m-%d')
                if start_date is not None and end_date is not None:
                    if start_date <= date_here <= end_date:
                        list_dates.append(date_here)
                else:
                    list_dates.append(date_here)
            except:
                pass
        f1.close()
    else:
        start_date, end_date = get_dates()
        if start_date is None or end_date is None:
            return
        while start_date<=end_date:
            list_dates.append(start_date)
            start_date = start_date + timedelta(hours=24)
    if len(list_dates)==0:
        print(f'[ERROR] No dates were selected')
        return


    fout = open(file_out,'w')
    fout.write('#!/bin/bash')
    write_line(fout,'')
    write_line(fout,f'sdir="/store/COP2-OC-TAC/BAL_Evolutions/slurmscripts_rep"')
    write_line(fout,f'logdirbase="$sdir/log_files"')
    write_line(fout,'')
    write_line(fout,'')
    str_print_nf = "'{print $NF}'"
    write_line(fout,f'#{mode}')
    for idx in range(len(list_dates)):
        date_here = list_dates[idx]
        date_here_prefix = date_here.strftime('%Y_%m_%d')
        date_here_script = date_here.strftime('%Y-%m-%d')
        index = idx + 1
        prefix = f'BAL_PROCESSING_DT_{date_here_prefix}'
        #write_line(fout,f'prefix={prefix}')
        write_line(fout,f'slurmlog="$logdirbase/{prefix}_slurm_{mode.lower()}.out"')
        write_line(fout,f'pylog="$logdirbase/{prefix}_log_{mode.lower()}.log"')
        if index==1:
            line_ini = f'job{index}=$(sbatch --output=$slurmlog $sdir/'
        else:
            index_prev = index-1
            line_ini = f'job{index}=$(sbatch --dependency=afterany:$job{index_prev}id --output=$slurmlog $sdir/'

        if mode=='DOWNLOAD':
            line_fin = f'make_download_bal.slurm {date_here_script} {date_here_script} $pylog)'

        if mode=='TRIM':
            line_fin = f'make_trim_bal.slurm NT {date_here_script} {date_here_script} $pylog)'

        if mode=='POLYMER':
            line_fin = f'make_polymer.slurm NT  {date_here_script} {date_here_script} $pylog)'

        if mode=='PROCESSING':
            line_fin = f'make_processing_bal.slurm NT {date_here_script} {date_here_script} $pylog)'

        if mode=='COMPLETE':
            line_fin = f'make_complete_bal.slurm NT {date_here_script})'

        if mode=='UPLOAD':
            line_fin = f'make_upload_bal.slurm DT {date_here_script} {date_here_script} $pylog)'

        write_line(fout,f'{line_ini}{line_fin}')
        write_line(fout,f'job{index}id=$(echo "$job{index}" | awk {str_print_nf})')
        write_line(fout,'')





    fout.close()


def write_line(fout,line):
    fout.write('\n')
    fout.write(line)

def get_dates():
    ##DATES SELECTION
    if not args.start_date and not args.end_date:
        print(f'[ERROR] Start date(-sd) is not given.')
        return None,None
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
        return None,None
    if end_date is None:
        print(
            f'[ERROR] End date {end_date_p} is not in the correct format. It should be YYYY-mm-dd or integer (relative days')
        return None,None
    if start_date > end_date:
        print(f'[ERROR] End date should be greater or equal than start date')
        return None,None
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
