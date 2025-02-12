import argparse
import os.path
from datetime import datetime as dt

import pandas as pd

from nasa_download import NASA_DOWNLOAD
from datetime import timedelta
# import numpy as np

parser = argparse.ArgumentParser(description='NASA CHECK')
# parser.add_argument("-m", "--mode", help="Mode", choices=["NEXT_DAY", "CHECK_DAY", "MAIL", "SET_LAST"])
parser.add_argument("-sd", "--start_date",
                    help="Start date for checking NASA download (yyyy-mm-dd or relative integer)", required=True)
parser.add_argument("-ed", "--end_date", help="End date for checking NASA download (yyyy-mm-dd or relative integer",
                    required=True)
parser.add_argument("-o", "--output_path", help="Output directory for csv,json and mail outputs")
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
parser.add_argument("-csv", "--csv_output", help="Create output CSV.", action="store_true")
parser.add_argument("-json", "--json_output", help="Create output json.", action="store_true")
parser.add_argument("-mail", "--mail_output", help="Create output mail.", action="store_true")

args = parser.parse_args()


def main():
    start_date, end_date = get_dates()
    if start_date is None or end_date is None:
        return
    if args.csv_output or args.json_output or args.mail_output:
        if not args.output_path:
            print(f'[ERROR] Option -o (--output_path) is required for -json, -csv and -mail options')
            return
        if not os.path.isdir(args.output_path):
            try:
                os.mkdir(args.output_path)
            except:
                print(f'[ERROR] {args.output_path} is not valid directory and could not be created. Please review permissions')
                return
    sensors = ['AQUA', 'VIIRS', 'VIIRSJ']
    nsensors = [0]*(len(sensors)+1)
    nasad = NASA_DOWNLOAD()

    results = {}
    work_date = start_date
    while work_date<=end_date:
        ref = work_date.strftime('%Y%m%d')
        results[ref] = {x:-2 for x in sensors}
        make_multi = 1
        line = f'[INFO] Date: {ref}'
        for isensor,sensor in enumerate(sensors):
            results[ref][sensor] = nasad.check_scenes_med_bs_API(sensor,work_date)
            line = f'{line} {sensor}:{results[ref][sensor]}'
            if results[ref][sensor]==0 or results[ref][sensor]==-2:
                make_multi = 0
            if results[ref][sensor] == 1:
                nsensors[isensor] = nsensors[isensor]+1
        line = f'{line} MULTI: {make_multi}'
        if args.verbose:
            print(line)
        results[ref]['MULTI'] = make_multi
        if make_multi:
            nsensors[-1] = nsensors[-1]+1
        work_date = work_date + timedelta(hours=24)

    if args.csv_output or args.json_output or args.mail_output:
        name_out = f'NASA_CHECK_{start_date.strftime("%Y%m%d")}_{end_date.strftime("%Y%m%d")}'
        if args.json_output:
            file_out = os.path.join(args.output_path, f'{name_out}.json')
            if os.path.isfile(file_out):
                print(f'[WARNING] File {file_out} already exists. It will be overwritten.')
            try:
                import json
                with open(file_out, "w", encoding='utf8') as outfile:
                    json.dump(results, outfile, indent=1, ensure_ascii=False)
            except:
                print(f'[ERROR] Error creating JSON file {file_out}')

        if args.json_output:
            file_out = os.path.join(args.output_path, f'{name_out}.csv')
            if os.path.isfile(file_out):
                print(f'[WARNING] File {file_out} already exists. It will be overwritten.')
            try:
                import pandas
                date_list = list(results.keys())
                columns = list(results[date_list[0]].keys())
                df = pd.DataFrame(index=date_list,columns=columns)
                for date in results:
                    df.loc[date].at[columns[0]] = results[date][columns[0]]
                    df.loc[date].at[columns[1]] = results[date][columns[1]]
                    df.loc[date].at[columns[2]] = results[date][columns[2]]
                    df.loc[date].at[columns[3]] = results[date][columns[3]]
                df.to_csv(file_out,sep=';',index=True, index_label='Date')
            except:
                print(f'[ERROR] Error creating JSON file {file_out}')

        if args.mail_output:
            file_out = os.path.join(args.output_path, f'{name_out}.mail')
            if os.path.isfile(file_out):
                print(f'[WARNING] File {file_out} already exists. It will be overwritten.')
            fw = open(file_out,'w')
            fw.write('CHECK NASA DOWNLOAD')
            add_new_line(fw,'-------------------')
            add_new_line(fw,f'Checking start date: {start_date.strftime("%Y-%m-%d")}')
            add_new_line(fw,f'Checking end date: {end_date.strftime("%Y-%m-%d")}')
            add_new_line(fw,f'Number of days: {(end_date-start_date).days+1}')
            add_new_line(fw,'')
            add_new_line(fw,f'Number of available files: ')
            for isensor in range(len(sensors)):
                add_new_line(fw,f'->{sensors[isensor]}: {nsensors[isensor]}')
            add_new_line(fw,'')
            add_new_line(fw,f'Number of dates available for MULTI processing: {nsensors[-1]}')
            add_new_line(fw,f'')
            add_new_line(fw,f'Please use the following script to proccess the data: ')
            add_new_line(fw,'')
            add_new_line(fw,'xxx')
            fw.close()

def add_new_line(fw,line):
    fw.write('\n')
    fw.write(line)
def get_dates():
    ##DATES SELECTION
    if not args.start_date and not args.end_date:
        print(f'[ERROR] Start date(-sd) is not given.')
        return [None] * 2
    start_date_p = args.start_date
    if args.end_date:
        end_date_p = args.end_date
    else:
        end_date_p = start_date_p
    start_date = get_date_from_param(start_date_p)
    end_date = get_date_from_param(end_date_p)
    if start_date is None:
        print(
            f'[ERROR] Start date {start_date_p} is not in the correct format. It should be YYYY-mm-dd or integer (relative days)')
        return [None] * 2
    if end_date is None:
        print(
            f'[ERROR] End date {end_date_p} is not in the correct format. It should be YYYY-mm-dd or integer (relative days)')
        return [None] * 2
    if start_date > end_date:
        print(f'[ERROR] End date should be greater or equal than start date')
        return [None] * 2
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


if __name__ == '__main__':
    main()
