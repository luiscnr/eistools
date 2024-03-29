import os
import json
import argparse
from nasa_download import NASA_DOWNLOAD
from datetime import datetime as dt
from datetime import timedelta
import numpy as np

parser = argparse.ArgumentParser(description='NASA CHECK')
parser.add_argument("-m", "--mode", help="Mode", choices=["NEXT_DAY", "CHECK_DAY", "MAIL", "SET_LAST"])
parser.add_argument("-pd", "--processing_date", help="Processing date for MAIL option")
args = parser.parse_args()


# def check_next_day():
#     sdir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
#     # sdir = '/mnt/c/DATA_LUIS/OCTAC_WORK/CC0C-591-_100days/'
#     file_json = os.path.join(sdir, 'info100days.json')
#     if not os.path.exists(file_json):
#         print(f'NONE;File: {file_json} does not exist')
#         return
#
#     with open(file_json, 'r', encoding='utf8') as j:
#         try:
#             jdict = json.loads(j.read())
#         except:
#             print(f'NONE; File: {file_json} is not a valid json file')
#             return
#
#     jdict, msg = check_sensor_dates(jdict)
#
#     if jdict is None:
#         print(msg)
#         return
#
#     with open(file_json, "w", encoding='utf8') as outfile:
#         json.dump(jdict, outfile, indent=1, ensure_ascii=False)
#
#     try:
#         processing_date = dt.strptime(jdict['PROCESSED'], '%Y-%m-%d') + timedelta(days=1)
#         multi_date = dt.strptime(jdict['MULTI'], '%Y-%m-%d')
#         processing_date_str = processing_date.strftime('%Y-%m-%d')
#         multi_date_str = multi_date.strftime('%Y-%m-%d')
#         if processing_date <= multi_date:
#             print(f'{processing_date_str};OK')
#         else:
#             print(
#                 f'NONE; Date {processing_date_str} can not be processed. Date are available only until {multi_date_str}')
#     except:
#         print(f'NONE; Error retrieving MULTI and PROCESSED dates from json file: {file_json}')


def check_sensor_dates(jdict):
    msg = ''
    sensors = ['AQUA', 'VIIRS', 'VIIRSJ']
    nasad = NASA_DOWNLOAD()
    last_date_multi = None
    for s in sensors:
        if s not in jdict.keys():
            msg = f'NONE; AQUA section is not included in json file'
            return None, msg
        try:
            last_date = dt.strptime(jdict[s], '%Y-%m-%d')
        except:
            msg = f'NONE; Last date for {s} is not in the correct format: YYYY-mm-dd'
            return None, msg
        date_check = last_date + timedelta(days=1)
        date_end = dt.now().replace(hour=0, minute=0, second=0, microsecond=0)
        while date_check <= date_end:
            nfiles = nasad.check_dt_files(date_check, s)
            if nfiles == -1:
                msg = f'NONE; Last date for {s} could not be checked.'
                return None, msg
            elif nfiles > 0:
                last_date = date_check
            elif nfiles == 0:
                break
            date_check = date_check + timedelta(days=1)
        jdict[s] = last_date.strftime('%Y-%m-%d')
        if last_date_multi is None:
            last_date_multi = last_date
        else:
            if last_date < last_date_multi:
                last_date_multi = last_date

    jdict['MULTI'] = last_date_multi.strftime('%Y-%m-%d')

    return jdict, msg


def check_date():
    sdir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    # sdir = '/mnt/c/DATA_LUIS/OCTAC_WORK/CC0C-591-_100days/'
    file_json = os.path.join(sdir, 'info100days.json')
    if not os.path.exists(file_json):
        msg = f'[ERROR] File: {file_json} does not exist'
        print(msg)
        return
    with open(file_json, 'r', encoding='utf8') as j:
        try:
            jdict = json.loads(j.read())
        except:
            msg = f'[ERROR] File: {file_json} is not a valid json file'
            print(msg)
            return
    if args.processing_date:
        try:
            processing_date = dt.strptime(args.processing_date, '%Y-%m-%d')
        except:
            msg = f'[ERROR] Date: {args.processing_date} is not valid'
            print(msg)
            return

    multi_date = dt.strptime(jdict['MULTI'], '%Y-%m-%d')
    if processing_date <= multi_date:
        print('OK')
    else:
        print('WARNING')

def set_last_condolidated_date():
    sdir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    file_json = os.path.join(sdir, 'info100days.json')
    if not os.path.exists(file_json):
        msg = f'[ERROR] File: {file_json} does not exist'
        print(msg)
        return
    with open(file_json, 'r', encoding='utf8') as j:
        try:
            jdict = json.loads(j.read())
        except:
            msg = f'[ERROR] File: {file_json} is not a valid json file'
            print(msg)
            return
    jdict, msg = check_sensor_dates(jdict)
    print(msg)
    if jdict is not None:
        with open(file_json, "w", encoding='utf8') as outfile:
            json.dump(jdict, outfile, indent=1, ensure_ascii=False)


# def set_last_processing_date():
#     sdir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
#     # sdir = '/mnt/c/DATA_LUIS/OCTAC_WORK/CC0C-591-_100days/'
#     file_json = os.path.join(sdir, 'info100days.json')
#     if not os.path.exists(file_json):
#         msg = f'[ERROR] File: {file_json} does not exist'
#         print(msg)
#         return
#     with open(file_json, 'r', encoding='utf8') as j:
#         try:
#             jdict = json.loads(j.read())
#         except:
#             msg = f'[ERROR] File: {file_json} is not a valid json file'
#             print(msg)
#             return
#     if not args.processing_date:
#         msg = f'[ERROR] Processing date (-pd) is a compulsory option'
#         print(msg)
#         return
#
#     try:
#         processing_date = dt.strptime(args.processing_date, '%Y-%m-%d')
#     except:
#         msg = f'[ERROR] Date: {args.processing_date} is not valid'
#         print(msg)
#         return
#     try:
#         last_pd = dt.strptime(jdict['PROCESSED'], '%Y-%m-%d')
#     except:
#         ld = jdict['PROCESSED']
#         msg = f'[ERROR] Date: {ld} is not valid'
#         print(msg)
#         return
#
#     if processing_date > last_pd:
#         jdict['PROCESSED'] = processing_date.strftime('%Y-%m-%d')
#
#     with open(file_json, "w", encoding='utf8') as outfile:
#         json.dump(jdict, outfile, indent=1, ensure_ascii=False)


def check_mail():
    email_lines = []
    sdir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    # sdir = '/mnt/c/DATA_LUIS/OCTAC_WORK/CC0C-591-_100days/'
    file_json = os.path.join(sdir, 'info100days.json')
    if not os.path.exists(file_json):
        print(f'[ERROR] File: {file_json} does not exist')
        return
    with open(file_json, 'r', encoding='utf8') as j:
        try:
            jdict = json.loads(j.read())
        except:
            print(f'[ERROR] File: {file_json} is not a valid json file')
            return

    try:
        today = dt.now().strftime('%Y-%m-%d')
        email_lines.append('CHECK - MULTI PROCESSING 180-DAYS FOR MEDITERRANEAN AND BLACK SEA')
        email_lines.append(f'Checking date: {today}')
        email_lines.append('----------------------------------')
        email_lines.append('Last dates with available consolidated files: ')
        sensors = ['AQUA', 'VIIRS', 'VIIRSJ']
        for s in sensors:
            dt.strptime(jdict[s], '%Y-%m-%d')
            email_lines.append(f'  {s}:{jdict[s]}')
        multi_date = dt.strptime(jdict['MULTI'], '%Y-%m-%d')
        multi_date_str = multi_date.strftime('%Y-%m-%d')
        email_lines.append('')
        email_lines.append(f'  MULTI:{multi_date_str}')

        processing_date = dt.now()-timedelta(days=180)
        if args.processing_date:
            try:
                processing_date = dt.strptime(args.processing_date, '%Y-%m-%d')
                processing_date_str = processing_date.strftime('%Y-%m-%d')
            except:
                pass
        email_lines.append('----------------------------------')
        if processing_date <= multi_date:
            email_lines.append(f'Started processing for date: {processing_date_str}')
        else:
            email_lines.append(
                f'Date {processing_date_str} can not be processed. Data are available only until {multi_date_str}')



    except:
        print(f'[ERROR] Error retrieving  dates from json file: {file_json}')


    print_email_lines(email_lines)


def print_email_lines(lines):
    for line in lines:
        print(line)


if __name__ == '__main__':
    # if args.mode == 'NEXT_DAY':
    #     check_next_day()

    if args.mode == 'CHECK_DAY':
        check_date()

    if args.mode == 'SET_LAST':
        set_last_condolidated_date()

    if args.mode == 'MAIL':
        check_mail()


