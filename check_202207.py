import argparse
from datetime import datetime as dt
from product_info import ProductInfo
import calendar
import pandas as pd
import os
from configparser import RawConfigParser
from ftplib import FTP
import json
import reformattoCMEMS_202207 as reformat
import uploadtoDBS_202207 as upload

parser = argparse.ArgumentParser(description='Upload 2DBS')
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


def main():
    pinfo = ProductInfo()
    do_multiple_datasets = False
    if args.mode and args.region and args.level and args.dataset_type and args.sensor:
        pinfo.set_dataset_info_fromparam(args.mode, args.region, args.level, args.dataset_type, args.sensor)
    elif args.name_product and args.name_dataset:
        pinfo.set_dataset_info(args.name_product, args.name_dataset)
    elif args.name_product and not args.name_dataset:
        pinfo.set_product_info(args.name_product)
        do_multiple_datasets = True

    if args.start_date and args.end_date and not do_multiple_datasets:
        start_date = dt.strptime(args.start_date, '%Y-%m-%d')
        end_date = dt.strptime(args.end_date, '%Y-%m-%d')
        if pinfo.dinfo['frequency'] == 'd':
            check_daily(pinfo, start_date, end_date, args.verbose)


def check_daily(pinfo, start_date, end_date, verbose):
    if verbose:
        print('[INFO] Starting checking...')

    # if start_date.year == end_date.year:
    #     nmonths = (end_date.month - start_date.month) + 1
    # else:
    #     nmonths = ((12 - start_date.month) + 1) + end_date.month + (12 * ((end_date.year - start_date.year) - 1))

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
            date = dt.strptime(fname[0:8],'%Y%m%d')
            if sdate_nan is None and edate_nan is None:
                sdate_nan = date
                edate_nan = date
            else:
                if date<sdate_nan:
                    sdate_nan = date
                if date>edate_nan:
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
        if mode == 'MY':
            du_server = "my-dev.cmems-du.eu"
        elif mode == 'NRT' or mode == 'DT':
            du_server = "nrt-dev.cmems-du.eu"
        du_uname = credentials.get('normal', 'uname')
        du_passwd = credentials.get('normal', 'passwd')
        self.ftpdu = FTP(du_server, du_uname, du_passwd)

    def go_subdir(self, rpath):
        # print('Changing directory to: ', rpath)
        self.ftpdu.cwd(rpath)

    def go_month_subdir(self, pinfo, year, month):
        dateref = dt(year, month, 15)
        rpath = os.path.join('/Core', pinfo.product_name, pinfo.dataset_name, dateref.strftime('%Y'),
                             dateref.strftime('%m'))

        self.go_subdir(rpath)
        return rpath

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
