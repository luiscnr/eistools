import argparse
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


def main():
    print('[INFO] STARTED REFORMAT AND UPLOAD')

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


# IMPORTANT: PINFO MUST CONTAIN PRODUCT NAME AND DATASET NAME
def check_dailyfile_du(mode, pinfo, date, verbose):
    if verbose:
        print('[INFO] Checking  in the DU: started...')
    ftpcheck = FTPCheck(mode)
    y = date.year
    m = date.month
    rpath = ftpcheck.go_month_subdir(pinfo, y, m)
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
        self.ftpdu.cwd(rpath)

    def go_month_subdir(self, pinfo, year, month):
        dateref = dt(year, month, 15)
        rpath = os.path.join('/Core', pinfo.product_name, pinfo.dataset_name, dateref.strftime('%Y'),
                             dateref.strftime('%m'))

        self.go_subdir(rpath)
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
