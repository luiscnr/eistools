import argparse
from datetime import datetime as dt
from datetime import timedelta
from product_info import ProductInfo
import subprocess

parser = argparse.ArgumentParser(description='Upload 2DBS')
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
parser.add_argument("-m", "--mode", help="Mode.", type=str, choices=['NRT', 'DT', 'MY'])
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
    print('STARTED')
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
            make_reformat_daily_dataset(pinfo, start_date, end_date)
        if pinfo.dinfo['frequency'] == 'm':
            make_reformat_monthly_dataset(pinfo, start_date, end_date)

    if args.start_date and args.end_date and do_multiple_datasets:

        start_date = dt.strptime(args.start_date, '%Y-%m-%d')
        end_date = dt.strptime(args.end_date, '%Y-%m-%d')

        for dname in pinfo.pinfo:
            pinfo_here = ProductInfo()
            pinfo_here.set_dataset_info(args.name_product, dname)
            make = True
            if args.frequency_product and args.frequency_product != pinfo_here.dinfo['frequency']:
                make = False
            if pinfo_here.dinfo['frequency'] == 'd' and make:
                make_reformat_daily_dataset(pinfo_here, start_date, end_date,args.verbose)
            if pinfo_here.dinfo['frequency'] == 'm' and make:
                make_reformat_monthly_dataset(pinfo_here, start_date, end_date, args.verbose)


def make_reformat_daily_dataset(pinfo, start_date, end_date,verbose):
    date_work = start_date
    while date_work <= end_date:
        cmd = pinfo.get_reformat_cmd(date_work)
        if cmd is None:
            date_work = date_work + timedelta(hours=24)
            continue
        if verbose:
            print(f'[INFO] CMD: {cmd}')
        date_work = date_work + timedelta(hours=24)
        prog = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        out, err = prog.communicate()
        if out:
            outstr = out.decode(("utf-8"))
            ierror = outstr.find('ERROR')
            if ierror >= 0:
                print(f'[CMD ERROR] {outstr[ierror:]}')
        if err:
            if err.decode("utf-8").find('ncks: unrecognized option') >= 0:
                pass
            else:
                print(f'[CMD ERROR]{err}')


def make_reformat_monthly_dataset(pinfo, start_date, end_date):
    year_ini = start_date.year
    month_ini = start_date.month
    year_fin = end_date.year
    month_fin = end_date.month
    for year in range(year_ini, year_fin + 1):
        for month in range(month_ini, month_fin + 1):
            date_here = dt(year, month, 15)
            cmd = pinfo.get_reformat_cmd(date_here)
            print(f'CMD: {cmd}')
            prog = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE)
            out, err = prog.communicate()
            if err:
                print(f'[ERROR]{err}')


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
