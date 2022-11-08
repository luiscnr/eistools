import argparse
import os.path
import shutil
from datetime import datetime as dt
from datetime import timedelta
from product_info import ProductInfo
import subprocess

parser = argparse.ArgumentParser(description='Reformat and upload 2DBS')
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
parser.add_argument('-check', "--check_param", help="Check params mode.", action="store_true")
parser.add_argument("-noupload","--no_upload", help="No upload mode, only reformat.",action="store_true")
parser.add_argument("-noreformat","--no_reformat",help="No reformat mode, only upload.",action="store_true")
parser.add_argument("-m", "--mode", help="Mode.", type=str, choices=['NRT', 'DT', 'MY'])
parser.add_argument("-r", "--region", help="Region.", type=str, choices=['BAL', 'MED', 'BLK','BS'])
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
parser.add_argument("-csize", "--size_file", help="Output file with size information. Files are deleted ")
parser.add_argument("-csizeopt", "--size_opt", help="Options to check file size without reformat",
                    choices=['olci_rrs', 'olci_plankton', 'olci_transp', 'olci_m_rrs', 'olci_m_plankton',
                             'olci_m_transp'])

args = parser.parse_args()


def main():
    print('STARTED REFORMAT')
    pinfo = ProductInfo()
    do_multiple_datasets = False

    if args.region:
        region = args.region
        if region == 'BS':
            region = 'BLK'

    if args.mode and args.region and args.level and args.dataset_type and args.sensor:
        pinfo.set_dataset_info_fromparam(args.mode, region, args.level, args.dataset_type, args.sensor)
    elif args.name_product and args.name_dataset:
        pinfo.set_dataset_info(args.name_product, args.name_dataset)
    elif args.name_product and not args.name_dataset:
        pinfo.set_product_info(args.name_product)
        do_multiple_datasets = True

    if args.start_date and args.end_date and not do_multiple_datasets:
        start_date = dt.strptime(args.start_date, '%Y-%m-%d')
        end_date = dt.strptime(args.end_date, '%Y-%m-%d')
        if pinfo.dinfo['frequency'] == 'd':
            if not args.size_opt:
                make_reformat_daily_dataset(pinfo, start_date, end_date, args.verbose)
            if args.size_file:
                file_size = args.size_file
                opt = None
                if args.size_opt:
                    opt = args.size_opt
                if args.verbose:
                    print(f'[INFO] Checking size...')
                df = pinfo.check_size_file_orig(start_date, end_date, opt, args.verbose)
                df.to_csv(file_size, sep=';')
                if not args.size_opt:
                    if args.verbose:
                        print(f'[INFO] Deleting...')
                    pinfo.delete_list_file_path_orig(start_date, end_date, args.verbose)

        if pinfo.dinfo['frequency'] == 'm':
            if not args.size_opt:
                make_reformat_monthly_dataset(pinfo, start_date, end_date)
            if args.size_file:
                file_size = args.size_file
                opt = None
                if args.size_opt:
                    opt = args.size_opt
                if args.verbose:
                    print(f'[INFO] Checking size...')
                if opt is not None:
                    df = pinfo.check_size_file_orig_monthly(start_date, end_date, opt, args.verbose)
                # else: NO IMPPLMENTED
                #     df = pinfo.check_size_file_orig(start_date,end_date,opt,args.verbose)
                df.to_csv(file_size, sep=';')
                if not args.size_opt:
                    if args.verbose:
                        print(f'[INFO] Deleting...')
                    pinfo.delete_list_file_path_orig(start_date, end_date, args.verbose)

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
                make_reformat_daily_dataset(pinfo_here, start_date, end_date, args.verbose)

            if pinfo_here.dinfo['frequency'] == 'm' and make:
                make_reformat_monthly_dataset(pinfo_here, start_date, end_date, args.verbose)


def make_reformat_daily_dataset(pinfo, start_date, end_date, verbose):
    date_work = start_date
    while date_work <= end_date:
        if verbose:
            print('----------------------------------------------------')
            print(f'[INFO] Reformating file for date: {date_work}')
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
            print(f'[CMD ERROR]{err}')

        preformat = pinfo.check_path_reformat()
        if preformat is not None:
            file_orig = pinfo.get_file_path_orig(None,date_work)
            file_dest = pinfo.get_file_path_orig_reformat(date_work)
            if os.path.exists(file_orig):
                if verbose:
                    print(f'[INFO] Moving reformated file to path reformat {preformat}')
                shutil.copy2(file_orig,file_dest)
                os.remove(file_orig)

            # if err.decode("utf-8").find('ncks: unrecognized option') >= 0:
            #     pass
            # else:
            #     print(f'[CMD ERROR]{err}')


def make_reformat_monthly_dataset(pinfo, start_date, end_date, verbose):
    year_ini = start_date.year
    year_fin = end_date.year
    for year in range(year_ini, year_fin + 1):
        mini = 1
        mfin = 12
        if year == start_date.year:
            mini = start_date.month
        if year == end_date.year:
            mfin = end_date.month
        for month in range(mini, mfin + 1):
            date_here = dt(year, month, 15)
            if verbose:
                print('----------------------------------------------------')
                print(f'[INFO] Reformating file for year: {year} month: {month}')
            cmd = pinfo.get_reformat_cmd(date_here)
            if verbose:
                print(f'[INFO] CMD: {cmd}')
            prog = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE)
            out, err = prog.communicate()
            if out:
                outstr = out.decode(("utf-8"))
                ierror = outstr.find('ERROR')
                if ierror >= 0:
                    print(f'[CMD ERROR] {outstr[ierror:]}')
            if err:
                print(f'[CMD ERROR]{err}')


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
