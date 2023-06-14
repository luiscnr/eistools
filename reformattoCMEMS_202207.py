import argparse
import os.path
import shutil
from datetime import datetime as dt
from datetime import timedelta
from product_info import ProductInfo
import subprocess
from dataset_selection import DatasetSelection

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
                    choices=['multi', 'olci', 'gapfree_multi', 'multi_climatology','cci'])
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
    print('[INFO] STARTED REFORMAT AND UPLOAD')

    ##DATASETS SELECTION
    name_products, name_datasets = get_datasets()
    n_datasets = len(name_products)
    if n_datasets == 0:
        print(f'[ERROR] No datasets selected')
        return
    if not check_datasets(name_products, name_datasets):
        return
    if args.verbose:
        print(f'[INFO] Number of selected datasets: {n_datasets}')
        for idataset in range(n_datasets):
            print(f'[INFO]  {name_products[idataset]}/{name_datasets[idataset]}')

    # DATE SELECTION
    start_date, end_date = get_dates()
    if start_date is None or end_date is None:
        return
    if args.verbose:
        print(f'[INFO] Start date: {start_date} End date: {end_date}')

    if args.check_param:
        return

    pinfo = ProductInfo()
    for idataset in range(n_datasets):
        pinfo.set_dataset_info(name_products[idataset], name_datasets[idataset])
        if args.verbose:
            print(f'[INFO] Working with dataset: {name_products[idataset]}/{name_datasets[idataset]}')
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


##DATASET SELECTION
def get_datasets():
    # name_products = []
    # name_datasets = []
    dsel = DatasetSelection(args.mode)
    if args.name_product and args.name_dataset:
        name_products = [args.name_product]
        name_datasets = [args.name_dataset]
    elif args.name_product and not args.name_dataset:
        name_products, name_datasets = dsel.get_list_product_datasets_from_product_nane(args.name_product)
    elif not args.name_product and args.name_dataset:
        name_products, name_datasets = dsel.get_list_product_datasets_from_dataset_nane(args.name_dataset)
    else:
        dsel = DatasetSelection(args.mode)
        region, sensor, dataset_type, frequency, level = get_params_selection_dataset()
        dsel.set_params(region, level, dataset_type, sensor, frequency)
        name_products, name_datasets = dsel.get_list_product_datasets_from_params()

    return name_products, name_datasets

def get_params_selection_dataset():
    region = None
    sensor = None
    dataset_type = None
    frequency = None
    level = None
    if args.region:
        region = args.region
    if args.sensor:
        sensor = args.sensor
    if args.dataset_type:
        dataset_type = args.dataset_type
    if args.frequency_product:
        frequency = args.frequency_product
    if args.level:
        level = args.level
    return region, sensor, dataset_type, frequency, level


##DATASETS CHECKING
def check_datasets(name_products, name_datasets):
    mode_check = args.mode
    if args.mode == 'MYINT':
        mode_check = 'MY'
    if args.mode == 'DT':
        mode_check = 'NRT'
    pinfo = ProductInfo()
    for idataset in range(len(name_products)):
        valid = pinfo.set_dataset_info(name_products[idataset], name_datasets[idataset])
        if not valid:
            return False
        mode_here = pinfo.dinfo['mode']
        if mode_here != mode_check:
            print(
                f'[ERROR] Dataset {name_datasets[idataset]} is {mode_here},but script was launched in mode {mode_check}')
            return False
        region, sensor, dataset_type, frequency, level = get_params_selection_dataset()
        if region is not None:
            region_here = pinfo.dinfo['region']
            if region_here != region:
                print(f'[ERROR] Dataset region is {region_here} but {region} was given in the script')
                return False
        if sensor is not None:
            sensor_here = pinfo.dinfo['sensor'].lower()
            if sensor_here != sensor:
                print(f'[ERROR] Dataset sensor is {sensor_here} but {sensor} was given in the script')
                return False
        if dataset_type is not None:
            dataset_type_here = pinfo.dinfo['dataset'].lower()
            if dataset_type_here != dataset_type:
                print(f'[ERROR] Dataset dataset_type is {dataset_type_here} but {dataset_type} was given in the script')
                return False
        if frequency is not None:
            frequency_here = pinfo.dinfo['frequency'].lower()
            if frequency_here != frequency:
                print(f'[ERROR] Dataset frequency is {frequency_here} but {frequency} was given in the script')
                return False
        if level is not None:
            level_here = pinfo.dinfo['level'].lower()
            if level_here != level:
                print(f'[ERROR] Dataset level is {level_here} but {level} was given in the script')
                return False



    return True


##DATES SELECTION
def get_dates():
    start_date = None
    end_date = None
    if not args.start_date and not args.end_date:
        print(f'[ERROR] Start date(-sd) is not given.')
        return start_date, end_date
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
        return None, None
    if end_date is None:
        print(
            f'[ERROR] End date {end_date_p} is not in the correct format. It should be YYYY-mm-dd or integer (relative days')
        return None, None
    if start_date > end_date:
        print(f'[ERROR] End date should be greater or equal than start date')
        return None, None
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


def main_deprecated():
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
            file_dest = pinfo.get_file_path_orig_reformat_name(date_work)
            if os.path.exists(file_orig) and file_dest is not None:
                if verbose:
                    print(f'[INFO] Moving reformated file {file_orig} to path reformat {file_dest}')
                shutil.copy2(file_orig,file_dest)
                os.remove(file_orig)

        date_work = date_work + timedelta(hours=24)





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
