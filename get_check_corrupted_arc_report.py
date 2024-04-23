import os
import argparse
import warnings
from datetime import datetime as dt
from datetime import timedelta
import zipfile as zp
from netCDF4 import Dataset

warnings.filterwarnings("ignore")
parser = argparse.ArgumentParser(description='Daily reports')
parser.add_argument("-m", "--mode", help="Mode.", type=str, required=True, choices=['NRT', 'DT'])
parser.add_argument("-sd", "--date", help="Date.")
args = parser.parse_args()

def main():
    date = None
    if args.date:
        date = get_date_from_param(args.date)
    if date is None:
        print(
            f'[ERROR] Date {args.date} is not in the correct format. It should be YYYY-mm-dd or integer (relative days')
        return
    dir_sources = '/store/COP2-OC-TAC/arc/sources/'
    dir_sources_date = os.path.join(dir_sources,date.strftime('%Y%m%d'))
    dir_unzip = '/store/COP2-OC-TAC/arc/unzip'
    timeliness = '_NR_'
    if args.mode=='DT':
        timeliness = '_NT_'
    print(f'---------------------------------------------------------------------------------------------------------')
    print(f'Starting checking of corrupted files for {date.strftime("%Y-%m-%d")}')
    print(f'Mode: {args.mode}')
    print(f'Timeliness: {timeliness}')
    print('')
    if not os.path.exists(dir_sources_date):
        print(f'Sources date folder: {dir_sources_date} does not exist. Checking was not performed')
        return

    ngranules = 0
    granules_corrupted = []

    for name in os.listdir(dir_sources_date):
        if not name.startswith('S3'):
            continue
        if name.endswith('.txt'):
            continue
        if name.find(timeliness)<0:
            continue
        path_prod = os.path.join(dir_sources_date,name)
        iszipped = False
        if os.path.isdir(path_prod):
            path_prod_u = path_prod
        elif zp.is_zipfile(path_prod):
            iszipped = True
            path_prod_u = path_prod.split('/')[-1][0:-4]
            path_prod_u = os.path.join(dir_unzip, path_prod_u)
            with zp.ZipFile(path_prod, 'r') as zprod:
                zprod.extractall(path=dir_unzip)
        else:
            continue
        if not os.path.isdir(path_prod_u):
            continue

        if path_prod_u.endswith('.SEN3'):
            output_name = path_prod_u.split('/')[-1][0:-5]
        else:
            output_name = path_prod_u.split('/')[-1]
        ngranules = ngranules +  1
        b = check_granule(path_prod_u)
        if not b:
            granules_corrupted.append(output_name)

        if iszipped:
            for fn in os.listdir(path_prod_u):
                os.remove(os.path.join(path_prod_u,fn))

    ncorrupted = len(granules_corrupted)
    print(f'Number of downloaded granules: {ngranules}')
    print(f'Number of corrupted granuels: {ncorrupted}')
    if ncorrupted>0:
        print(f'Corrupted granules:')
        for granule in granules_corrupted:
            print(granule)


def check_granule(path_source):
    nfiles = 0
    valid = True
    for name in os.listdir(path_source):
        if name!='iop_lsd.nc':
            nfiles = nfiles + 1
        if name.endswith('.nc'):
            try:
                dataset = Dataset(os.path.join(path_source,name))
                dataset.close()
            except:
                valid = False
                break
    if nfiles<32:
        valid = False
    return valid


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