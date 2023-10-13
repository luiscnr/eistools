import argparse
import os.path

from product_info import ProductInfo
from datetime import datetime as dt
from datetime import timedelta
parser = argparse.ArgumentParser(description='Upload 2DBS')
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
parser.add_argument("-pname", "--name_product", help="Product name")
parser.add_argument("-dname", "--name_dataset", help="Product name")
parser.add_argument("-sd", "--start_date", help="Start date (yyyy-mm-dd)")
parser.add_argument("-ed", "--end_date", help="Start date (yyyy-mm-dd)")
args = parser.parse_args()

def main():
    print('[INFO] Started add qi bands')
    pinfo = ProductInfo()
    product_name = args.name_product
    dataset_name = args.name_dataset
    valid = pinfo.set_dataset_info(product_name,dataset_name)
    if not valid:
        print(f'[ERROR] Dataset {product_name}/{dataset_name} is not valid')
        return
    start_date, end_date = get_dates()
    if start_date is None or end_date is None:
        return
    if args.verbose:
        print(f'[INFO] Start date: {start_date} End date: {end_date}')

    date_work = start_date
    while date_work<=end_date:
        file_orig = pinfo.get_file_path_orig(None,date_work)
        if file_orig is None:
            print(f'[WARNING] File for date {date_work} is not available. Skipping...')
            date_work = date_work + timedelta(hours=24)
            continue
        add_empty_qi_bands(file_orig)

        date_work = date_work + timedelta(hours=24)

def add_empty_qi_bands(file_in):
    if args.verbose:
        print(f'[INF0] Add qi bands for file: {file_in}')
    from netCDF4 import Dataset
    import numpy as np
    vars_no_qi = ['lat','lon','stereographic','time','x','y']
    file_out = os.path.join(os.path.dirname(file_in),f'{os.path.basename(file_in)}.tmp.nc')

    input_dataset = Dataset(file_in)
    addqi = False
    for name, variable in input_dataset.variables.items():
        if name not in vars_no_qi and not name.startswith('QI'):
            newname = f'QI_{name}'
            if newname in input_dataset.variables:
                print(f'[INFO] {newname} already exist.')
            else:
                addqi = True
    if not addqi:
        print(f'[INFO] QI bands already exists. Skipping...')
        input_dataset.close()
        return



    ncout = Dataset(file_out, 'w', format='NETCDF4')

    # copy global attributes all at once via dictionary
    ncout.setncatts(input_dataset.__dict__)

    # copy dimensions
    for name, dimension in input_dataset.dimensions.items():
        ncout.createDimension(
            name, (len(dimension) if not dimension.isunlimited() else None))

    for name, variable in input_dataset.variables.items():
        fill_value = None
        if '_FillValue' in list(variable.ncattrs()):
            fill_value = variable._FillValue

        ncout.createVariable(name, variable.datatype, variable.dimensions, fill_value=fill_value, zlib=True,
                             shuffle=True, complevel=6)

        # copy variable attributes all at once via dictionary
        ncout[name].setncatts(input_dataset[name].__dict__)


        # copy data
        if name.startswith('RRS'):
            array = np.array(input_dataset.variables[name])
            ncout[name].valid_max=1.0
            ncout[name][:] = array[:]
        else:
            ncout[name][:] = input_dataset[name][:]

        if name not in vars_no_qi:
            newname = f'QI_{name}'
            print(f'[INFO]--> Adding band: {newname}')
            varqi = ncout.createVariable(newname,variable.datatype,variable.dimensions,fill_value=fill_value,zlib=True,shuffle=True,complevel=6)
            #print(ncout[name].long_name)
            lname = f'Quality Index for  {input_dataset[name].long_name.lower()}'
            if lname.find('rrs')>0:
                lname = lname.replace('rrs','')
                lname = lname + ' nm'

            varqi.long_name = lname
            varqi.comment = f'QI=(DailyData-ClimatologyWeightedMean)/ClimatologyWeightedStandardDeviation'
            varqi.type = 'surface'
            varqi.units = '1'
            varqi.missing_value = -999.0
            varqi.valid_min = -5.0
            varqi.valid_max = 5.0


    ncout.close()
    input_dataset.close()

    os.rename(file_out,file_in)


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

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()