import argparse
import os.path

from product_info import ProductInfo
from datetime import datetime as dt
from datetime import timedelta
import warnings
warnings.filterwarnings("ignore")

parser = argparse.ArgumentParser(description='Upload 2DBS')
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
parser.add_argument("-pname", "--name_product", help="Product name")
parser.add_argument("-dname", "--name_dataset", help="Product name")
parser.add_argument("-sd", "--start_date", help="Start date (yyyy-mm-dd)")
parser.add_argument("-ed", "--end_date", help="Start date (yyyy-mm-dd)")
parser.add_argument("-var", "--variable", help="Variable to compute the QI")
parser.add_argument("-pclima", "--path_climatology", help="Path to climatology files")
args = parser.parse_args()


def main():
    print('[INFO] Started add qi bands')
    pinfo = ProductInfo()
    product_name = args.name_product
    dataset_name = args.name_dataset
    valid = pinfo.set_dataset_info(product_name, dataset_name)
    if not valid:
        print(f'[ERROR] Dataset {product_name}/{dataset_name} is not valid')
        return
    start_date, end_date = get_dates()
    if start_date is None or end_date is None:
        return
    if args.verbose:
        print(f'[INFO] Start date: {start_date} End date: {end_date}')

    date_work = start_date
    while date_work <= end_date:
        file_orig = pinfo.get_file_path_orig(None, date_work)
        if file_orig is None:
            print(f'[WARNING] File for date {date_work} is not available. Skipping...')
            date_work = date_work + timedelta(hours=24)
            continue
        add_empty_qi_bands(file_orig)
        if args.variable and args.path_climatology:
            if os.path.isdir(args.path_climatology):
                file_clima = get_file_climatology(args.path_climatology, args.variable, date_work)
                if file_clima is None:
                    print(
                        f'[ERROR] Climatology file for date {date_work} and variable {args.variable} is not available. Skipping')
                    continue

                compute_climatology(file_orig,file_clima,args.variable)


            else:
                print(f'[ERROR] Climatology path {args.path_climatology} is not correct. Skipping')
        date_work = date_work + timedelta(hours=24)

def compute_climatology(file_orig,file_clima,variable):
    from netCDF4 import Dataset
    import numpy as np
    if args.verbose:
        print(f'[INFO] Computing climatology for variable: {variable}')
    dclima = Dataset(file_clima)
    central = np.array(dclima.variables['MEDIAN'])
    dispersion = np.array(dclima.variables['N_STD'])
    dclima.close()
    dout = Dataset(file_orig)
    input_array = np.array(dout.variables[variable.upper()])
    dout.close()
    if variable.upper()=='CHL' or variable.upper()=='KD490':
        input_array[input_array!=-999] = np.log10(input_array[input_array!=-999])
    output_array = np.zeros((input_array.shape[1],input_array.shape[2]))
    output_array[:,:] = input_array[0,:,:]
    output_array[output_array!=-999] = (output_array[output_array!=-999]-central[output_array!=-999])/dispersion[output_array!=-999]
    if args.verbose:
        print(f'[INFO] Climatology median: {np.median(output_array[output_array!=-999])}')

    add_climatology_data(file_orig,output_array,variable)

def add_climatology_data(file_in,output_data,variable):

    if args.verbose:
        print(f'[INFO] Adding qi data to file: {file_in} Variable: {variable}')

    from netCDF4 import Dataset
    qivar = f'QI_{variable.upper()}'
    file_out = os.path.join(os.path.dirname(file_in), f'{os.path.basename(file_in)}.tmp.nc')

    input_dataset = Dataset(file_in)

    ncout = Dataset(file_out, 'w', format='NETCDF4')

    # copy global attributes all at once via dictionary
    ncout.setncatts(input_dataset.__dict__)

    # copy dimensions
    for name, dimension in input_dataset.dimensions.items():
        ncout.createDimension(
            name, (len(dimension) if not dimension.isunlimited() else None))

    # variables
    for name, variable in input_dataset.variables.items():
        fill_value = None
        if '_FillValue' in list(variable.ncattrs()):
            fill_value = variable._FillValue

        ncout.createVariable(name, variable.datatype, variable.dimensions, fill_value=fill_value, zlib=True,
                             shuffle=True, complevel=6)

        # copy variable attributes all at once via dictionary
        ncout[name].setncatts(input_dataset[name].__dict__)

        # copy data
        if name==qivar:
            ncout[name][0,:,:] = output_data[:,:]
        else:
            ncout[name][:] = input_dataset[name][:]

    ncout.close()
    input_dataset.close()

    os.rename(file_out, file_in)

    if args.verbose:
        print(f'[INFO] Adding qi data completed')

def get_file_climatology(path, variable, date_work):
    if date_work.month==2 and date_work.day==29:
        date_work = date_work.replace(day=28)
    ddmm = date_work.strftime('%m%d')
    if variable.lower() == 'kd490':
        variable = 'transp'
    fname = f'1998{ddmm}_2022{ddmm}_{variable.lower()}_arc_multi_clima.nc'

    file_clima = os.path.join(path, fname)
    if os.path.isfile(file_clima):
        return file_clima
    else:
        return None


def add_empty_qi_bands(file_in):
    if args.verbose:
        print(f'[INF0] Add qi bands for file: {file_in}')
    from netCDF4 import Dataset
    import numpy as np
    vars_no_qi = ['lat', 'lon', 'stereographic', 'time', 'x', 'y']
    file_out = os.path.join(os.path.dirname(file_in), f'{os.path.basename(file_in)}.tmp.nc')

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
            ncout[name].valid_max = 1.0
            ncout[name][:] = array[:]
        else:
            ncout[name][:] = input_dataset[name][:]

        if name not in vars_no_qi:
            newname = f'QI_{name}'
            print(f'[INFO]--> Adding band: {newname}')
            varqi = ncout.createVariable(newname, variable.datatype, variable.dimensions, fill_value=fill_value,
                                         zlib=True, shuffle=True, complevel=6)
            # print(ncout[name].long_name)
            lname = f'Quality Index for  {input_dataset[name].long_name.lower()}'
            if lname.find('rrs') > 0:
                lname = lname.replace('rrs', '')
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

    os.rename(file_out, file_in)


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
