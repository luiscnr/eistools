import argparse
from datetime import datetime as dt
from datetime import timedelta
from product_info import ProductInfo
import reformattoCMEMS_202207 as reformat
import uploadtoDBS_202207 as upload
import deleteDBS_202207 as delete
from dataset_selection import DatasetSelection

# import check_202207 as checkf

parser = argparse.ArgumentParser(description='Reformat and upload to the DBS')
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
parser.add_argument("-noupload", "--no_upload", help="No upload mode, only reformat.", action="store_true")
parser.add_argument("-noreformat", "--no_reformat", help="No reformat mode, only upload.", action="store_true")
parser.add_argument('-check', "--check_param", help="Check params mode.", action="store_true")
parser.add_argument("-m", "--mode", help="Mode.", type=str, required=True, choices=['NRT', 'DT', 'MY', 'MYINT'])
parser.add_argument("-r", "--region", help="Region.", type=str, choices=['BAL', 'MED', 'BLK', 'BS'])
parser.add_argument("-l", "--level", help="Level.", type=str, choices=['l3', 'l4'])
parser.add_argument("-d", "--dataset_type", help="Dataset.", type=str,
                    choices=['reflectance', 'plankton', 'optics', 'transp','pp'])
parser.add_argument("-s", "--sensor", help="Sensor.", type=str,
                    choices=['multi', 'olci', 'gapfree_multi', 'multi_climatology','cci'])
parser.add_argument("-sd", "--start_date", help="Start date (yyyy-mm-dd)")
parser.add_argument("-ed", "--end_date", help="Start date (yyyy-mm-dd)")
parser.add_argument("-pname", "--name_product", help="Product name")
parser.add_argument("-pfreq", "--frequency_product",
                    help="Select datasets of selected product (-pname) with this frequency", choices=['d', 'm', 'c'])
parser.add_argument("-dname", "--name_dataset", help="Product name")

args = parser.parse_args()


def make_reformat_daily(pinfo, pinfomy, start_date, end_date):
    pinfo.MODE = 'REFORMAT'
    if args.verbose:
        print('***********************************************************')
        print(f'[INFO] Deleting previous files: Started')
    pinfo.delete_list_file_path_orig(start_date, end_date, args.verbose)
    if args.verbose:
        print(f'[INFO] Deleting previous files: Completed')
        print('***********************************************************')
        print(f'[INFO] Reformatting files: Started')
    pinfomy = None
    if args.mode == 'DT':
        pinfomy = pinfo.get_pinfomy_equivalent()
    if pinfomy is not None:
        if args.verbose:
            print(f'[INFO] Using equivalent MY product: {pinfomy.product_name};dataset:{pinfomy.dataset_name}')
        pinfomy.MODE = 'REFORMAT'
        reformat.make_reformat_daily_dataset(pinfomy, start_date, end_date, args.verbose)
    else:
        reformat.make_reformat_daily_dataset(pinfo, start_date, end_date, args.verbose)
    if args.verbose:
        print(f'[INFO] Reformating files: Completed')
        print('***********************************************************')


def make_upload_daily(pinfo, pinfomy, start_date, end_date):
    if args.verbose:
        print(f'[INFO] Uploading files to DU: Started')
    delete_nrt = False
    pinfo.MODE = 'UPLOAD'
    if pinfomy is not None:
        if args.verbose:
            print(f'[INFO] Using equivalent MY product: {pinfomy.product_name};dataset:{pinfomy.dataset_name}')
        pinfomy.MODE = 'UPLOAD'
        upload.upload_daily_dataset_pinfo(pinfomy, 'MY', start_date, end_date, args.verbose)
        delete_nrt = True
        # delete nrt
        # delete.make_delete_daily_dataset(pinfo, 'NRT', start_date, end_date, args.verbose)
    else:
        upload.upload_daily_dataset_pinfo(pinfo, args.mode, start_date, end_date, args.verbose)

    #delete nrt if neeed
    if delete_nrt:
        start_date_nrt = start_date - timedelta(days=1)
        end_date_nrt = end_date - timedelta(days=1)
        upload.delete_nrt_daily_dataset(pinfo,start_date_nrt,end_date_nrt,args.verbose)

    if args.verbose:
        print(f'[INFO] Uploading files to DU: Completed')
        print('***********************************************************')
        print(f'[INFO] Deleting files: Started')

    pinfo.MODE = 'REFORMAT'
    pinfo.delete_list_file_path_orig(start_date, end_date, args.verbose)
    if args.verbose:
        print(f'[INFO] Deleting files: Completed')


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
            pinfomy = None
            if args.mode == 'DT':
                pinfomy = pinfo.get_pinfomy_equivalent()
            if not args.no_reformat:
                make_reformat_daily(pinfo, pinfomy, start_date, end_date)
            if not args.no_upload:
                make_upload_daily(pinfo, pinfomy, start_date, end_date)

        if pinfo.dinfo['frequency'] == 'm':
            pinfo.MODE = 'REFORMAT'
            if args.verbose:
                print('***********************************************************')
                print(f'[INFO] Deleting previous files: Started')
            pinfo.delete_list_file_path_orig_monthly(start_date, end_date, args.verbose)
            if args.verbose:
                print(f'[INFO] Deleting previous files: Completed')
                print('***********************************************************')
                print(f'[INFO] Reformatting files: Started')
            pinfomy = None
            if args.mode == 'DT':
                pinfomy = pinfo.get_pinfomy_equivalent()
            if pinfomy is not None:
                if args.verbose:
                    print(f'[INFO] Using equivalent MY product: {pinfomy.product_name};dataset:{pinfomy.dataset_name}')
                pinfomy.MODE = 'REFORMAT'
                reformat.make_reformat_monthly_dataset(pinfomy, start_date, end_date, args.verbose)
            else:
                reformat.make_reformat_monthly_dataset(pinfo, start_date, end_date, args.verbose)
            if args.verbose:
                print(f'[INFO] Reformating files: Completed')
                print('***********************************************************')
                print(f'[INFO] Uploading files to DU: Started')
            pinfo.MODE = 'UPLOAD'
            if pinfomy is not None:
                if args.verbose:
                    print(f'[INFO] Using equivalent MY product: {pinfomy.product_name};dataset:{pinfomy.dataset_name}')
                pinfomy.MODE = 'UPLOAD'
                upload.upload_monthly_dataset_pinfo(pinfomy, 'MY', start_date, end_date, args.verbose)
                # delete nrt
                delete.make_delete_monthly_dataset(pinfo, 'NRT', start_date, end_date, args.verbose)
            else:
                upload.upload_monthly_dataset_pinfo(pinfo, args.mode, start_date, end_date, args.verbose)
            if args.verbose:
                print(f'[INFO] Uploading files to DU: Completed')
                print('***********************************************************')
                print(f'[INFO] Deleting files: Started')
            pinfo.MODE = 'REFORMAT'
            pinfo.delete_list_file_path_orig_monthly(start_date, end_date, args.verbose)
            if args.verbose:
                print(f'[INFO] Deleting files: Completed')


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
            if region_here=='BLK' and region=='BS':
                region = 'BLK'
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
    print('DEPRECATED')
    # pinfo.MODE = 'REFORMAT'
    # if args.verbose:
    #     print('***********************************************************')
    #     print(f'[INFO] Deleting previous files: Started')
    # pinfo.delete_list_file_path_orig(start_date, end_date, args.verbose)
    # if args.verbose:
    #     print(f'[INFO] Deleting previous files: Completed')
    #     print('***********************************************************')
    #     print(f'[INFO] Reformatting files: Started')
    # pinfomy = None
    # if args.mode == 'DT':
    #     pinfomy = pinfo.get_pinfomy_equivalent()
    # if pinfomy is not None:
    #     if args.verbose:
    #         print(f'[INFO] Using equivalent MY product: {pinfomy.product_name};dataset:{pinfomy.dataset_name}')
    #     pinfomy.MODE = 'REFORMAT'
    #     reformat.make_reformat_daily_dataset(pinfomy, start_date, end_date, args.verbose)
    # else:
    #     reformat.make_reformat_daily_dataset(pinfo, start_date, end_date, args.verbose)
    # if args.verbose:
    #     print(f'[INFO] Reformating files: Completed')
    #     print('***********************************************************')
    #     print(f'[INFO] Uploading files to DU: Started')
    # pinfo.MODE = 'UPLOAD'
    # if pinfomy is not None:
    #     if args.verbose:
    #         print(f'[INFO] Using equivalent MY product: {pinfomy.product_name};dataset:{pinfomy.dataset_name}')
    #     pinfomy.MODE = 'UPLOAD'
    #     upload.upload_daily_dataset_pinfo(pinfomy, 'MY', start_date, end_date, args.verbose)
    #     #delete nrt
    #     delete.make_delete_daily_dataset(pinfo,'NRT',start_date,end_date,args.verbose)
    # else:
    #     upload.upload_daily_dataset_pinfo(pinfo, args.mode, start_date, end_date, args.verbose)
    # if args.verbose:
    #     print(f'[INFO] Uploading files to DU: Completed')
    #     print('***********************************************************')
    #     print(f'[INFO] Deleting files: Started')
    # pinfo.MODE = 'REFORMAT'
    # pinfo.delete_list_file_path_orig(start_date, end_date, args.verbose)
    # if args.verbose:
    #     print(f'[INFO] Deleting files: Completed')

    # if args.region:
    #     region = args.region
    #     if region == 'BS':
    #         region = 'BLK'
    #
    # if args.mode and args.region and args.level:
    #     # pinfo.set_dataset_info_fromparam(args.mode, args.region, args.level, args.dataset_type, args.sensor)
    #     dataset_type = None
    #     sensor = None
    #     mode_search = args.mode
    #     frequency = None
    #     if args.mode=='DT':
    #         mode_search = 'NRT'
    #     if args.dataset_type:
    #         dataset_type = args.dataset_type
    #     if args.sensor:
    #         sensor = args.sensor
    #     if args.frequency_product:
    #         frequency = args.frequency_product
    #     name_products, name_datasets = pinfo.get_list_datasets_params(mode_search, region, args.level, dataset_type,
    #                                                                   sensor,frequency)
    #     n_datasets = len(name_products)
    #
    # elif args.name_product and args.name_dataset:
    #     name_products.append(args.name_product)
    #     name_datasets.append(args.name_dataset)
    #     n_datasets = 1
    #     # pinfo.set_dataset_info(args.name_product, args.name_dataset)
    # elif args.name_product and not args.name_dataset:
    #     name_products, name_datasets = pinfo.get_list_datasets(args.name_product)
    #     n_datasets = len(name_products)
    #     # pinfo.set_product_info(args.name_product)
    #     do_multiple_datasets = True

    # reformat.make_reformat_monthly_dataset(pinfo, start_date, end_date, args.verbose)
    # file_list = pinfo.get_list_file_path_orig_monthly(start_date, end_date)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
