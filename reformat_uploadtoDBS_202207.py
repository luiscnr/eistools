import argparse
from datetime import datetime as dt
from datetime import timedelta
from product_info import ProductInfo
import reformattoCMEMS_202207 as reformat
import uploadtoDBS_202207 as upload

import check_202207 as checkf

parser = argparse.ArgumentParser(description='Reformat and upload 2DBS')
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
#parser.add_argument('-check', "--check_param", help="Check params mode.", action="store_true")
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

    ##DATASETS SELECTION
    pinfo = ProductInfo()
    name_products = []
    name_datasets = []
    n_datasets = 0
    do_multiple_datasets = False


    if args.mode and args.region and args.level:
        # pinfo.set_dataset_info_fromparam(args.mode, args.region, args.level, args.dataset_type, args.sensor)
        dataset_type = None
        sensor = None
        mode_search = args.mode
        if args.mode=='DT':
            mode_search = 'NRT'
        if args.dataset_type:
            dataset_type = args.dataset_type
        if args.sensor:
            sensor = args.sensor
        name_products, name_datasets = pinfo.get_list_datasets_params(mode_search, args.region, args.level, dataset_type,
                                                                      sensor)
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

    # if args.check_param:
    #     return

    # if args.start_date and args.end_date and not do_multiple_datasets:
    for idataset in range(n_datasets):
        # start_date = dt.strptime(args.start_date, '%Y-%m-%d')
        # end_date = dt.strptime(args.end_date, '%Y-%m-%d')
        pinfo.set_dataset_info(name_products[idataset], name_datasets[idataset])
        if args.verbose:
            print(f'[INFO] Working with dataset: {name_products[idataset]}/{name_datasets[idataset]}')
        if pinfo.dinfo['frequency'] == 'd':
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
                print(f'[INFO] Uploading files to DU: Started')
            pinfo.MODE = 'UPLOAD'
            if pinfomy is not None:
                if args.verbose:
                    print(f'[INFO] Using equivalent MY product: {pinfomy.product_name};dataset:{pinfomy.dataset_name}')
                pinfomy.MODE = 'UPLOAD'
                upload.upload_daily_dataset_pinfo(pinfomy, 'MY', start_date, end_date, args.verbose)
            else:
                upload.upload_daily_dataset_pinfo(pinfo, args.mode, start_date, end_date, args.verbose)
            if args.verbose:
                print(f'[INFO] Uploading files to DU: Completed')
                print('***********************************************************')
                print(f'[INFO] Deleting files: Started')
            pinfo.MODE = 'REFORMAT'
            pinfo.delete_list_file_path_orig(start_date, end_date, args.verbose)
            if args.verbose:
                print(f'[INFO] Deleting files: Completed')

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
            reformat.make_reformat_monthly_dataset(pinfo, start_date, end_date, args.verbose)
            if args.verbose:
                print(f'[INFO] Reformating files: Completed')
                print('***********************************************************')
                print(f'[INFO] Uploading files to DU: Started')
            pinfo.MODE = 'UPLOAD'
            upload.upload_monthly_dataset_pinfo(pinfo, args.mode, start_date, end_date, args.verbose)
            if args.verbose:
                print(f'[INFO] Uploading files to DU: Completed')
                print('***********************************************************')
                print(f'[INFO] Deleting files: Started')
            pinfo.MODE = 'REFORMAT'
            pinfo.delete_list_file_path_orig_monthly(start_date, end_date, args.verbose)
            if args.verbose:
                print(f'[INFO] Deleting files: Completed')

            # reformat.make_reformat_monthly_dataset(pinfo, start_date, end_date, args.verbose)
            # file_list = pinfo.get_list_file_path_orig_monthly(start_date, end_date)


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
