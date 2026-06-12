import argparse

from dataset_selection import DatasetSelection
from datetime import datetime as dt
from datetime import timedelta
from product_info import ProductInfo
from reformatCMEMS import ReformatCMEMS
from uploadMDS import UploadMDS


parser = argparse.ArgumentParser(description='Reformat and upload to the MDS')
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
parser.add_argument("-version", "--reformat_version", help="Reformat version.", choices=['202207', '202411'],
                    default='202411')
parser.add_argument("-only_upload", "--make_only_upload", help="Only upload mode.", action="store_true")
parser.add_argument("-only_reformat", "--make_only_reformat", help="Only reformat mode.", action="store_true")
parser.add_argument("-use_sh", "--use_sh_reformat", help="Create sh file for reformat.", action="store_true")
parser.add_argument("-test_reformat", "--make_test_reformat", help="Reformat script is created but not launched. To create a sh script instead of the slurm default, use -use_sh(--use_sh_reformat) option", action="store_true")
parser.add_argument('-check', "--check_param", help="Check params mode.", action="store_true")
parser.add_argument('-select_choices', "--check_select_choices", help="Check choices for the arguments for dataset selection.", action="store_true")
parser.add_argument("-m", "--mode", help="Mode.", type=str, required=True, choices=['NRT', 'DT', 'MY'])
parser.add_argument("-r", "--region", help="Dataset selection by region.", type=str)
parser.add_argument("-l", "--level", help="Dataset selection by level.", type=str)
parser.add_argument("-d", "--dataset_type",
                    help="Dataset selection by dataset type.",
                    type=str)
parser.add_argument("-s", "--sensor", help="Dataset selection by sensor.",
                    type=str)
parser.add_argument("-fr", "--frequency", help="Dataset selection by frequency.")
parser.add_argument("-user", "--user_value", help="Dataset selection by user value. ")
parser.add_argument("-sd", "--start_date", help="Start date (yyyy-mm-dd)")
parser.add_argument("-ed", "--end_date", help="Start date (yyyy-mm-dd)")
parser.add_argument("-pname", "--name_product", help="Product name")
parser.add_argument("-dname", "--name_dataset", help="Dataset name")
parser.add_argument("-pfolder", "--product_info_folder", help="Product info folder (defaults is ../PRODUCT_INFO)")

args = parser.parse_args()




def get_params_selection_dataset(choices):
    args_dict = vars(args)
    if 'dataset_type' in args_dict.keys():
        args_dict['dataset'] = args_dict.pop('dataset_type')

    params = {
        'region': {
            'values': None,
            'potential_values': [x.upper() for x in choices['region']] if 'region' in choices else None
        },
        'level': {
            'values': None,
            'potential_values': [x.upper() for x in choices['level']] if 'level' in choices else None
        },
        'sensor': {
            'values': None,
            'potential_values': [x.upper() for x in choices['sensor']] if 'sensor' in choices else None
        },
        'dataset': {
            'values': None,
            'potential_values': [x.upper() for x in choices['dataset']] if 'dataset' in choices else None
        },
        'frequency': {
            'values': None,
            'potential_values': [x.upper() for x in choices['frequency']] if 'frequency' in choices else None
        },
        'user_value': {
            'values': [x.upper() for x in choices['user_value']] if 'user_value' in choices else None
        }

    }

    for param in params:
        if args_dict[param] is not None:
            vals = [x.strip().upper() for x in args_dict[param].split(',')]
            for val in vals:
                if vals.count(val) > 1:
                    print(f'[ERROR] Value {val} is repeated for argument {param}')
                    return None
            if param == 'region' and 'BLK' in vals and 'BS' in vals:
                print(f'[ERROR] BLK and BS refers to the Black Sea region, please use only one')
                return None
            if param == 'region' and 'BS' in vals:
                vals[vals.index('BS')] = 'BLK'

            params[param]['values'] = vals
            if params[param]['potential_values'] is None:
                continue
            for val in vals:
                if val not in params[param]['potential_values']:
                    print(
                        f'[ERROR] {val} is not a correct option for the {param} argument. Please use: {params[param]["potential_values"]}')
                    return None
    # region = None
    # sensor = None
    # dataset_type = None
    # frequency = None
    # level = None
    # if args.region:
    #     region = args.region
    # if args.sensor:
    #     sensor = args.sensor
    # if args.dataset_type:
    #     dataset_type = args.dataset_type
    # if args.frequency_product:
    #     frequency = args.frequency_product
    # if args.level:
    #     level = args.level
    return params


def get_datasets():
    dsel = DatasetSelection(args.mode, args.product_info_folder)
    if args.name_product and args.name_dataset:
        name_products = [args.name_product]
        name_datasets = [args.name_dataset]
    elif args.name_product and not args.name_dataset:
        name_products, name_datasets = dsel.get_list_product_datasets_from_product_nane(args.name_product)
    elif not args.name_product and args.name_dataset:
        name_products, name_datasets = dsel.get_list_product_datasets_from_dataset_nane(args.name_dataset)
    else:
        choices = dsel.get_potential_choices()

        selection_params = get_params_selection_dataset(choices)
        if selection_params is None:
            return [None]*2

        dsel.set_params_from_dict(selection_params)
        name_products, name_datasets = dsel.get_list_product_datasets_from_params()

    return name_products, name_datasets


##DATASETS CHECKING
def check_datasets(name_products, name_datasets):
    mode_check = 'NRT' if args.mode == 'DT' else args.mode
    pinfo = ProductInfo()
    if args.product_info_folder:
        pinfo.path2info = args.product_info_folder

    dsel = DatasetSelection(mode_check, args.product_info_folder)
    choices = dsel.get_potential_choices()
    params_dict = get_params_selection_dataset(choices)

    for idataset in range(len(name_products)):
        valid = pinfo.set_dataset_info(name_products[idataset], name_datasets[idataset])
        if not valid:
            print(f'[ERROR] {name_products[idataset]}/{name_datasets[idataset]} is not a valid dataset.')
            return False
        mode_here = pinfo.dinfo['mode']
        if mode_here != mode_check:
            print(
                f'[ERROR] Dataset {name_datasets[idataset]} is {mode_here},but script was launched in mode {mode_check} ({args.mode})')
            return False


        valid_dataset = True
        for param in params_dict:

            if param == 'user_value':
                continue
            if params_dict[param]['values'] is None:
                continue
            param_here = pinfo.dinfo[param]
            arg_vals = params_dict[param]['values']
            if param_here.upper() not in arg_vals:
                print(f'[WARNING] {name_products[idataset]}/{name_datasets[idataset]}:Inconsistency  between CSV dictionary and JSON product file. {param} is defined as {param_here} in JSON, whereas the argument(s) passed and defined in the CSV dictionary are {arg_vals}')

        if not valid_dataset:
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
    # pinfomy = None
    # if args.mode == 'DT':
    #    pinfomy = pinfo.get_pinfomy_equivalent()
    reformat = ReformatCMEMS(args.reformat_version,args.verbose)
    reformat.use_sh = args.use_sh_reformat
    reformat.launch_script = False if args.make_test_reformat else True
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
        print(f'[INFO] Uploading files to MDS: Started')
    delete_nrt = False
    pinfo.MODE = 'UPLOAD'
    upload = UploadMDS(args.verbose)
    if pinfomy is not None:
        if args.verbose:
            print(f'[INFO] Using equivalent MY product: {pinfomy.product_name};dataset:{pinfomy.dataset_name}')
        pinfomy.MODE = 'UPLOAD'
        upload.make_upload_daily('MY',pinfomy,start_date,end_date)
        #upload.upload_daily_dataset_pinfo(pinfomy, 'MY', start_date, end_date, True,args.verbose)
        delete_nrt = True
    else:
        upload.make_upload_daily(args.mode,pinfo,start_date,end_date)
        #Upload.upload_daily_dataset_pinfo(pinfo, args.mode, start_date, end_date, True, args.verbose)

    #delete nrt if needed
    if delete_nrt:
        start_date_nrt = start_date - timedelta(days=1)
        end_date_nrt = end_date - timedelta(days=1)
        upload.delete_nrt_daily_dataset(pinfo,start_date_nrt,end_date_nrt,True,args.verbose)

    if args.verbose:
        print(f'[INFO] Uploading files to MDS: Completed')
        print('***********************************************************')
        print(f'[INFO] Deleting files: Started')

    pinfo.MODE = 'REFORMAT'
    pinfo.delete_list_file_path_orig(start_date, end_date, args.verbose)
    if args.verbose:
        print(f'[INFO] Deleting files: Completed')


def main():
    print('[INFO] Started reformat and upload code!')

    if args.check_select_choices:
        dsel = DatasetSelection(args.mode, args.product_info_folder)
        choices = dsel.get_potential_choices()
        if choices is None:
            return
        for choice in choices:
            print(f'[DATASET SELECTION] {choice}: {choices[choice]}')
        return

    ##DATASETS SELECTION
    if args.verbose:
        print(f'[INFO] Dataset selection')
    name_products, name_datasets = get_datasets()
    if name_products is None or name_datasets is None:
        print(f'[ERROR] No datasets selected')
        return
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
    if args.product_info_folder:
        pinfo.path2info = args.product_info_folder

    for idataset in range(n_datasets):
        pinfo.set_dataset_info(name_products[idataset], name_datasets[idataset])
        if args.verbose:
            print(f'[INFO] Working with dataset: {name_products[idataset]}/{name_datasets[idataset]}')

        pinfomy = None
        if args.mode == 'DT':
            pinfomy = pinfo.get_pinfomy_equivalent()

        if pinfo.dinfo['frequency'] == 'd':
            if args.make_test_reformat:
                make_reformat_daily(pinfo, pinfomy, start_date, end_date)
            elif args.make_only_reformat and not args.make_only_upload:
                make_reformat_daily(pinfo, pinfomy, start_date, end_date)
            elif not args.make_only_reformat and args.make_only_upload:
                make_upload_daily(pinfo,pinfomy,start_date,end_date)
            elif not args.make_only_reformat and not args.make_only_upload:
                make_reformat_daily(pinfo, pinfomy, start_date, end_date)
                make_upload_daily(pinfo, pinfomy, start_date, end_date)
            #if not args.no_upload:
            #    make_upload_daily(pinfo, pinfomy, start_date, end_date)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
