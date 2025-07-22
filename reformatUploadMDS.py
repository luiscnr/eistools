import argparse

from dataset_selection import DatasetSelection
from datetime import datetime as dt
from datetime import timedelta
from product_info import ProductInfo
#import reformattoCMEMS_202207 as reformat
from reformatCMEMS import ReformatCMEMS
from reformat_uploadtoDBS_202207 import make_upload_daily

parser = argparse.ArgumentParser(description='Reformat and upload to the MDS')
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
parser.add_argument("-version", "--reformat_version", help="Reformat version.", choices=['202207', '202411'],
                    default='202411')
parser.add_argument("-only_upload", "--make_only_upload", help="Only upload mode.", action="store_true")
parser.add_argument("-only_reformat", "--make_only_reformat", help="Only reformat mode.", action="store_true")
parser.add_argument("-use_sh", "--use_sh_reformat", help="Create sh file for reformat.", action="store_true")
parser.add_argument("-test_reformat", "--make_test_reformat", help="Reformat script is created but not launched. To create a sh script instead of the slurm default, use -use_sh(--use_sh_reformat) option", action="store_true")
parser.add_argument('-check', "--check_param", help="Check params mode.", action="store_true")
parser.add_argument("-m", "--mode", help="Mode.", type=str, required=True, choices=['NRT', 'DT', 'MY'])
parser.add_argument("-r", "--region", help="Dataset selection by region. Choices: MED, BS, BLK, ARC.", type=str)
parser.add_argument("-l", "--level", help="Dataset selection by level. Choices: L3, L4", type=str)
parser.add_argument("-d", "--dataset_type",
                    help="Dataset selection by dataset type. Choices: reflectance, plankton, optics, transp, pp.",
                    type=str)
parser.add_argument("-s", "--sensor", help="Dataset selection by sensor. Choices: multi, olci, gapfree_multi, cci",
                    type=str)
parser.add_argument("-fr", "--frequency", help="Dataset selection by frequency. Choices: daily, monthly, climatology")
parser.add_argument("-user", "--user_value", help="Dataset selection by user value. ")
parser.add_argument("-sd", "--start_date", help="Start date (yyyy-mm-dd)")
parser.add_argument("-ed", "--end_date", help="Start date (yyyy-mm-dd)")
parser.add_argument("-pname", "--name_product", help="Product name")
parser.add_argument("-dname", "--name_dataset", help="Dataset name")
parser.add_argument("-pfolder", "--product_info_folder", help="Product info folder (defaults is ../PRODUCT_INFO)")

args = parser.parse_args()




def get_params_selection_dataset():
    args_dict = vars(args)
    if 'dataset_type' in args_dict.keys():
        args_dict['dataset'] = args_dict.pop('dataset_type')
    params = {
        'region': {
            'values': None,
            'potential_values': ['BAL', 'ARC', 'BLK', 'BS', 'MED']
        },
        'level': {
            'values': None,
            'potential_values': ['L3', 'L4']
        },
        'sensor': {
            'values': None,
            'potential_values': ['MULTI', 'OLCI', 'GAPFREE_MULTI', 'CCI']
        },
        'dataset': {
            'values': None,
            'potential_values': ['REFLECTANCE', 'PLANKTON', 'OPTICS', 'TRANSP', 'PP']
        },
        'frequency': {
            'values': None,
            'potential_values': ['DAILY', 'MONTHLY', 'CLIMA']
        },
        'user_value': {
            'values': None
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
            if param == 'user_value':
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
        selection_params = get_params_selection_dataset()
        dsel.set_params_from_dict(selection_params)
        # print(dsel.params)
        # dsel.set_params(region, level, dataset_type, sensor, frequency)
        name_products, name_datasets = dsel.get_list_product_datasets_from_params()

    return name_products, name_datasets


##DATASETS CHECKING
def check_datasets(name_products, name_datasets):
    mode_check = 'NRT' if args.mode == 'DT' else args.mode
    # if args.mode == 'MYINT':
    #     mode_check = 'MY'
    # if args.mode == 'DT':
    #     mode_check = 'NRT'
    pinfo = ProductInfo()
    if args.product_info_folder:
        pinfo.path2info = args.product_info_folder

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
        params_dict = get_params_selection_dataset()
        valid_dataset = True
        for param in params_dict:

            if param == 'user_value':
                continue
            if params_dict[param]['values'] is None:
                continue
            param_here = pinfo.dinfo[param]
            arg_vals = params_dict[param]['values']
            if param_here.upper() not in arg_vals:
                valid_dataset = False
                print(
                    f'[ERROR] Inconsistency error between CSV dictionary and JSON product file. {param} is defined as {param_here} in JSON, but it was not given as a possible argument: {args.vals}  ')

        if not valid_dataset:
            return False

        # if region is not None:
        #     region_here = pinfo.dinfo['region']
        #     if region_here != region:
        #         print(f'[ERROR] Dataset region is {region_here} but {region} was given in the script')
        #         return False
        # if sensor is not None:
        #     sensor_here = pinfo.dinfo['sensor'].lower()
        #     if sensor_here != sensor:
        #         print(f'[ERROR] Dataset sensor is {sensor_here} but {sensor} was given in the script')
        #         return False
        # if dataset_type is not None:
        #     dataset_type_here = pinfo.dinfo['dataset'].lower()
        #     if dataset_type_here != dataset_type:
        #         print(f'[ERROR] Dataset dataset_type is {dataset_type_here} but {dataset_type} was given in the script')
        #         return False
        # if frequency is not None:
        #     frequency_here = pinfo.dinfo['frequency'].lower()
        #     if frequency_here != frequency:
        #         print(f'[ERROR] Dataset frequency is {frequency_here} but {frequency} was given in the script')
        #         return False
        # if level is not None:
        #     level_here = pinfo.dinfo['level'].lower()
        #     if level_here != level:
        #         print(f'[ERROR] Dataset level is {level_here} but {level} was given in the script')
        #         return False

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
    reformat.launch_script = False if args.make_test_reformat else False
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


def main():
    print('[INFO] Started reformat and upload')

    ##DATASETS SELECTION
    if args.verbose:
        print(f'[INFO] Dataset selection')
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
            elif not args.make_only_reformat and not args.make_only_upload():
                make_reformat_daily(pinfo, pinfomy, start_date, end_date)
                make_upload_daily(pinfo, pinfomy, start_date, end_date)
            #if not args.no_upload:
            #    make_upload_daily(pinfo, pinfomy, start_date, end_date)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
