import argparse
from datetime import datetime as dt
from product_info import ProductInfo
import reformattoCMEMS_202207 as reformat

parser = argparse.ArgumentParser(description='Upload 2DBS')
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
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
            pinfo.MODE = 'REFORMAT'
            pinfo.delete_list_file_path_orig(start_date,end_date,args.verbose)
            reformat.make_reformat_daily_dataset(pinfo, start_date, end_date, args.verbose)
            # file_list = pinfo.get_list_file_path_orig(start_date,end_date)
            # for f in file_list:
            #     print(f)
        if pinfo.dinfo['frequency'] == 'm':
            reformat.make_reformat_monthly_dataset(pinfo, start_date, end_date,args.verbose)
            file_list = pinfo.get_list_file_path_orig_monthly(start_date, end_date)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()