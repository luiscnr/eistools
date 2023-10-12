import argparse
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

        date_work = date_work + timedelta(hours=24)


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