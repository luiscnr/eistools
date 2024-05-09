import argparse
import os.path
from datetime import datetime as dt
from datetime import timedelta
from product_info import ProductInfo
import calendar

parser = argparse.ArgumentParser(description='Check make monthly')
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
parser.add_argument("-m", "--mode", help="Mode.", type=str, required=True, choices=['NRT', 'DT', 'MY'])
parser.add_argument("-pname", "--name_product", help="Product name",required=True)
parser.add_argument("-dname", "--name_dataset", help="Product name",required=True)
parser.add_argument("-sd", "--start_date", help="Start date (yyyy-mm-dd)",required=True)
parser.add_argument("-o", "--output_file", help="Start date (yyyy-mm-dd)",required=True)

args = parser.parse_args()

def main():
    print('STARTED')
    date = None
    try:
        date= dt.strptime(args.start_date,'%Y-%m-%d')
    except:
        date = None
    if date is None:
        return
    mode = args.mode
    pinfo = ProductInfo()
    pinfo.set_dataset_info(args.name_product,args.name_dataset)

    last_date_month = (date.replace(day=15)-timedelta(days=30))
    ls = calendar.monthrange(last_date_month.year,last_date_month.month)
    last_date_month = last_date_month.replace(day=ls[1])

    file_orig = pinfo.get_file_path_orig_monthly(None,last_date_month)
    file_exist = False
    if file_orig is not None:
        file_exist = os.path.exists(file_orig)

    make_processing = False
    if mode=='NRT':
        if date.day==1:
            make_processing = True
        if 1 < date.day < 8:
            if not file_exist:
                make_processing = True

    if mode=='DT':
        if date.day==8:
            make_processing = True
        if date.day>8:
            if not file_exist:
                make_processing = True
            else:
                ti_m = dt.utcfromtimestamp(os.path.getmtime(file_orig))
                if ti_m.month==date.month and ti_m.day<8:
                    make_processing = True

    file_out = args.output_file
    fout = open(file_out,'w')
    if make_processing:
        fout.write(last_date_month.strftime("%Y-%m-%d"))
    else:
        fout.write('NONE')
    fout.close()





# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
