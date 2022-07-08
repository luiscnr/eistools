import argparse
from datetime import datetime as dt
from datetime import timedelta
from product_info import ProductInfo
import check_202207 as checkftp

parser = argparse.ArgumentParser(description='Daily reports')
parser.add_argument("-m", "--mode", help="Mode.", type=str, required=True, choices=['NRT', 'DT'])
parser.add_argument("-sd", "--date", help="Date.")
args = parser.parse_args()


def main():
    date = None
    if args.date:
        date = get_date_from_param(args.date)
    else:
        date = dt.now().replace(hour=12, minute=0, second=0, microsecond=0)

    if date is None:
        print(
            f'[ERROR] Date {args.date} is not in the correct format. It should be YYYY-mm-dd or integer (relative days')
        return

    name_products, name_datasets, dates = get_list_products_datasets(args.mode, date)
    lines = []
    ndatasets = len(name_datasets)
    nuploaded = 0
    for idx in range(len(name_products)):
        #print(name_products[idx], name_datasets[idx], dates[idx])
        lines_dataset, isuploaded = get_lines_dataset(name_products[idx], name_datasets[idx], dates[idx])
        if isuploaded:
            nuploaded = nuploaded + 1
        lines = [*lines, *lines_dataset]

    start_lines = get_start_lines(date, ndatasets, ndatasets, ndatasets, nuploaded)
    lines = [*start_lines, *lines]
    print_email_lines(lines)


def get_start_lines(date, ndatasets, ncompleted, nprocessed, nuploaded):
    lines = []
    datestr = date.strftime('%Y-%m-%d')
    lines.append(f'DAILY TECHNICAL REPORT')
    lines.append(f'MODE: {args.mode}')
    lines.append(f'DATE: {datestr}')
    lines.append(f'TOTAL NUMBER OF DATASETS: {ndatasets}')
    lines.append(f'COMPLETED DATASETS (NON DEGRADED): {ncompleted}/{ndatasets} * NO IMPLEMENTED YET')
    lines.append(f'PROCESSED DATASETS: {nprocessed}/{ndatasets} * NO IMPLEMENTED YET')
    lines.append(f'UPLOADED DATASETS: {nuploaded}/{ndatasets}')
    lines.append('')
    return lines


def get_lines_dataset(name_product, name_dataset, date):
    lines = []
    datestr = date.strftime('%Y-%m-%d')
    pinfo = ProductInfo()
    pinfo.set_dataset_info(name_product, name_dataset)
    lines.append('*************************************************************************************************')
    lines.append(f'PRODUCT: {name_product}')
    lines.append(f'DATASET: {name_dataset}')
    lines.append(f'DATE: {datestr}')
    lines.append(f'REGION: {pinfo.get_region()}')
    lines.append(f'SENSOR: {pinfo.get_sensor()}')
    lines.append(f'DATASET TYPE: {pinfo.get_dtype()}')
    lines.append(f'LEVEL: {pinfo.get_level()}')
    lines.append(f'FREQUENCY: {pinfo.get_frequency()}')

    upload_mode = args.mode
    if args.mode == 'DT':
        pinfomy = pinfo.get_pinfomy_equivalent()
        if not pinfomy is None:
            upload_mode = 'MYINT'
    if upload_mode == 'MYINT':
        rpath, remote_file_name, isuploaded = checkftp.check_dailyfile_du('MYINT', pinfomy, date, False)
    else:
        rpath, remote_file_name, isuploaded = checkftp.check_dailyfile_du(args.mode, pinfo, date, False)
    lines.append('------------------')
    lines.append('DU Upload')
    lines.append(f'Upload mode:  {upload_mode.lower()}')
    lines.append(f'Remote path: {rpath}')
    lines.append(f'Remote file name: {remote_file_name}')
    if isuploaded:
        lines.append('Status: OK')
    else:
        lines.append('Status: FAILED')

    return lines, isuploaded


def get_list_products_datasets(mode, date):
    pinfo = ProductInfo()
    name_products = []
    name_datasets = []
    dates = []
    regions = ['BAL', 'MED', 'BLK']
    levels = ['L3', 'L4']
    sensors = ['olci', 'multi', 'gapfree_multi']
    if mode == 'NRT':
        date_nrt = date - timedelta(days=1)
    if mode == 'DT':
        date_dt_multi = date - timedelta(days=20)
        date_dt_interp = date - timedelta(days=24)
        date_dt_olci = date - timedelta(days=8)

    # DAILY PRODUCTS
    for region in regions:
        for level in levels:
            for sensor in sensors:
                name_p, name_d = pinfo.get_list_datasets_params('NRT', region, level, None, sensor, 'd')

                if mode == 'NRT':
                    dates_d = [date_nrt] * len(name_p)
                if mode == 'DT':
                    if sensor == 'multi':
                        dates_d = [date_dt_multi] * len(name_p)
                    elif sensor == 'olci':
                        dates_d = [date_dt_olci] * len(name_p)
                    elif sensor == 'gapfree_multi':
                        dates_d = [date_dt_interp] * len(name_p)
                name_products = [*name_products, *name_p]
                name_datasets = [*name_datasets, *name_d]
                dates = [*dates, *dates_d]

    return name_products, name_datasets, dates


def print_email_lines(lines):
    for line in lines:
        print(line)


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
