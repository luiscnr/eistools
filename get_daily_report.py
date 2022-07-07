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
        if args.mode == 'NRT':
            date = dt.now().replace(hour=12, minute=0, second=0, microsecond=0) - timedelta(hours=24)
    if date is None:
        print(
            f'[ERROR] Date {args.date} is not in the correct format. It should be YYYY-mm-dd or integer (relative days')
        return

    name_products, name_datasets = get_list_products_datasets(args.mode, date)
    lines = []
    ndatasets = len(name_datasets)
    nuploaded = 0
    for idx in range(len(name_products)):
        lines_dataset, isuploaded = get_lines_dataset(name_products[idx], name_datasets[idx], date)
        if isuploaded:
            nuploaded = nuploaded + 1
        lines = [*lines, *lines_dataset]

    start_lines = get_start_lines(date,ndatasets,ndatasets,ndatasets,nuploaded)
    lines = [*start_lines,*lines]
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

    rpath, remote_file_name, isuploaded = checkftp.check_dailyfile_du(args.mode, pinfo, date, False)
    lines.append('------------------')
    lines.append('DU Upload')
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
    regions = ['BAL', 'MED', 'BLK']
    levels = ['L3', 'L4']
    for region in regions:
        for level in levels:
            name_p, name_d = pinfo.get_list_datasets_params('NRT', region, level, None, None, 'd')
            name_products = [*name_products, *name_p]
            name_datasets = [*name_datasets, *name_d]

    return name_products, name_datasets


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
