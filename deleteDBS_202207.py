import argparse
from datetime import datetime as dt
from datetime import timedelta
from calendar import monthrange
from product_info import ProductInfo
from ftplib import FTP
from configparser import RawConfigParser
import os, hashlib
import lxml.etree as ET
import general_options as goptions
from dataset_selection import DatasetSelection

parser = argparse.ArgumentParser(description='Reformat and upload to the DBS')
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
parser.add_argument("-umds", "--use_mds", help="Use MDS server", action="store_true")
parser.add_argument("-version","--reformat_version",help="Reformat version.",choices=['202207','202411'],default='202207')
parser.add_argument('-check', "--check_param", help="Check params mode.", action="store_true")
parser.add_argument("-noupload", "--no_upload", help="No upload mode, only reformat.", action="store_true")
parser.add_argument("-noreformat", "--no_reformat", help="No reformat mode, only upload.", action="store_true")
parser.add_argument('-del', "--del_folder", help="Delete folders mode.", action="store_true")
parser.add_argument("-m", "--mode", help="Mode.", type=str, required=True, choices=['NRT', 'DT', 'MY'])
parser.add_argument("-r", "--region", help="Region.", type=str, choices=['BAL', 'MED', 'BLK', 'BS'])
parser.add_argument("-l", "--level", help="Level.", type=str, choices=['l3', 'l4'])
parser.add_argument("-d", "--dataset_type", help="Dataset.", type=str,
                    choices=['reflectance', 'plankton', 'optics', 'transp', 'pp'])
parser.add_argument("-s", "--sensor", help="Sensor.", type=str,
                    choices=['multi', 'olci', 'gapfree_multi', 'multi_climatology', 'cci'])
parser.add_argument("-sd", "--start_date", help="Start date (yyyy-mm-dd)")
parser.add_argument("-ed", "--end_date", help="Start date (yyyy-mm-dd)")
parser.add_argument("-pname", "--name_product", help="Product name")
parser.add_argument("-pfreq", "--frequency_product",
                    help="Select datasets of selected product (-pname) with this frequency", choices=['d', 'm', 'c'])
parser.add_argument("-dname", "--name_dataset", help="Product name")

args = parser.parse_args()


def main():
    print('[INFO] Started delete DBS')

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
            make_delete_daily_dataset(pinfo, args.mode, start_date, end_date, args.use_mds, args.verbose)
        if pinfo.dinfo['frequency'] == 'm':
            make_delete_monthly_dataset(pinfo, args.mode, start_date, end_date, args.use_mds, args.verbose)


def make_delete_daily_dataset(pinfo, mode, start_date, end_date, use_mds, verbose):
    year_ini = start_date.year
    year_fin = end_date.year
    for year in range(year_ini, year_fin + 1):
        month_ini = 1
        month_fin = 12
        if year == year_ini:
            month_ini = start_date.month
        if year == year_fin:
            month_fin = end_date.month
        for month in range(month_ini, month_fin + 1):
            day_ini = 1
            if year == year_ini and month == month_ini:
                day_ini = start_date.day
            day_fin = monthrange(year, month)[1]
            if year == year_fin and month == month_fin:
                day_fin = end_date.day
            if verbose:
                print('-------------------------------------------------------------------')
                print(f'[INFO] Launching delete from DU for year {year} and month {month}')

            delete_daily_dataset_impl(pinfo, mode, year, month, day_ini, day_fin, use_mds, verbose)

    ##DEPRECATED: Year and month folders don't need to be removed manually
    # for year in range(year_ini, year_fin + 1):
    #     month_ini = 1
    #     month_fin = 12
    #     if year == year_ini:
    #         month_ini = start_date.month
    #     if year == year_fin:
    #         month_fin = end_date.month
    #     for month in range(month_ini, month_fin + 1):
    #         delete_month_folder(pinfo, mode, year, month, use_mds, verbose)
    #
    # for year in range(year_ini, year_fin + 1):
    #     delete_year_folder(pinfo, mode, year, use_mds, verbose)


def make_delete_monthly_dataset(pinfo, mode, start_date, end_date, use_mds, verbose):
    year_ini = start_date.year
    year_fin = end_date.year
    for year in range(year_ini, year_fin + 1):
        month_ini = 1
        month_fin = 12
        if year == year_ini:
            month_ini = start_date.month
        if year == year_fin:
            month_fin = end_date.month
        delete_monthly_dataset_impl(pinfo, mode, year, month_ini, month_fin, use_mds, verbose)

    ## DEPRECATED, MANUAL DELETE OF EMPTY FOLDERS IS NOT NEEDED ANYMORE
    # for year in range(year_ini, year_fin + 1):
    #     delete_year_folder(pinfo, mode, year, use_mds, verbose)


def delete_daily_dataset_impl(pinfo, mode, year, month, start_day, end_day, use_mds, verbose):
    ftpdu = FTPUpload('cnr', mode, use_mds)
    # DEPRECATED FTP to check files, replaced by S3Bucket
    # ftpnormal = FTPUpload('normal', mode, False)
    from s3buckect import S3Bucket
    sb = S3Bucket()
    sb.update_params_from_pinfo(pinfo)
    conn = sb.star_client()
    if not conn:
        print(f'[ERROR] Connection error with s3 bucket. Daily dataset could not be deleted')
        return
    deliveries = Deliveries()
    rpath, sdir = pinfo.get_remote_path(year, month)
    if verbose:
        print(f'[INFO] Remote path: {rpath}/{sdir}')
    ftpdu.go_month_subdir(rpath, year, month)

    ##DEPRECTATED FTP NORMAL, REPLACED BY S3 BUCKECT
    # rpathnormal, sdir = pinfo.get_remote_path_normal(year, month)
    # gomonth = ftpnormal.go_month_subdir(rpathnormal, year, month)
    # if not gomonth:
    #     print(f'[INFO] No directory found for year {year} and month {month}')
    #     return

    use_dt_suffix = goptions.use_dt_suffix()



    for day in range(start_day, end_day + 1):
        date_here = dt(year, month, day)
        if args.verbose:
            print('-------------------------')
            print(f'[INFO] Date: {date_here}')
        remote_file_name = pinfo.get_remote_file_name(date_here)
        if mode == 'DT' and pinfo.dinfo['mode'] == 'NRT' and use_dt_suffix:
            remote_file_name = remote_file_name.replace('nrt', 'dt')
        if (mode == 'MY' or mode == 'MYINT') and pinfo.dinfo['mode'] == 'MY':
            datemyintref = dt.strptime(pinfo.dinfo['myint_date'], '%Y-%m-%d')
            if date_here >= datemyintref:
                remote_file_name = remote_file_name.replace('my', 'myint')

        if args.verbose:
            print(f'[INFO] Remote_file_name: {remote_file_name}')

        ##DEPRECATED,USING S3 BUCKET
        # if not ftpnormal.exist_file_name(remote_file_name):
        #     print(f'[INFO] Expected file name to be deleted {remote_file_name} does not exist.')
        #     continue

        s3bname,key,isuploaded = sb.check_daily_file(mode, pinfo, date_here, False)
        if not isuploaded :
            print(f'[INFO] Expected file name to be deleted {remote_file_name} does not exist.')
            continue

        sdir_remote_file_name = os.path.join(sdir, remote_file_name)
        tagged_dataset = pinfo.get_tagged_dataset()
        upload_TS = dt.utcnow().strftime('%Y%m%dT%H%M%SZ')
        deliveries.add_delete(pinfo.product_name, tagged_dataset, sdir_remote_file_name, upload_TS)
        dnt_file_name, dnt_file_path = deliveries.create_dnt_file(pinfo.product_name)
        ftpdu.go_dnt(pinfo.product_name)
        status, rr, start_upload_TS, stop_upload_TS = ftpdu.transfer_file(dnt_file_name, dnt_file_path)
        if status == 'Delivered':
            if args.verbose:
                print(f'[INFO] DNT file {dnt_file_name} transfer to DU succeeded')
        else:
            print(f'[ERROR] DNT file {dnt_file_name} transfer to DU failed')

    ftpdu.close()
    sb.close_client()
    # ftpnormal.close()


def delete_monthly_dataset_impl(pinfo, mode, year, month_ini, month_fin, use_mds, verbose):
    ftpdu = FTPUpload('cnr', mode, use_mds)
    # DEPRECATED FTP to check files, replaced by S3Bucket
    #ftpnormal = FTPUpload('normal', mode, False)
    from s3buckect import S3Bucket
    sb = S3Bucket()
    sb.update_params_from_pinfo(pinfo)
    conn = sb.star_client()
    if not conn:
        print(f'[ERROR] Connection error with s3 bucket. Daily dataset could not be deleted')
        return
    deliveries = Deliveries()
    rpath, sdir = pinfo.get_remote_path_monthly(year)
    if verbose:
        print(f'[INFO] Remote path: {rpath}/{sdir}')
    ftpdu.go_year_subdir(rpath, year)

    # DEPRECATED
    # rpathnormal, sdir = pinfo.get_remote_path_monthly_normal(year)
    # goyear = ftpnormal.go_year_subdir(rpathnormal, year)
    # if not goyear:
    #     print(f'[INFO] No directory found for year: {year}')
    #     return

    use_dt_suffix = goptions.use_dt_suffix() ##always false, use_dt_suffix is deprecated



    for month in range(month_ini, month_fin + 1):
        date_here = dt(year, month, 15)
        if args.verbose:
            print('-------------------------')
            print(f'[INFO] Date: {date_here}')
        remote_file_name = pinfo.get_remote_file_name_monthly(date_here)
        if mode == 'DT' and pinfo.dinfo['mode'] == 'NRT' and use_dt_suffix:
            remote_file_name = remote_file_name.replace('nrt', 'dt')
        if (mode == 'MY' or mode=='MYINT') and pinfo.dinfo['mode'] == 'MY':
            datemyintref = dt.strptime(pinfo.dinfo['myint_date'], '%Y-%m-%d')
            if date_here >= datemyintref:
                remote_file_name = remote_file_name.replace('my', 'myint')
        if args.verbose:
            print(f'[INFO] Remote_file_name: {remote_file_name}')
        # if not ftpnormal.exist_file_name(remote_file_name):
        #     print(f'[INFO] Expected file name to be deleted {remote_file_name} does not exist.')
        #     continue
        s3bname, key, isuploaded = sb.check_monthly_file(mode, pinfo, date_here, True)
        if not isuploaded:
            print(f'[INFO] Expected file name to be deleted {remote_file_name} does not exist.')
            continue
        sdir_remote_file_name = os.path.join(sdir, remote_file_name)
        tagged_dataset = pinfo.get_tagged_dataset()
        upload_TS = dt.utcnow().strftime('%Y%m%dT%H%M%SZ')
        deliveries.add_delete(pinfo.product_name, tagged_dataset, sdir_remote_file_name, upload_TS)
        dnt_file_name, dnt_file_path = deliveries.create_dnt_file(pinfo.product_name)
        ftpdu.go_dnt(pinfo.product_name)
        status, rr, start_upload_TS, stop_upload_TS = ftpdu.transfer_file(dnt_file_name, dnt_file_path)
        if status == 'Delivered':
            if args.verbose:
                print(f'[INFO] DNT file {dnt_file_name} transfer to DU succeeded')
        else:
            print(f'[ERROR] DNT file {dnt_file_name} transfer to DU failed')

    ftpdu.close()
    sb.close_client()
    #ftpnormal.close()


def delete_year_folder(pinfo, mode, year, use_mds, verbose):
    ftpdu = FTPUpload('cnr', mode, use_mds)
    rpath, sdir = pinfo.get_remote_path_monthly(year)
    if verbose:
        print('-------------------------')
        print(f'[INFO] Deleting remote path for {year}: {rpath}/{sdir}...')

    ##DEPRECATED BY S3BUCKET
    # ftpnormal = FTPUpload('normal', mode, False)
    rpathnormal, sdir = pinfo.get_remote_path_monthly_normal(year)
    # goyear = ftpnormal.go_year_subdir(rpathnormal, year)
    # if not goyear:
    #     print(f'[INFO] No directory found for year: {year}')
    #     return

    from s3buckect import S3Bucket
    sb = S3Bucket()
    sb.update_params_from_pinfo(pinfo)
    sb.star_client()


    ftpdu.go_year_subdir(rpath, year)

    if sb.is_empty_year(year):
        deliveriesf = Deliveries()
        tagged_dataset = pinfo.get_tagged_dataset()
        upload_TS = dt.utcnow().strftime('%Y%m%dT%H%M%SZ')
        deliveriesf.add_delete_year(pinfo.product_name, tagged_dataset, year, upload_TS)
        if verbose:
            print(f'[INFO] Deleting empty year directory: {pinfo.product_name}/{tagged_dataset}/{year}')
        dnt_file_name, dnt_file_path = deliveriesf.create_dnt_file(pinfo.product_name)
        ftpdu.go_dnt(pinfo.product_name)
        status, rr, start_upload_TS, stop_upload_TS = ftpdu.transfer_file(dnt_file_name, dnt_file_path)
        if status == 'Delivered':
            if args.verbose:
                print(f'[INFO] DNT file {dnt_file_name} transfer to DU succeeded')
        else:
            print(f'[ERROR] DNT file {dnt_file_name} transfer to DU failed')
    else:
        tagged_dataset = pinfo.get_tagged_dataset()
        if verbose:
            print(f'[INFO] Year directory: {pinfo.product_name}/{tagged_dataset}/{year} is not empty. Skiping...')
    ftpdu.close()
    #ftpnormal.close()
    sb.close_client()

def delete_month_folder(pinfo, mode, year, month, use_mds, verbose):
    ftpdu = FTPUpload('cnr', mode, use_mds)

    rpath, sdir = pinfo.get_remote_path(year, month)
    if verbose:
        print('-------------------------')
        print(f'[INFO] Deleting remote path for {year}-{month}: {rpath}/{sdir}...')
    rpathnormal, sdir = pinfo.get_remote_path_normal(year, month)

    ##DEPRECATED BY S3BUCKET
    # ftpnormal = FTPUpload('normal', mode, False)
    # gomonth = ftpnormal.go_month_subdir(rpathnormal, year, month)
    # if not gomonth:
    #     print(f'[INFO] No directory found for year-month: {year}-{month} ')
    #     return
    ftpdu.go_month_subdir(rpath, year, month)
    from s3buckect import S3Bucket
    sb = S3Bucket()
    sb.update_params_from_pinfo(pinfo)
    sb.star_client()
    sb.update_params_from_pinfo(pinfo)


    if sb.is_empty_month(year,month):
        deliveriesf = Deliveries()
        tagged_dataset = pinfo.get_tagged_dataset()
        upload_TS = dt.utcnow().strftime('%Y%m%dT%H%M%SZ')
        deliveriesf.add_delete_month(pinfo.product_name, tagged_dataset, year, month, upload_TS)
        if verbose:
            print(f'[INFO] Deleting empty month directory: {pinfo.product_name}/{tagged_dataset}/{year}/{month}')
        dnt_file_name, dnt_file_path = deliveriesf.create_dnt_file(pinfo.product_name)
        ftpdu.go_dnt(pinfo.product_name)
        status, rr, start_upload_TS, stop_upload_TS = ftpdu.transfer_file(dnt_file_name, dnt_file_path)
        if status == 'Delivered':
            if args.verbose:
                print(f'[INFO] DNT file {dnt_file_name} transfer to DU succeeded')
        else:
            print(f'[ERROR] DNT file {dnt_file_name} transfer to DU failed')
    else:
        tagged_dataset = pinfo.get_tagged_dataset()
        if verbose:
            print(
                f'[INFO] Year/Month directory: {pinfo.product_name}/{tagged_dataset}/{year}/{month} is not empty. Skiping...')
    ftpdu.close()
    #ftpnormal.close()
    sb.close_client()


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


class FTPUpload():
    # user: cnr or normal
    def __init__(self, user, mode, use_mds):
        sdir = os.path.abspath(os.path.dirname(__file__))
        # path2script = "/".join(sdir.split("/")[0:-1])
        path2script = os.path.dirname(sdir)
        credentials = RawConfigParser()
        credentials.read(os.path.join(path2script, 'credentials.ini'))

        if mode == 'MY':
            if use_mds:
                du_server = 'ftp-my.marine.copernicus.eu'
            else:
                du_server = "my.cmems-du.eu"

        elif mode == 'NRT' or mode == 'DT':
            if use_mds:
                du_server = "ftp-nrt.marine.copernicus.eu"
            else:
                du_server = "nrt.cmems-du.eu"

        du_uname = credentials.get(user, 'uname')
        du_passwd = credentials.get(user, 'passwd')
        self.ftpdu = FTP(du_server, du_uname, du_passwd)

    def go_subdir(self, rpath):
        # print('Changing directory to: ', rpath)
        self.ftpdu.cwd(rpath)

    def go_year_subdir(self, rpath, year):
        dateref = dt(year, 1, 1)
        yearstr = dateref.strftime('%Y')
        self.ftpdu.cwd(rpath)
        if not (yearstr in self.ftpdu.nlst()):
            # self.ftpdu.mkd(yearstr)
            return False
        self.ftpdu.cwd(yearstr)
        return True

    def go_month_subdir(self, rpath, year, month):
        dateref = dt(year, month, 1)
        yearstr = dateref.strftime('%Y')  # dt.strptime(str(year), '%Y')
        monthstr = dateref.strftime('%m')  # dt.strptime(month, '%m')

        self.ftpdu.cwd(rpath)

        if not (yearstr in self.ftpdu.nlst()):
            # self.ftpdu.mkd(yearstr)
            return False
        self.ftpdu.cwd(yearstr)

        if not (monthstr in self.ftpdu.nlst()):
            # self.ftpdu.mkd(monthstr)
            return False
        self.ftpdu.cwd(monthstr)
        return True

    def go_dnt(self, product):
        dnt_dir_name = os.path.join(os.sep, product, 'DNT')
        self.ftpdu.cwd(dnt_dir_name)
        self.ftpdu.pwd()

    def transfer_file(self, remote_file_name, pfile):
        start_upload_TS = dt.utcnow().strftime('%Y%m%dT%H%M%SZ')
        ftpCMD = 'STOR ' + remote_file_name
        rr = self.ftpdu.storbinary(ftpCMD, open(pfile, 'rb'))
        if rr == '226 Transfer complete.':
            status = 'Delivered'
        else:
            status = 'Failed'
        stop_upload_TS = dt.utcnow().strftime('%Y%m%dT%H%M%SZ')

        return status, rr, start_upload_TS, stop_upload_TS

    def exist_file_name(self, remote_file_name):
        if remote_file_name in self.ftpdu.nlst():
            return True
        else:
            return False

    def isempty(self):
        a = self.ftpdu.nlst()
        if len(a) == 0:
            return True
        else:
            return False

    def close(self):
        self.ftpdu.close()


class Deliveries():
    def __init__(self):
        self.deliveries = dict()
        self.dnt_file_name = None
        sdir = os.path.abspath(os.path.dirname(__file__))
        # path2script = "/".join(sdir.split("/")[0:-1])
        path2script = os.path.dirname(sdir)
        self.XMLPath = os.path.join(path2script, 'XML_del')

    def create_dnt_file(self, product):
        dnt_file_name = None
        dnt_file_path = None
        if not (product in self.deliveries.keys()):
            return dnt_file_name, dnt_file_path
        testo = ET.tostring(self.deliveries[product], xml_declaration='yes', pretty_print=True).decode()
        dnt_file_name = product + '_P' + self.deliveries[product].attrib['date'] + '.xml'
        dnt_file_path = os.path.join(self.XMLPath, dnt_file_name)
        # print(year, '-', month, ': ', product)
        dnt_file = open(dnt_file_path, 'w')
        dnt_file.write(testo)
        dnt_file.close()

        return dnt_file_name, dnt_file_path

    def get_dataset_subelement(self, product, dataset):
        if not (product in self.deliveries.keys()):
            return None
        dataset_se = None
        for index, child in enumerate(self.deliveries[product]):
            if child.attrib['DatasetName'] == dataset:
                dataset_se = child
                break
        return dataset_se

    def get_datafile_subelement(self, product, dataset, remote_file_name):
        dataset_se = self.get_dataset_subelement(product, dataset)
        if dataset_se is None:
            return None
        datafile_se = None
        for index, child in enumerate(dataset_se):
            if child.attrib['FileName'] == remote_file_name:
                datafile_se = child
                break
        return datafile_se

    def get_directory_subelement(self, product, dataset, directory):
        dataset_se = self.get_dataset_subelement(product, dataset)
        if dataset_se is None:
            return None
        datafile_se = None
        for index, child in enumerate(dataset_se):
            if child.attrib['SourceFolderName'] == directory:
                datafile_se = child
                break
        return datafile_se

    def add_product(self, product, start_upload_TS):
        if not (product in self.deliveries.keys()):
            self.deliveries[product] = ET.Element("delivery", product=product, PushingEntity="OC-CNR-ROMA-IT",
                                                  date=start_upload_TS)

    def add_dataset(self, product, dataset, start_upload_TS):
        if not product in self.deliveries.keys():
            self.add_product(product, start_upload_TS)
        dataset_se = self.get_dataset_subelement(product, dataset)
        if dataset_se is None:
            dataset_se = ET.SubElement(self.deliveries[product], "dataset", DatasetName=dataset)
        return dataset_se

    def add_delete_month(self, product, dataset, year, month, upload_TS):
        dataset_se = self.get_dataset_subelement(product, dataset)
        datafile_se = None
        dataref = dt(year, month, 15)
        yearstr = dataref.strftime('%Y')
        monthstr = dataref.strftime('%m')
        pathym = os.path.join(yearstr, monthstr)
        if dataset_se is None:
            dataset_se = self.add_dataset(product, dataset, upload_TS)
        else:
            datafile_se = self.get_directory_subelement(product, dataset, pathym)
        if datafile_se is None:
            datafile_se = ET.SubElement(dataset_se, "directory",
                                        SourceFolderName=pathym,
                                        DestinationFolderName="",
                                        )
            ET.SubElement(datafile_se, "KeyWord").text = 'Delete'
        return datafile_se

    def add_delete_year(self, product, dataset, year, upload_TS):
        dataset_se = self.get_dataset_subelement(product, dataset)
        datafile_se = None
        dataref = dt(year, 6, 15)
        pathy = dataref.strftime('%Y')
        if dataset_se is None:
            dataset_se = self.add_dataset(product, dataset, upload_TS)
        else:
            datafile_se = self.get_directory_subelement(product, dataset, pathy)
        if datafile_se is None:
            datafile_se = ET.SubElement(dataset_se, "directory",
                                        SourceFolderName=pathy,
                                        DestinationFolderName="",
                                        )
            ET.SubElement(datafile_se, "KeyWord").text = 'Delete'
        return datafile_se

    def add_delete(self, product, dataset, remote_file_name, upload_TS):
        dataset_se = self.get_dataset_subelement(product, dataset)
        datafile_se = None
        if dataset_se is None:
            dataset_se = self.add_dataset(product, dataset, upload_TS)
        else:
            datafile_se = self.get_datafile_subelement(product, dataset, remote_file_name)

        if datafile_se is None:
            datafile_se = ET.SubElement(dataset_se, "file",
                                        FileName=remote_file_name,
                                        StartUploadTime="",
                                        StopUploadTime="",
                                        Checksum="",
                                        FinalStatus="",
                                        FileType="")
            # datafile_se = ET.SubElement(dataset_se, "directory",
            #                             SourceFolderName="2021",
            #                             DestinationFolderName="",
            #                             )
            ET.SubElement(datafile_se, "KeyWord").text = 'Delete'

        return datafile_se

    def add_datafile(self, product, dataset, pfile, remote_file_name, start_upload_TS, stop_upload_TS, status):

        dataset_se = self.get_dataset_subelement(product, dataset)
        datafile_se = None
        if dataset_se is None:
            dataset_se = self.add_dataset(product, dataset, start_upload_TS)
        else:
            datafile_se = self.get_datafile_subelement(product, dataset, remote_file_name)
        if datafile_se is None:
            datafile_se = ET.SubElement(dataset_se, "file",
                                        FileName=remote_file_name,
                                        StartUploadTime=start_upload_TS,
                                        StopUploadTime=stop_upload_TS,
                                        Checksum=self.md5(pfile),
                                        FinalStatus=status,
                                        FileType='DT')
        return datafile_se

    def add_resend_attempt_to_datafile_se(self, datafile_se, rr, count):
        ET.SubElement(datafile_se,
                      "resendAttempt",
                      DueToErrorCode=str(rr.split(' ')[0]),
                      DueToErrorMsg=rr,
                      NumberOfAttempts=str(count))

    def add_delete_NRT_from_datafile_se(self, datafile_se):
        dataset_se = datafile_se.getparent()
        path_complete = datafile_se.attrib['FileName']
        path_name = os.path.dirname(path_complete)
        file_name = os.path.basename(path_complete)
        nrtfile_tobedeleted = file_name.replace('dt', 'nrt')
        datafile_delete_se = ET.SubElement(dataset_se, "file",
                                           FileName=os.path.join(path_name, nrtfile_tobedeleted),
                                           StartUploadTime="",
                                           StopUploadTime="",
                                           Checksum="",
                                           FinalStatus="",
                                           FileType="")
        ET.SubElement(datafile_delete_se, "KeyWord").text = 'Delete'

    def md5(self, fname):
        hash_md5 = hashlib.md5()
        with open(fname, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
