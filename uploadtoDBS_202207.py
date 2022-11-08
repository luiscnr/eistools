import calendar
import datetime
from datetime import datetime as dt
from ftplib import FTP
from configparser import RawConfigParser
import os, hashlib
import lxml.etree as ET
from product_info import ProductInfo
from calendar import monthrange
import argparse
import deleteDBS_202207 as delete
import general_options as goptions

parser = argparse.ArgumentParser(description='Upload 2DBS')
parser.add_argument("-m", "--mode", help="Mode.", type=str, required=True, choices=['NRT', 'DT', 'MY'])
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
parser.add_argument("-noupload","--no_upload", help="No upload mode, only reformat.",action="store_true")
parser.add_argument("-noreformat","--no_reformat",help="No reformat mode, only upload.",action="store_true")
parser.add_argument('-check', "--check_param", help="Check params mode.", action="store_true")
parser.add_argument("-r", "--region", help="Region.", type=str, choices=['BAL', 'MED', 'BLK', 'BS'])
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
parser.add_argument("-del", "--delete_orig", help="Delete original files after uploading them", action="store_true")
args = parser.parse_args()


def main():
    print('STARTED')
    pinfo = ProductInfo()

    if args.region:
        region = args.region
        if region == 'BS':
            region = 'BLK'

    do_multiple_datasets = False
    if args.mode and args.region and args.level and args.dataset_type and args.sensor:
        # pinfo.set_dataset_info_fromparam('MY','BAL','l3','plankton','multi')
        pinfo.set_dataset_info_fromparam(args.mode, region, args.level, args.dataset_type, args.sensor)
    elif args.mode and args.name_product and args.name_dataset:
        pinfo.set_dataset_info(args.name_product, args.name_dataset)
    elif args.mode and args.name_product and not args.name_dataset:
        pinfo.set_product_info(args.name_product)
        do_multiple_datasets = True

    if args.start_date and args.end_date and not do_multiple_datasets:
        start_date = dt.strptime(args.start_date, '%Y-%m-%d')
        end_date = dt.strptime(args.end_date, '%Y-%m-%d')
        if pinfo.dinfo['frequency'] == 'd':
            upload_daily_dataset_pinfo(pinfo, args.mode, start_date, end_date, args.verbose)
            if args.delete_orig:
                pinfo.MODE = 'REFORMAT'
                pinfo.delete_list_file_path_orig(start_date, end_date, args.verbose)
                if args.verbose:
                    print(f'[INFO] Deleting files: Completed')
        if pinfo.dinfo['frequency'] == 'm':
            upload_monthly_dataset_pinfo(pinfo, args.mode, start_date, end_date)
        if pinfo.dinfo['frequency'] == 'c':
            upload_climatology_dataset_pinfo(pinfo, args.mode, start_date, end_date)

    if args.start_date and args.end_date and do_multiple_datasets:
        start_date = dt.strptime(args.start_date, '%Y-%m-%d')
        end_date = dt.strptime(args.end_date, '%Y-%m-%d')
        for dname in pinfo.pinfo:
            pinfo_here = ProductInfo()
            pinfo_here.set_dataset_info(args.name_product, dname)
            make = True
            if args.frequency_product and args.frequency_product != pinfo_here.dinfo['frequency']:
                make = False
            if pinfo_here.dinfo['frequency'] == 'd' and make:
                upload_daily_dataset_pinfo(pinfo_here, args.mode, start_date, end_date, args.verbose)
                if args.delete_orig:
                    pinfo.MODE = 'REFORMAT'
                    pinfo.delete_list_file_path_orig(start_date, end_date, args.verbose)
                    if args.verbose:
                        print(f'[INFO] Deleting files: Completed')
            if pinfo_here.dinfo['frequency'] == 'm' and make:
                upload_monthly_dataset_pinfo(pinfo_here, args.mode, start_date, end_date)
            if pinfo_here.dinfo['frequency'] == 'c' and make:
                upload_climatology_dataset_pinfo(pinfo_here, args.mode, start_date, end_date)

    # b = pinfo.check_dataset_namesin_dict()
    # print(b)
    # pinfo.get_product_info()
    # pinfo.set_dataset_info('OCEANCOLOUR_BAL_BGC_L3_MY_009_133', "cmems_obs-oc_bal_bgc-plankton_my_l3-multi-1km_P1D")
    # upload_daily_dataset_impl(pinfo, "MY", 2020, 1, 1, 1)
    # ftpdu = FTPUpload('MY')
    # ftpdu.close()


def upload_daily_dataset(product, dataset, mode, start_date, end_date, verbose):
    pinfo = ProductInfo()
    pinfo.set_dataset_info(product, dataset)
    upload_daily_dataset_pinfo(pinfo, mode, start_date, end_date, verbose)


def upload_daily_dataset_pinfo(pinfo, mode, start_date, end_date, verbose):
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
                print(f'[INFO] Launching upload to DU for year: {year} and month: {month}')
            pinfomy = None
            if mode=='DT':
                pinfomy = pinfo.get_pinfomy_equivalent()
            if pinfomy is not None:
                if args.verbose:
                    print(f'[INFO] Upload to equivalent MY (myint) product: {pinfomy.product_name} : datataset: {pinfomy.dataset_name}')
                upload_daily_dataset_impl(pinfomy,'MY',year,month,day_ini,day_fin,verbose)
                #delete also nrt
                delete.delete_daily_dataset_impl(pinfo,'NRT',year,month,day_ini,day_fin,verbose)
            else:
                upload_daily_dataset_impl(pinfo, mode, year, month, day_ini, day_fin, verbose)


def upload_climatology_dataset_pinfo(pinfo, mode, start_date, end_date):
    month_ini = start_date.month
    month_fin = end_date.month

    for month in range(month_ini, month_fin + 1):
        day_ini = 1
        if month == month_ini:
            day_ini = start_date.day
        day_fin = monthrange(2001, month)[1]
        if month == month_fin:
            day_fin = end_date.day
        upload_climatology_dataset_impl(pinfo, mode, month, day_ini, day_fin)


def upload_monthly_dataset_pinfo(pinfo, mode, start_date, end_date, verbose):
    year_ini = start_date.year
    year_fin = end_date.year
    for year in range(year_ini, year_fin + 1):
        mini = 1
        mfin = 12
        if year == start_date.year:
            mini = start_date.month
        if year == end_date.year:
            mfin = end_date.month
        upload_monthly_dataset_impl(pinfo, mode, year, mini, mfin, verbose)


# important: pinfo is a ProductInfo object already containing the information of the product and dataset
# mode: MY, NRT, DT
def upload_daily_dataset_impl(pinfo, mode, year, month, start_day, end_day, verbose):
    ftpdu = FTPUpload(mode)
    deliveries = Deliveries()
    path_orig = pinfo.get_path_orig(year)
    rpath, sdir = pinfo.get_remote_path(year, month)
    if verbose:
        print(f'[INFO] Remote path: {rpath}/{sdir}')

    # print(path_orig)
    # print(rpath)
    # print(sdir)

    ftpdu.go_month_subdir(rpath, year, month)
    ndelivered = 0
    for day in range(start_day, end_day + 1):
        date_here = dt(year, month, day)
        if args.verbose:
            print('-------------------------')
            print(f'[INFO] Date: {date_here}')
        pfile = pinfo.get_file_path_orig_reformat(date_here)
        if pfile is None:
            pfile = pinfo.get_file_path_orig(path_orig, date_here)
        CHECK = pinfo.check_file(pfile)
        if verbose:
            print(f'[INFO] Checking origin (local) file: {pfile} --> {CHECK}')
        if not CHECK:
            print(f'[ERROR] Error with the file: {pfile}')
            continue
        remote_file_name = pinfo.get_remote_file_name(date_here)
        use_dt_suffix = goptions.use_dt_suffix()
        if mode == 'DT' and pinfo.dinfo['mode'] == 'NRT' and use_dt_suffix:
            remote_file_name = remote_file_name.replace('nrt', 'dt')

        if mode == 'MY' and pinfo.dinfo['mode'] == 'MY':
            datemyintref = dt.strptime(pinfo.dinfo['myint_date'], '%Y-%m-%d')
            if date_here >= datemyintref:
                remote_file_name = remote_file_name.replace('my', 'myint')

        status = ''
        count = 0
        if args.verbose:
            print(f'[INFO] Remote_file_name: {remote_file_name}')

        while status != 'Delivered' and count < 10:

            status, rr, start_upload_TS, stop_upload_TS = ftpdu.transfer_file(remote_file_name, pfile)
            tagged_dataset = pinfo.get_tagged_dataset()
            # tagged_dataset = os.path.join(sdir,pinfo.get_tagged_dataset())
            sdir_remote_file_name = os.path.join(sdir, remote_file_name)
            datafile_se = deliveries.add_datafile(pinfo.product_name, tagged_dataset, pfile, sdir_remote_file_name,
                                                  start_upload_TS, stop_upload_TS, status)

            if count > 0:
                deliveries.add_resend_attempt_to_datafile_se(datafile_se, rr, count)
            count = count + 1
            if status == "Delivered" and mode == 'DT' and use_dt_suffix:
                deliveries.add_delete_NRT_from_datafile_se(datafile_se)

        if status == 'Failed':
            print(f'[WARNING] {pfile} could not be uploaded to DU')
        elif status == 'Delivered':
            ndelivered = ndelivered + 1

    if ndelivered > 0:
        if args.verbose:
            print(f'[INFO] Number of files to be delivered: {ndelivered}')
        dnt_file_name, dnt_file_path = deliveries.create_dnt_file(pinfo.product_name)
        ftpdu.go_dnt(pinfo.product_name)
        status, rr, start_upload_TS, stop_upload_TS = ftpdu.transfer_file(dnt_file_name, dnt_file_path)
        if status == 'Delivered':
            if args.verbose:
                print(f'[INFO] DNT file {dnt_file_name} transfer to DU succeeded')
        else:
            print(f'[ERROR] DNT file {dnt_file_name} transfer to DU failed')
    else:
        if args.verbose:
            print(f'[INFO] No files to be delivered')

    ftpdu.close()


def upload_climatology_dataset_impl(pinfo, mode, month, start_day, end_day):
    ftpdu = FTPUpload(mode)
    deliveries = Deliveries()
    path_orig = pinfo.get_path_orig(-1)
    rpath, sdir = pinfo.get_remote_path_climatology()

    print(path_orig)
    print(rpath)
    print(sdir)

    ftpdu.go_subdir(rpath)
    ndelivered = 0
    for day in range(start_day, end_day + 1):
        date_here = dt(2000, month, day)
        pfile = pinfo.get_file_path_orig_climatology(path_orig, date_here)
        CHECK = pinfo.check_file(pfile)
        print(f'[INFO] Checking file: {pfile} Exist: {os.path.exists(pfile)} Check: {CHECK}')
        if not CHECK:
            print(f'[ERROR] Error with the file: {pfile}')
            continue
        remote_file_name = pinfo.get_remote_file_name_climatology(date_here)
        print(remote_file_name)
        status = ''
        count = 0
        print(pfile)
        print(remote_file_name)

        while status != 'Delivered' and count < 10:
            status, rr, start_upload_TS, stop_upload_TS = ftpdu.transfer_file(remote_file_name, pfile)
            tagged_dataset = pinfo.get_tagged_dataset()
            # tagged_dataset = os.path.join(sdir,pinfo.get_tagged_dataset())
            sdir_remote_file_name = os.path.join(sdir, remote_file_name)
            datafile_se = deliveries.add_datafile(pinfo.product_name, tagged_dataset, pfile, sdir_remote_file_name,
                                                  start_upload_TS, stop_upload_TS, status)
            if count > 0:
                deliveries.add_resend_attempt_to_datafile_se(datafile_se, rr, count)
            count = count + 1
            if status == "Delivered" and mode == 'DT':
                deliveries.add_delete_NRT_from_datafile_se(datafile_se)

        if status == 'Failed':
            print(f'[WARNING] {pfile} could not be uploaded to DU')
        elif status == 'Delivered':
            ndelivered = ndelivered + 1

    if ndelivered > 0:
        dnt_file_name, dnt_file_path = deliveries.create_dnt_file(pinfo.product_name)
        ftpdu.go_dnt(pinfo.product_name)
        status, rr, start_upload_TS, stop_upload_TS = ftpdu.transfer_file(dnt_file_name, dnt_file_path)
        if status == 'Delivered':
            print(f'DNT file {dnt_file_name} transfer to DU succeeded')
        else:
            print(f'DNT file {dnt_file_name} transfer to DU failed')

    ftpdu.close()


def upload_monthly_dataset_impl(pinfo, mode, year, start_month, end_month, verbose):
    ftpdu = FTPUpload(mode)
    deliveries = Deliveries()
    path_orig = pinfo.get_path_orig(year)
    rpath, sdir = pinfo.get_remote_path_monthly(year)

    if verbose:
        print(f'[INFO] Remote path: {rpath}/{sdir}')

    ftpdu.go_year_subdir(rpath, year)
    ndelivered = 0
    for month in range(start_month, end_month + 1):
        date_here = dt(year, month, 15)
        if args.verbose:
            print('-------------------------')
            print(f'[INFO] Date: {date_here}')
        pfile = pinfo.get_file_path_orig_monthly(path_orig, date_here)
        CHECK = pinfo.check_file(pfile)
        if verbose:
            print(f'[INFO] Checking origin (local) file: {pfile} --> {CHECK}')

        if not CHECK:
            print(f'[ERROR] Error with the file: {pfile}')
            continue
        remote_file_name = pinfo.get_remote_file_name_monthly(date_here)
        use_dt_suffix = goptions.use_dt_suffix()
        if mode == 'DT' and pinfo.dinfo['mode'] == 'NRT' and use_dt_suffix:
            remote_file_name = remote_file_name.replace('nrt', 'dt')

        if mode == 'MY' and pinfo.dinfo['mode'] == 'MY':
            datemyintref = dt.strptime(pinfo.dinfo['myint_date'], '%Y-%m-%d')
            if date_here >= datemyintref:
                remote_file_name = remote_file_name.replace('my', 'myint')

        status = ''
        count = 0
        if args.verbose:
            print(f'[INFO] Remote_file_name: {remote_file_name}')

        while status != 'Delivered' and count < 10:
            status, rr, start_upload_TS, stop_upload_TS = ftpdu.transfer_file(remote_file_name, pfile)
            tagged_dataset = pinfo.get_tagged_dataset()
            # tagged_dataset = os.path.join(sdir,pinfo.get_tagged_dataset())
            sdir_remote_file_name = os.path.join(sdir, remote_file_name)
            datafile_se = deliveries.add_datafile(pinfo.product_name, tagged_dataset, pfile, sdir_remote_file_name,
                                                  start_upload_TS, stop_upload_TS, status)
            if count > 0:
                deliveries.add_resend_attempt_to_datafile_se(datafile_se, rr, count)
            count = count + 1
            if status == "Delivered" and mode == 'DT' and use_dt_suffix:
                deliveries.add_delete_NRT_from_datafile_se(datafile_se)

        if status == 'Failed':
            print(f'[WARNING] {pfile} could not be uploaded to DU')
        elif status == 'Delivered':
            ndelivered = ndelivered + 1

    if ndelivered > 0:
        if args.verbose:
            print(f'[INFO] Number of files to be delivered: {ndelivered}')
        dnt_file_name, dnt_file_path = deliveries.create_dnt_file(pinfo.product_name)
        ftpdu.go_dnt(pinfo.product_name)
        status, rr, start_upload_TS, stop_upload_TS = ftpdu.transfer_file(dnt_file_name, dnt_file_path)
        if status == 'Delivered':
            if args.verbose:
                print(f'[INFO] DNT file {dnt_file_name} transfer to DU succeeded')
        else:
            print(f'[ERROR] DNT file {dnt_file_name} transfer to DU failed')
    else:
        if args.verbose:
            print(f'[INFO] No files to be delivered')

    ftpdu.close()


class FTPUpload():
    def __init__(self, mode):
        sdir = os.path.abspath(os.path.dirname(__file__))
        # path2script = "/".join(sdir.split("/")[0:-1])
        path2script = os.path.dirname(sdir)
        credentials = RawConfigParser()
        credentials.read(os.path.join(path2script, 'credentials.ini'))
        if mode == 'MY':
            du_server = "my.cmems-du.eu"
        elif mode == 'NRT' or mode == 'DT':
            du_server = "nrt.cmems-du.eu"
        du_uname = credentials.get('cnr', 'uname')
        du_passwd = credentials.get('cnr', 'passwd')
        self.ftpdu = FTP(du_server, du_uname, du_passwd)

    def go_subdir(self, rpath):
        # print('Changing directory to: ', rpath)
        self.ftpdu.cwd(rpath)

    def go_year_subdir(self, rpath, year):
        dateref = dt(year, 1, 1)
        yearstr = dateref.strftime('%Y')
        # print('Changing directory to: ', rpath)
        self.ftpdu.cwd(rpath)
        if not (yearstr in self.ftpdu.nlst()):
            self.ftpdu.mkd(yearstr)
        self.ftpdu.cwd(yearstr)

    def go_month_subdir(self, rpath, year, month):
        dateref = dt(year, month, 1)
        yearstr = dateref.strftime('%Y')  # dt.strptime(str(year), '%Y')
        monthstr = dateref.strftime('%m')  # dt.strptime(month, '%m')

        self.ftpdu.cwd(rpath)

        if not (yearstr in self.ftpdu.nlst()):
            self.ftpdu.mkd(yearstr)
        self.ftpdu.cwd(yearstr)

        if not (monthstr in self.ftpdu.nlst()):
            self.ftpdu.mkd(monthstr)
        self.ftpdu.cwd(monthstr)

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

    def close(self):
        self.ftpdu.close()


class Deliveries():
    def __init__(self):
        self.deliveries = dict()
        self.dnt_file_name = None
        sdir = os.path.abspath(os.path.dirname(__file__))
        # path2script = "/".join(sdir.split("/")[0:-1])
        path2script = os.path.dirname(sdir)
        self.XMLPath = os.path.join(path2script, 'XML')

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
