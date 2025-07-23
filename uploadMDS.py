from datetime import timedelta
from datetime import datetime as dt
from calendar import monthrange
from ftplib import FTP
from configparser import RawConfigParser
import os, hashlib
import lxml.etree as ET
from deleteMDS import DeleteMDS

class UploadMDS:

    def __init__(self, version,verbose):
        #self.mode = 'REFORMAT'
        self.verbose = verbose

    # def make_upload_daily(self,mode, pinfo, start_date, end_date):
    #     if self.verbose:
    #         print(f'[INFO] Uploading files to DU: Started')
    #     pinfo.MODE = 'UPLOAD'
    #     #delete_nrt = False
    #
    #     # if pinfomy is not None:
    #     #     if self.verbose:
    #     #         print(f'[INFO] Using equivalent MY product: {pinfomy.product_name};dataset:{pinfomy.dataset_name}')
    #     #     pinfomy.MODE = 'UPLOAD'
    #     #     self.upload_daily_dataset_pinfo(pinfomy, 'MY', start_date, end_date)
    #     #     #delete_nrt = True
    #     # else:
    #     self.upload_daily_dataset_pinfo(pinfo, mode, start_date, end_date)
    #
    #     # delete nrt if neeed
    #     # if delete_nrt:
    #     #     start_date_nrt = start_date - timedelta(days=1)
    #     #     end_date_nrt = end_date - timedelta(days=1)
    #     #     self.delete_nrt_daily_dataset(pinfo, start_date_nrt, end_date_nrt)
    #
    #     if self.verbose:
    #         print(f'[INFO] Uploading files to DU: Completed')

    #mode: NRT, DT or MY
    def make_upload_daily(self,mode,pinfo,start_date,end_date):
        if self.verbose:
            print(f'[INFO] Started uploading of daily files to the MDS  with mode {mode}')
            print(f'[INFO] {pinfo.product_name}/{pinfo.dataset_name}')
        delete_nrt = False
        year_ini = start_date.year
        year_fin = end_date.year
        pinfomy = None
        if mode == 'DT':
            pinfomy = pinfo.get_pinfomy_equivalent()
            if self.verbose:
                print(f'[INFO] DT Mode: using equivalent MY product/dataset {pinfomy.product_name}/{pinfomy.dataset_name}')

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
                if self.verbose:
                    print(f'[INFO] Launching upload to MDS from {year}/{month}/{day_ini} to {year}/{month}/{day_fin}')

                if pinfomy is not None:##i.e. mode==DT
                    self.upload_daily_dataset_impl(pinfomy, 'MY', year, month, day_ini, day_fin)
                    delete_nrt = True
                else:
                    self.upload_daily_dataset_impl(pinfo, mode, year, month, day_ini, day_fin)
        if delete_nrt:
            start_date_nrt = start_date - timedelta(days=1)
            end_date_nrt = end_date - timedelta(days=1)
            self.delete_nrt_daily_dataset(pinfo, start_date_nrt, end_date_nrt)
        if self.verbose:
            print(f'[INFO] Uploading files to DU: Completed')

    #important: pinfo is a ProductInfo object already containing the information of the product and dataset
    # mode: MY o NRT, (DT)
    def upload_daily_dataset_impl(self,pinfo, mode, year, month, start_day, end_day):
        ftpdu = FTPUpload(mode,True)
        deliveries = Deliveries()
        path_orig = pinfo.get_path_orig(year)
        rpath, sdir = pinfo.get_remote_path(year, month)
        if self.verbose:
            print(f'[INFO] Remote path: {rpath}/{sdir}')

        ftpdu.go_month_subdir(rpath, year, month)
        ndelivered = 0
        for day in range(start_day, end_day + 1):
            date_here = dt(year, month, day)
            if self.verbose:
                print(f'[INFO] Date: {date_here}')
            pfile = pinfo.get_file_path_orig_reformat(date_here)
            if pfile is None:
                pfile = pinfo.get_file_path_orig(path_orig, date_here)
            if self.verbose:
                print(f'[INFO] File: {pfile}')
            CHECK = pinfo.check_file(pfile)
            if self.verbose:
                print(f'[INFO] Checking origin (local) file: {pfile} --> {CHECK}')
            if not CHECK:
                print(f'[ERROR] Error with the file: {pfile}')
                continue
            remote_file_name = pinfo.get_remote_file_name(date_here)

            if mode == 'MY' and pinfo.dinfo['mode'] == 'MY':
                datemyintref = dt.strptime(pinfo.dinfo['myint_date'], '%Y-%m-%d')
                if date_here >= datemyintref:
                    remote_file_name = remote_file_name.replace('my', 'myint')

            status = ''
            count = 0
            if self.verbose:
                print(f'[INFO] Remote_file_name: {remote_file_name}')

            while status != 'Delivered' and count < 10:

                status, rr, start_upload_TS, stop_upload_TS = ftpdu.transfer_file(remote_file_name, pfile)
                tagged_dataset = pinfo.get_tagged_dataset()
                sdir_remote_file_name = os.path.join(sdir, remote_file_name)
                datafile_se = deliveries.add_datafile(pinfo.product_name, tagged_dataset, pfile, sdir_remote_file_name,
                                                  start_upload_TS, stop_upload_TS, status)

                if count > 0:
                    deliveries.add_resend_attempt_to_datafile_se(datafile_se, rr, count)
                count = count + 1

            if status == 'Failed':
                print(f'[WARNING] {pfile} could not be uploaded to the MDS')
            elif status == 'Delivered':
                ndelivered = ndelivered + 1

        if ndelivered > 0:
            if self.verbose:
                print(f'[INFO] Number of files to be delivered: {ndelivered}')
            dnt_file_name, dnt_file_path = deliveries.create_dnt_file(pinfo.product_name)
            ftpdu.go_dnt(pinfo.product_name)
            status, rr, start_upload_TS, stop_upload_TS = ftpdu.transfer_file(dnt_file_name, dnt_file_path)
            if status == 'Delivered':
                if self.verbose:
                    print(f'[INFO] DNT file {dnt_file_name}. Transfer to MDS succeeded')
            else:
                print(f'[ERROR] DNT file {dnt_file_name}. Transfer to DU failed')
        else:
            if self.verbose:
                print(f'[INFO] No files to be delivered')

        ftpdu.close()

    def delete_nrt_daily_dataset(self,pinfo, start_date, end_date):
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
                # delete also nrt
                delete = DeleteMDS(self.verbose)
                delete.delete_daily_dataset_impl(pinfo, 'NRT', year, month, day_ini, day_fin)


class FTPUpload():
    def __init__(self, mode):
        sdir = os.path.abspath(os.path.dirname(__file__))
        # path2script = "/".join(sdir.split("/")[0:-1])
        path2script = os.path.dirname(sdir)
        credentials = RawConfigParser()
        credentials.read(os.path.join(path2script, 'credentials.ini'))
        if mode == 'MY':
            du_server = 'ftp-my.marine.copernicus.eu'
        elif mode == 'NRT' or mode == 'DT':
            du_server = "ftp-nrt.marine.copernicus.eu"

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

            ET.SubElement(datafile_se, "KeyWord").text = 'Delete'

        return datafile_se

    def md5(self, fname):
        hash_md5 = hashlib.md5()
        with open(fname, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()