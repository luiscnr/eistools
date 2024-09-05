import os
from datetime import timedelta
from datetime import datetime as dt

class CMEMS_LOIS:

    def __init__(self, verbose):
        self.verbose = verbose


    def check_last_file(self,cmems_options):
        from s3buckect import S3Bucket
        sb = S3Bucket()
        b = sb.star_client()
        if not b:
            print(f'[WARNING] Client S3 could not be started')
            return
        sb.update_params_from_dict(cmems_options)
        date_here = dt.now()
        date_last = dt(2000,1,1)
        while date_here>=date_last:
            bucket, key, available, usemyint = sb.check_daily_file_params(date_here)
            if available:
                date_last_file = date_here
                break
            date_here = date_here-timedelta(hours=24)

        return date_last_file

    def make_cmems_download(self,cmems_options, make_download, output_directory, ods, overwrite):
        from s3buckect import S3Bucket
        sb = S3Bucket()
        b = sb.star_client()
        if not b:
            print(f'[WARNING] Client S3 could not be started')
            return
        sb.update_params_from_dict(cmems_options)
        date_list = []
        if cmems_options['date_list'] is not None:
            date_list = cmems_options['date_list']
        else:
            work_date = cmems_options['start_date']
            end_date = cmems_options['end_date']
            while work_date <= end_date:
                date_list.append(work_date)
                work_date = work_date + timedelta(hours=24)

        for work_date in date_list:
            bucket, key, available, usemyint = sb.check_daily_file_params(work_date)
            if self.verbose or not make_download:
                print(f'[INFO]{work_date.strftime("%Y-%m-%d")}:{bucket}/{key}->{available}')
            if make_download and available:
                folder_out = self.get_folder_out(work_date, output_directory, ods)
                if folder_out is not None:
                    if usemyint:
                        file_out, isdownloaded = sb.download_daily_file_params_myint(work_date, folder_out, False, overwrite)
                    else:
                        file_out, isdownloaded = sb.download_daily_file_params(work_date, folder_out, False, overwrite)
                    if self.verbose:
                        print(f'--> Output file: {file_out} Download status: {os.path.exists(file_out)}')
        sb.close_client()

    def get_folder_out(self,work_date, od, ods):
        if ods is None:
            return od
        ods_parts = ods.split('/')
        for part in ods_parts:
            dir_date_name = work_date.strftime(part)
            od = os.path.join(od, dir_date_name)
            if not self.create_if_not_exists(od):
                return None
        return od

    def create_if_not_exists(self,folder):
        if not os.path.exists(folder):
            try:
                os.mkdir(folder)
                os.chmod(folder, 0o775)
            except:
                return False

        return True

    def test(self):
        print('TESTING MODE...')