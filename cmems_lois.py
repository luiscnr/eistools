import os
from datetime import timedelta

class CMEMS_LOIS:

    def __init__(self, verbose):
        self.verbose = verbose

    def make_cmems_download(self,cmems_options, make_download, output_directory, ods):
        from s3buckect import S3Bucket
        sb = S3Bucket()
        sb.star_client()
        sb.update_params_from_dict(cmems_options)
        work_date = cmems_options['start_date']
        end_date = cmems_options['end_date']
        while work_date <= end_date:
            bucket, key, available = sb.check_daily_file_params(work_date)
            if self.verbose or not make_download:
                print(f'[INFO]{work_date.strftime("%Y-%m-%d")}:{bucket}/{key}->{available}')
            if make_download and available:
                folder_out = self.get_folder_out(work_date, output_directory, ods)
                if folder_out is not None:
                    file_out, isdownloaded = sb.download_daily_file_params(work_date, folder_out, False, args.overwrite)
                    if self.verbose:
                        print(f'--> Output file: {file_out} Download status: {os.path.exists(file_out)}')

            work_date = work_date + timedelta(hours=24)
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