import pytz,os

import uploadMDS as uMDS
from s3buckect import S3Bucket
from datetime import datetime as dt

class DeleteMDS:

    def __init__(self, version,verbose):
        self.verbose = verbose

    def delete_daily_dataset_impl(self,pinfo, mode, year, month, start_day, end_day):
        ftpdu = uMDS.FTPUpload(mode)

        sb = S3Bucket()
        sb.update_params_from_pinfo(pinfo)
        conn = sb.star_client()
        if not conn:
            print(f'[ERROR] Connection error with s3 bucket. Daily dataset could not be deleted')
            return
        deliveries = uMDS.Deliveries()
        rpath, sdir = pinfo.get_remote_path(year, month)
        if self.verbose:
            print(f'[INFO] Remote path: {rpath}/{sdir}')
        ftpdu.go_month_subdir(rpath, year, month)

        for day in range(start_day, end_day + 1):
            date_here = dt(year, month, day)
            if self.verbose:
                print(f'[INFO] Date: {date_here}')
            remote_file_name = pinfo.get_remote_file_name(date_here)

            if mode == 'MY'  and pinfo.dinfo['mode'] == 'MY':
                datemyintref = dt.strptime(pinfo.dinfo['myint_date'], '%Y-%m-%d')
                if date_here >= datemyintref:
                    remote_file_name = remote_file_name.replace('my', 'myint')

            if self.verbose:
                print(f'[INFO] Remote_file_name: {remote_file_name}')



            s3bname, key, isuploaded = sb.check_daily_file(mode, pinfo, date_here, False)
            if not isuploaded:
                print(f'[INFO] Expected file name to be deleted {remote_file_name} does not exist.')
                continue

            sdir_remote_file_name = os.path.join(sdir, remote_file_name)
            tagged_dataset = pinfo.get_tagged_dataset()
            upload_TS = dt.now(pytz.UTC).strftime('%Y%m%dT%H%M%SZ')
            deliveries.add_delete(pinfo.product_name, tagged_dataset, sdir_remote_file_name, upload_TS)
            dnt_file_name, dnt_file_path = deliveries.create_dnt_file(pinfo.product_name)
            ftpdu.go_dnt(pinfo.product_name)
            status, rr, start_upload_TS, stop_upload_TS = ftpdu.transfer_file(dnt_file_name, dnt_file_path)
            if status == 'Delivered':
                if self.verbose:
                    print(f'[INFO] DNT file {dnt_file_name} transfer to DU succeeded')
            else:
                print(f'[ERROR] DNT file {dnt_file_name} transfer to DU failed')

        ftpdu.close()
        sb.close_client()
