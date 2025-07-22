import os, shutil, subprocess
from datetime import timedelta

class ReformatCMEMS:

    def __init__(self, version,verbose):
        #self.mode = 'REFORMAT'
        self.verbose = verbose
        self.version = version if version is not None else '202411'
        self.use_sh = False
        self.launch_script = True

    def make_reformat_daily_dataset(self,pinfo, start_date, end_date, verbose):
        date_work = start_date
        while date_work <= end_date:
            if verbose:
                print('----------------------------------------------------')
                print(f'[INFO] Reformating file for date: {date_work}')

            cmd = None
            if self.version == '202207':
                cmd = pinfo.get_reformat_cmd(date_work)
            elif self.version == '202411':
                cmd = pinfo.get_reformat_cmd_202411(date_work,self.use_sh)

            if cmd is None:
                date_work = date_work + timedelta(hours=24)
                continue
            if verbose:
                print(f'[INFO] CMD: {cmd}')

            if not self.launch_script:
                date_work = date_work + timedelta(hours=24)
                continue

            prog = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            out, err = prog.communicate()
            if out:
                outstr = out.decode(("utf-8"))
                ierror = outstr.find('ERROR')
                if ierror >= 0:
                    print(f'[CMD ERROR] {outstr[ierror:]}')
            if err:
                print(f'[CMD ERROR]{err}')

            preformat = pinfo.check_path_reformat()
            if preformat is not None:
                file_orig = pinfo.get_file_path_orig(None, date_work)
                file_dest = pinfo.get_file_path_orig_reformat_name(date_work)
                if os.path.exists(file_orig) and file_dest is not None:
                    if verbose:
                        print(f'[INFO] Moving reformated file {file_orig} to path reformat {file_dest}')
                    shutil.copy2(file_orig, file_dest)
                    os.remove(file_orig)

            date_work = date_work + timedelta(hours=24)
