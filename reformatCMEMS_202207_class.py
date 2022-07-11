from datetime import timedelta
from datetime import datetime as dt
import subprocess


class ReformatCMEMS:
    def __init__(self):
        self.mode = 'REFORMAT'

    def make_reformat_daily_dataset(self, pinfo, start_date, end_date, verbose):
        date_work = start_date
        while date_work <= end_date:
            if verbose:
                print('----------------------------------------------------')
                print(f'[INFO] Reformating file for date: {date_work}')
            cmd = pinfo.get_reformat_cmd(date_work)
            if cmd is None:
                date_work = date_work + timedelta(hours=24)
                continue
            if verbose:
                print(f'[INFO] CMD: {cmd}')
            date_work = date_work + timedelta(hours=24)
            prog = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            out, err = prog.communicate()
            if out:
                outstr = out.decode(("utf-8"))
                ierror = outstr.find('ERROR')
                if ierror >= 0:
                    print(f'[CMD ERROR] {outstr[ierror:]}')
            if err:
                print(f'[CMD ERROR]{err}')

    def make_reformat_monthly_dataset(pinfo, start_date, end_date, verbose):
        year_ini = start_date.year
        year_fin = end_date.year
        for year in range(year_ini, year_fin + 1):
            mini = 1
            mfin = 12
            if year == start_date.year:
                mini = start_date.month
            if year == end_date.year:
                mfin = end_date.month
            for month in range(mini, mfin + 1):
                date_here = dt(year, month, 15)
                if verbose:
                    print('----------------------------------------------------')
                    print(f'[INFO] Reformating file for year: {year} month: {month}')
                cmd = pinfo.get_reformat_cmd(date_here)
                if verbose:
                    print(f'[INFO] CMD: {cmd}')
                prog = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE)
                out, err = prog.communicate()
                if out:
                    outstr = out.decode(("utf-8"))
                    ierror = outstr.find('ERROR')
                    if ierror >= 0:
                        print(f'[CMD ERROR] {outstr[ierror:]}')
                if err:
                    print(f'[CMD ERROR]{err}')
