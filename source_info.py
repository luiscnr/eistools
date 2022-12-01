from datetime import datetime as dt
import subprocess
import os
import json


class SourceInfo():
    def __init__(self, eis):
        sdir = os.path.abspath(os.path.dirname(__file__))
        self.path2info = os.path.join(os.path.dirname(sdir), 'PRODUCT_INFO')
        if self.path2info == '/home/lois/PycharmProjects/PRODUCT_INFO':
            self.path2info = '/mnt/c/DATA_LUIS/OCTAC_WORK/EiSJuly2022/PRODUCT_INFO'
        self.eis = eis
        self.source = None
        self.dsource = {}
        self.sessionid = None

        self.sessionid_list = []

    def start_source(self, source):
        self.dsource = {}
        self.source = None
        fsources = os.path.join(self.path2info, 'SOURCES.json')
        if os.path.exists(fsources):
            f = open(fsources, "r")
            sourcesinfo = json.load(f)

            try:
                self.source = source
                self.dsource = sourcesinfo[source]
            except:
                print(f'[ERROR] Source {source} was not correctly parsed from sources file {fsources}')
            f.close()
        else:
            print(f'[ERROR] Source file {fsources} does not exit')

    def get_last_session_id(self, mode, region, date):
        if mode=='NRT':
            proc_folder = self.dsource["proc_folder_nrt"]
        if mode=='DT':
            proc_folder = self.dsource["proc_folder_dt"]
        self.sessionid = None
        if self.source is None:
            return None
        if region == 'BLK':
            region = 'BS'
        path_search = f'/home/gosuser/Processing/{proc_folder}/sessions'
        datestr = date.strftime('%Y%m%d')
        source_str = self.source
        if self.source == 'OLCI' or self.source == 'S3A_FR' or self.source == 'S3B_FR':
            if self.source == 'OLCI':
                source_str = 's3olci_S3A_RR'
            else:
                source_str = f's3olci_{self.source}'
            if mode == 'NRT':
                mode = 'NR'
            if mode == 'DT':
                mode = 'NT'
            path_search = '/store/TDIR/'

        if self.source == 'OLCIP':
            source_str = 'OLCIA'

        if self.source == 'S3A_FR' or self.source == 'S3B_FR' or self.source == 'OLCI':
            prename = f'OC_PROC_EIS{self.eis}_{source_str}_{mode}_{region}_{datestr}'
        else:
            prename = f'{proc_folder}_{source_str}_{mode}_{region}_{datestr}'
        self.sessionid = self.search_session_id_inlist(prename)
        if self.sessionid is not None:
            return
        cmd = f'find {path_search} -name {prename}* -type d > list.temp'



        prog = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE)
        out, err = prog.communicate()
        if err:
            print(f'[ERROR]{err}')
            return
        wref = 0
        file_r = open('list.temp', 'r')
        for line in file_r:
            path = line.strip()
            if os.path.exists(path) and os.path.isdir(path):
                wrefhere_str = os.path.basename(path).split('_')[-1]
                wrefhere = float(wrefhere_str)
                if wrefhere >= wref:
                    wref = wrefhere
                    self.sessionid = os.path.basename(path)
                    self.sessionid_list.append(self.sessionid)
        file_r.close()
        os.remove('list.temp')

    def search_session_id_inlist(self, prename):
        if len(self.sessionid_list) == 0:
            return None
        for s in self.sessionid_list:
            if s.startswith(prename):
                return s

    def get_session_folder(self,mode):
        if self.sessionid is None:
            return None
        if mode=='NRT':
            proc_folder = self.dsource["proc_folder_nrt"]
        if mode=='DT':
            proc_folder = self.dsource["proc_folder_dt"]
        #session_folder = f'/home/gosuser/Processing/OC_PROC_EIS{self.eis}/sessions/{self.sessionid}'
        session_folder = f'/home/gosuser/Processing/{proc_folder}/sessions/{self.sessionid}'
        if os.path.exists(session_folder) and os.path.isdir(session_folder):
            return session_folder
        else:
            return None

    def get_processing_folder(self):
        if self.sessionid is None:
            return None
        tdir_folder = f'/EO_DATA/TDIR/{self.sessionid}'
        if os.path.exists(tdir_folder) and os.path.isdir(tdir_folder):
            return tdir_folder
        else:
            return None

    def get_log_file(self,mode):
        if self.sessionid is None:
            return None
        if mode=='NRT':
            proc_folder = self.dsource["proc_folder_nrt"]
        if mode=='DT':
            proc_folder = self.dsource["proc_folder_dt"]
        log_file = f'/home/gosuser/Processing/{proc_folder}/log/{self.sessionid}.log'
        if os.path.exists(log_file):
            return log_file
        else:
            return None

    def check_source(self, source, mode, region, date):
        valid_source = False
        lines_source = [f'Source: {source}']
        self.start_source(source)
        if self.source is None:
            lines_source.append(f'[WARNING] Source: {source} was not identified in sources config file')
            return lines_source, valid_source
        self.get_last_session_id(mode, region, date)
        if self.sessionid is None:
            lines_source.append(f'[WARNING] Session id for: {source} {mode} {region} {date} could not be found')
            return lines_source, valid_source

        if source == 'OLCI':
            session_folder = None
            proccessing_folder = self.get_processing_folder()
            if proccessing_folder is not None and os.path.exists(proccessing_folder):
                jdate = date.strftime('%Y%j')
                log_file = os.path.join(proccessing_folder, f'{jdate}{self.sessionid[17:]}.log')
                if not os.path.exists(log_file):
                    log_file = None
        elif source == 'S3A_FR' or source == 'S3B_FR':
            session_folder = None
            proccessing_folder = self.get_processing_folder()
            if proccessing_folder is not None and os.path.exists(proccessing_folder):
                jdate = date.strftime('%Y%j')
                log_file = os.path.join(proccessing_folder, f'{jdate}{self.sessionid[17:]}.log')
                if not os.path.exists(log_file):
                    log_file = None
        else:
            session_folder = self.get_session_folder(mode)
            proccessing_folder = self.get_processing_folder()
            log_file = self.get_log_file(mode)
        lines_source.append(f' Session ID: {self.sessionid}')
        lines_source.append(f' Session folder: {session_folder}')
        lines_source.append(f' Proccessing folder: {proccessing_folder}')
        lines_source.append(f' Log file: {log_file}')

        if self.dsource["agency"] == "NASA":
            lines_source, valid_source = self.check_source_NASA(lines_source, mode)
        elif source == "OLCI" or source == 'S3A_FR' or source == 'S3B_FR':
            lines_source, valid_source = self.check_source_OLCI(date, lines_source)
        else:
            lines_source.append(' Status: no implemented')
            valid_source = True

        return lines_source, valid_source

    # implementation of check_source, session id is already defined
    def check_source_NASA(self, lines_source, mode):
        valid_sources = True

        session_folder = self.get_session_folder(mode)
        proccessing_folder = self.get_processing_folder()

        fsource_list = os.path.join(session_folder, 'source_files.list')
        if not os.path.exists(fsource_list):
            valid_sources = False
            lines_source.append(f' Status: FAIL Source files for {self.source} are not available')
            return lines_source, valid_sources

        fs = open(fsource_list, 'r')
        ntot = 0
        ngood = 0
        notexisting = []
        for fname in fs:
            if len(fname) == 0:
                continue
            ntot = ntot + 1
            file_s = os.path.join(proccessing_folder, fname.strip())
            if os.path.exists(file_s):
                ngood = ngood + 1
            else:
                notexisting.append(fname.strip())
        fs.close()

        lines_source.append(f' Available source files: {ngood}/{ntot}')
        if ngood < ntot:
            valid_sources = False
            for fname in notexisting:
                lines_source.append(f'  Source file not found: {fname}')
        if valid_sources:
            lines_source.append(f' Status: OK')
        else:
            lines_source.append(f' Status: FAIL')

        return lines_source, valid_sources

    def check_source_OLCI(self, date, lines_source):
        valid_sources = True
        proccessing_folder = self.get_processing_folder()
        mail_file = os.path.join(proccessing_folder, 'final_mail.txt')
        if not os.path.exists(mail_file):
            lines_source.append(f' Expected file {mail_file} was not found. Error downloading OLCI files')
            lines_source.append(f' Status: FAIL')
            valid_sources = False
            return lines_source, valid_sources
        fmail = open(mail_file, 'r')
        dfiles = []
        tfiles = []
        fpfiles = []
        start_dfiles = False
        start_tfiles = False
        start_fpfiles = False
        for line in fmail:
            if line.startswith('Downloaded files:'):
                start_dfiles = True
                continue
            if line.startswith('Trimmed files:'):
                start_tfiles = True
                start_dfiles = False
                continue
            if line.startswith('Final Products:'):
                start_fpfiles = True
                start_tfiles = False
                continue
            if start_dfiles and len(line.strip()) > 0:
                dfiles.append(line.strip())
            if start_tfiles and len(line.strip()) > 0:
                tfiles.append(line.strip())
            if start_fpfiles and len(line.strip()) > 0:
                fpfiles.append(line.strip())
        fmail.close()
        n_dfiles = 0
        n_tfiles = 0
        n_fpfiles = 0

        path = self.dsource["source_dir_orig"]
        path = os.path.join(path, date.strftime('%Y'), date.strftime('%j'))
        for name in dfiles:
            f = os.path.join(path, name)
            if os.path.exists(f):
                n_dfiles = n_dfiles + 1
        path = self.dsource["source_dir_trim"]
        path = os.path.join(path, date.strftime('%Y'), date.strftime('%j'))
        for name in tfiles:
            f = os.path.join(path, name)
            if os.path.exists(f):
                n_tfiles = n_tfiles + 1

        for name in fpfiles:
            f = os.path.join(proccessing_folder, name)
            if os.path.exists(f):
                n_fpfiles = n_fpfiles + 1

        if n_dfiles == len(dfiles) and n_tfiles == len(tfiles) and n_fpfiles == len(fpfiles):
            valid_sources = True

        lines_source.append(f' Downloaded files: {n_dfiles}/{len(dfiles)}')
        lines_source.append(f' Trimmed files: {n_tfiles}/{len(tfiles)}')
        lines_source.append(f' Final products: {n_fpfiles}/{len(fpfiles)}')

        if valid_sources:
            lines_source.append(f' Status: OK')
        else:
            lines_source.append(f' Status: FAIL')

        return lines_source, valid_sources

    def get_cmd_nrt(self):
        return self.dsource["cmd_nrt"]

    def get_processing_cmd_nrt(self):
        return self.dsource["cmd_proc_nrt"]

    def get_cmd_dt(self):
        return self.dsource["cmd_dt"]

    def get_processing_cmd_dt(self):
        return self.dsource["cmd_proc_dt"]

    def get_processed_files(self):
        sfiles = self.dsource["processed_files"]
        sfiles_list = sfiles.split(',')
        return sfiles_list
