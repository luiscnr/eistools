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
        self.sessionid = None
        if self.source is None:
            return None
        if region == 'BLK':
            region = 'BS'
        path_search = f'/home/gosuser/Processing/OC_PROC_EIS{self.eis}/sessions'
        datestr = date.strftime('%Y%m%d')
        prename = f'OC_PROC_EIS{self.eis}_{self.source}_{mode}_{region}_{datestr}'
        cmd = f'find {path_search} -name {prename}* -type d > list.temp'
        prog = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE)
        out, err = prog.communicate()
        if err:
            print(f'[ERROR]{err}')
            return None
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
        file_r.close()
        os.remove('list.temp')

    def get_session_folder(self):
        if self.sessionid is None:
            return None
        session_folder = f'/home/gosuser/Processing/OC_PROC_EIS{self.eis}/sessions/{self.sessionid}'
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

    def get_log_file(self):
        if self.sessionid is None:
            return None
        log_file = f'/home/gosuser/Processing/OC_PROC_EIS{self.eis}/log/{self.sessionid}.log'
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
            return valid_source, lines_source
        self.get_last_session_id(mode, region, date)
        if self.sessionid is None:
            lines_source.append(f'[WARNING] Session id for: {source} {mode} {region} {date} could not be found')
            return valid_source, lines_source

        session_folder = self.get_session_folder()
        proccessing_folder = self.get_processing_folder()
        log_file = self.get_log_file()
        lines_source.append(f' Session ID: {self.sessionid}')
        lines_source.append(f' Session folder: {session_folder}')
        lines_source.append(f' Proccessing folder: {proccessing_folder}')
        lines_source.append(f' Log file: {log_file}')

        if self.dsource["agency"] == "NASA":
            lines_source, valid_source = self.check_source_NASA(lines_source)
        else:
            lines_source.append(' Status: no implemented')
            valid_source = True

        return lines_source, valid_source

    # implementation of check_source, session id is already defined
    def check_source_NASA(self,lines_source):
        valid_sources = True

        session_folder = self.get_session_folder()
        proccessing_folder = self.get_processing_folder()

        fsource_list = os.path.join(session_folder, 'source_files.list')
        if not os.path.exists(fsource_list):
            valid_sources = False
            lines_source.append(f' Status: FAIL Source files for {self.source} are not available')
            return lines_source,valid_sources

        fs = open(fsource_list,'r')
        ntot = 0
        ngood = 0
        notexisting = []
        for fname in fs:
            if len(fname)==0:
                continue
            ntot = ntot +1
            file_s = os.path.join(proccessing_folder,fname.strip())
            if os.path.exists(file_s):
                ngood = ngood +1
            else:
                notexisting.append(fname.strip())
        fs.close()

        lines_source.append(f' Available source files: {ngood}/{ntot}')
        if ngood<ntot:
            valid_sources = False
            for fname in notexisting:
                lines_source.append(f'  Source file not found: {fname}')
        if valid_sources:
            lines_source.append(f' Status: OK')
        else:
            lines_source.append(f' Status: FAIL')

        return lines_source, valid_sources

    def get_cmd(self):
        return self.dsource["cmd"]
