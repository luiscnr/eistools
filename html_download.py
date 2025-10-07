import os, requests
from datetime import datetime as dt
from requests.adapters import HTTPAdapter
from pathlib import Path

class GeneralHTML_Download():

    def __init__(self,server):
        self.server = server
        self.path = None
        self.date_formats = []
        self.chunk_size = 131072
        self.global_session = None
        self.ntries = 5
        self.timeout = 30
        self.verbose = True
        self.overwritte = False

    def get_url_date(self,date_here):
        if self.server is None:
            return None
        path_here = ''
        if self.path is not None:
            path_here = self.path
        if len(self.date_formats)>0:
            for iformat,format in enumerate(self.date_formats):
                path_here = path_here.replace(f'$DATE{iformat}$',date_here.strftime(format))
        url = self.server + '/' + path_here
        return url

    def download_date(self,date_here,output_path):
        url = self.get_url_date(date_here)
        if url is None:
            print(f'[ERROR] URL for date {date_here.strftime("%Y-%m-%d")} could not be retrieved')
            return
        return self.httpdl(url,output_path)

    def getSession(self):
        if self.global_session is None:
            # turn on debug statements for requests
            if self.verbose:
                print("[INFO] Session started")
                #logging.basicConfig(level=logging.DEBUG)

            self.global_session = requests.Session()
            self.global_session.mount('https://', HTTPAdapter(max_retries=self.ntries))
        else:
            if self.verbose :
                print("[INFO] Reusing existing session")



    def httpdl(self,urlStr,output_path):

        status = 0
        self.getSession()

        if os.path.isfile(output_path):
            ofile = output_path
        else:
            name = urlStr.split('/')[-1]
            ofile = os.path.join(output_path,name)

        if os.path.exists(ofile) and not self.overwritte:
            print(f'[WARNING] {ofile} already exist. Skipping...')
            return status


        percent_included = []

        with self.global_session.get(urlStr, stream=True, timeout=self.timeout) as req:
            if req.status_code != 200:
                status = req.status_code
            else:
                total_length = req.headers.get('content-length')
                length_downloaded = 0
                total_length = int(total_length)
                if self.verbose:
                    print(f'[INFO] Downloading {os.path.basename(ofile)} ({total_length/1024/1024:.2f} Mbs) ')
                with open(ofile, 'wb') as fd:
                    for chunk in req.iter_content(chunk_size=self.chunk_size):
                        if chunk:  # filter out keep-alive new chunks
                            length_downloaded += len(chunk)
                            fd.write(chunk)
                            if self.verbose:
                                percent_done = int(50 * length_downloaded / total_length)
                                if percent_done%5==0 and percent_done not in percent_included:
                                    percent_included.append(percent_done)
                                    print(f'[INFO] Download progress: {percent_done*2}%')
                if self.verbose:
                    print(f'[INFO] Completed')

        return status

    def check_file_date(self,outputdir,date_here):
        path_here = self.path.split('/')[-1]
        for iformat,format in enumerate(self.date_formats):
            path_here = path_here.replace(f'$DATE{iformat}$',date_here.strftime(format))
        file_here = os.path.join(outputdir,path_here)
        return os.path.isfile(file_here)




##CHILD Classes
class OC_CCI_V6_Download(GeneralHTML_Download):

    def __init__(self):
        server = 'https://www.oceancolour.org/thredds/fileServer/cci/v6.0-release/geographic/daily/all_products'
        super().__init__(server)
        self.path = '$DATE0$/ESACCI-OC-L3S-OC_PRODUCTS-MERGED-1D_DAILY_4km_GEO_PML_OCx_QAA-$DATE1$-fv6.0.nc'
        self.date_formats = ['%Y','%Y%m%d']


