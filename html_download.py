import os, requests
from datetime import datetime as dt
from requests.adapters import HTTPAdapter
from pathlib import Path

class GeneralHTML_Download:

    def __init__(self,server,all_vars=None):
        self.server = server
        self.path = None
        self.date_formats = []
        self.chunk_size = 131072
        self.global_session = None
        self.ntries = 5
        self.timeout = 30
        self.verbose = True
        self.overwritte = False
        self.all_vars = all_vars

    def get_url_date(self,date_here,netcdf_subset = None):
        if self.server is None:
            return None
        path_here = ''
        if self.path is not None:
            path_here = self.path
        if len(self.date_formats)>0:
            for iformat,format in enumerate(self.date_formats):
                path_here = path_here.replace(f'$DATE{iformat}$',date_here.strftime(format))
        url = self.server + '/' + path_here

        if netcdf_subset is not None:
            if self.all_vars is None:
                print(f'[ERROR] Accepted variable list is required to apply NetCDF subset')
                return None
            if 'vars' in netcdf_subset:
                vars = netcdf_subset['vars']
                for var in vars:
                    if var  not in self.all_vars:
                        print(f'[ERROR] {var} is not in the list of accepted variables: {self.all_vars}')
                        return None
            else:
                vars = self.all_vars
            var_condition = '&'.join([f'var={x}' for x in vars])

            if netcdf_subset['geo'] is not None:
                limits = netcdf_subset['geo']  #south, north, west, east
                geo_condition = f'&north={limits[1]}&west={limits[2]}&east={limits[3]}&south={limits[0]}&horizStride=1'
            else:
                geo_condition = ''
            date_here_str = date_here.replace(hour=0,minute=0,second=0).strftime('%Y-%m-%dT%H:%M:%SZ')
            date_here_str = date_here_str.replace(':','%3A')
            time_condition = f'&time_start={date_here_str}&time_end={date_here_str}&timeStride=1'

            url = url + '?' + var_condition + geo_condition + time_condition + '&accept=netcdf'

        return url

    def download_date(self,date_here,output_path,netcdf_subset=None):

        url = self.get_url_date(date_here,netcdf_subset=netcdf_subset)

        if url is None:
            print(f'[ERROR] URL for date {date_here.strftime("%Y-%m-%d")} could not be retrieved')
            return
        else:
            print(f'[INFO] URL: {url}')
        print(f'[INFO] Output path: {output_path}')
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
            if name.find('?')>0:
                name  = name.split('?')[0]
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
                if total_length is not None:
                    total_length = int(total_length)
                    if self.verbose:
                        print(f'[INFO] Downloading {os.path.basename(ofile)} ({total_length/1024/1024:.2f} Mbs) ')
                else:
                    if self.verbose:
                        print(f'[INFO] Downloading {os.path.basename(ofile)} Unknown size. ')

                with open(ofile, 'wb') as fd:
                    for chunk in req.iter_content(chunk_size=self.chunk_size):
                        if chunk:  # filter out keep-alive new chunks
                            length_downloaded += len(chunk)
                            fd.write(chunk)
                            if self.verbose :
                                if total_length is not None:
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

    def check_file_date_ods(self,work_date,od,ods):
        output_path_date = od
        if ods is not None:
            ods_parts = ods.split('/')
            for part in ods_parts:
                dir_date_name = work_date.strftime(part)
                output_path_date = os.path.join(output_path_date, dir_date_name)
        if not os.path.isdir(output_path_date):
            return False
        return self.check_file_date(output_path_date,work_date)


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
            except Exception as ex:
                print(f'[ERROR] Exception creating directory {folder}: {ex}')
                return False

        return True








##CHILD Classes
class OC_CCI_V6_Download(GeneralHTML_Download):

    def __init__(self,type_product):
        server = None
        path = None
        date_formats = ['%Y', '%Y%m%d']
        all_vars = None
        self.bands = ['412','443','490','510','560','665']
        if type_product=='all':
            server = 'https://www.oceancolour.org/thredds/fileServer/cci/v6.0-release/geographic/daily/all_products'
            path = '$DATE0$/ESACCI-OC-L3S-OC_PRODUCTS-MERGED-1D_DAILY_4km_GEO_PML_OCx_QAA-$DATE1$-fv6.0.nc'
        elif type_product=='rrs':
            server = 'https://www.oceancolour.org/thredds/fileServer/cci/v6.0-release/geographic/daily/rrs'
            path = '$DATE0$/ESACCI-OC-L3S-RRS-MERGED-1D_DAILY_4km_GEO_PML_RRS-$DATE1$-fv6.0.nc'
        elif type_product=='chl':
            server = 'https://www.oceancolour.org/thredds/fileServer/cci/v6.0-release/geographic/daily/chlor_a'
            path = '$DATE0$/ESACCI-OC-L3S-CHLOR_A-MERGED-1D_DAILY_4km_GEO_PML_OCx-$DATE1$-fv6.0.nc'
        elif type_product=='iop':
            server = 'https://www.oceancolour.org/thredds/fileServer/cci/v6.0-release/geographic/daily/iop'
            path = '$DATE0$/ESACCI-OC-L3S-IOP-MERGED-1D_DAILY_4km_GEO_PML_OCx_QAA-$DATE1$-fv6.0.nc'
        elif type_product=='kd':
            server = 'https://www.oceancolour.org/thredds/fileServer/cci/v6.0-release/geographic/daily/kd'
            path = '$DATE0$/ESACCI-OC-L3S-K_490-MERGED-1D_DAILY_4km_GEO_PML_KD490_Lee-$DATE1$-fv6.0.nc'
        elif type_product=='subset':
            server = 'https://www.oceancolour.org/thredds/ncss/cci/v6.0-release/geographic/daily/all_products'
            path = '$DATE0$/ESACCI-OC-L3S-OC_PRODUCTS-MERGED-1D_DAILY_4km_GEO_PML_OCx_QAA-$DATE1$-fv6.0.nc'
            all_vars = self.get_all_vars()

        self.type_product = type_product

        if server is not None and path is not None:
            super().__init__(server,all_vars=all_vars)
            self.path = path
            self.date_formats = date_formats

    def download_subset(self,work_date,output_path,var_group,region,var_list=None,geo_limits=None):
        variable_list =None
        if var_list is not None:
            variable_list = var_list
        elif var_group is not None:
            variable_list = self.get_var_by_group(var_group)
            if variable_list is None:
                return
        limits = None
        if geo_limits is not None:
            limits = geo_limits
        elif region is not None:
            if region.lower()=='arc':
                limits = [65,90,-180,180]
            elif region.lower()=='bal':
                limits = [53.2,65.9,9.2,30.3]
            elif region.lower()=='med':
                limits = [30,46,-6,36.5]
            elif region.lower()=='blk':
                limits = [40,48,26.5,42]
            elif region.lower()=='med+blk':
                limits = [30,48,-6,42]
            else:
                print(f'[WARNING] Region is not valid. Choose among: arc, bal, blk, med or med+blk')
                return
        subset = {
            'vars': variable_list,
            'geo': limits
        }
        self.download_date(work_date,output_path,netcdf_subset=subset)

    def make_bulk_download(self,options,output_directory,ods,overwrite):
        date_list = options['date_list']
        for op in options:
            if op=='date_list':
                print(f'[INFO][CCI_DOWNLOAD_OPTIONS] Date list with {len(date_list)} dates')
            else:
                print(f'[INFO][CCI_DOWNLOAD_OPTIONS] {op} : {options[op]}')
        for work_date in date_list:
            output_path_date = self.get_folder_out(work_date,output_directory,ods)
            if output_path_date is None:
                continue
            if self.check_file_date(output_path_date,work_date) and not overwrite:
                print(f'[INFO] File for date {work_date.strftime("%Y-%m-%d")} has been already downloaded. Skipping...')
                continue

            if self.type_product=='subset':
                self.download_subset(work_date,output_path_date,options['var_group'],options['region'],var_list=options['var_list'],geo_limits=options['geo_limits'])
            else:
                self.download_date(work_date,output_path_date,netcdf_subset=None)



    ##groups separated by _: group1_group_2
    def get_var_by_group(self,groups):
        var_list = []
        for group in groups.split('_'):
            if group=='all':
                var_list = var_list + self.get_all_vars()
            elif group=='rrs':
                var_list = var_list + self.get_rrs_vars()
            elif group=='rrsfile':
                var_list = var_list + self.get_rrsfile_vars()
            elif group=='chl':
                var_list = var_list + self.get_chl_vars()
            elif group=='chlfile':
                var_list = var_list + self.get_chlfile_vars()
            elif group=='iop':
                var_list = var_list + self.get_iop_vars()
            elif group=='iopfile':
                var_list = var_list + self.get_iopfile_vars()
            elif group=='kd':
                var_list = var_list + self.get_kd_vars()
            elif group=='kdfile':
                var_list = var_list + self.get_kdfile_vars()
            elif group=='obs':
                var_list = var_list + self.get_obs_vars()
            elif group=='owt':
                var_list = var_list + self.get_owt_vars()
            else:
                print(f'[ERROR] {group} is not valid. Choose among all, rrs, rrsfile, chl, chlfile, iop, iopfile, kd, kdfile, obs, owt')
                return None
        if len(var_list)==0:
            return None
        var_list = list(dict.fromkeys(var_list))##sure elements are not repeated
        return var_list

    def get_all_vars(self):
        return self.get_rrsfile_vars()+self.get_chl_vars()+self.get_iop_vars()+self.get_kd_vars()

    def get_rrs_vars(self):
        rrs_vars = [f'Rrs_{x}' for x in self.bands]
        rrs_bias = [f'{x}_bias'for x in rrs_vars]
        rrs_rmsd = [f'{x}_rmsd'for x in rrs_vars]

        return rrs_vars+rrs_bias+rrs_rmsd

    def get_rrsfile_vars(self):
        return self.get_rrs_vars()+self.get_obs_vars()+self.get_owt_vars()

    def get_obs_vars(self):
        obs_vars = ['MERIS_nobs','MODISA_nobs','OLCI-A_nobs','OLCI-B_nobs','SeaWiFS_nobs','VIIRS_nobs','total_nobs']
        return obs_vars

    def get_owt_vars(self):
        owt_vars = [f'water_class{x:.0f}' for x in range(1,15)]
        return owt_vars

    def get_iop_vars(self):
        adg_vars = [f'adg_{x}' for x in self.bands]
        adg_bias = [f'{x}_bias' for x in adg_vars]
        adg_rmsd = [f'{x}_rmsd' for x in adg_vars]

        aph_vars = [f'aph_{x}' for x in self.bands]
        aph_bias = [f'{x}_bias' for x in aph_vars]
        aph_rmsd = [f'{x}_rmsd' for x in aph_vars]

        atot_vars = [f'atot_{x}' for x in self.bands]
        bbp_vars = [f'bbp_{x}' for x in self.bands]

        return adg_vars+adg_bias+adg_rmsd+aph_vars+aph_bias+aph_rmsd+atot_vars+bbp_vars

    def get_iopfile_vars(self):
        return self.get_iop_vars()+self.get_obs_vars()

    def get_chl_vars(self):
        chl_vars = ['chlor_a','chlor_a_log10_bias','chlor_a_log10_rmsd']
        return chl_vars

    def get_chlfile_vars(self):
        return self.get_chl_vars()+self.get_obs_vars()

    def get_kd_vars(self):
        kd_vars = ['kd_490', 'kd_490_bias','kd_490_rmsd']
        return kd_vars

    def get_kdfile_vars(self):
        return self.get_kd_vars()+self.get_obs_vars()




