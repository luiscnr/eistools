import time
from datetime import datetime as dt
from datetime import timedelta
import os, re
import urllib.request
import urllib.parse
import ssl
import requests
import urllib3
import json


class NASA_DOWNLOAD:
    def __init__(self):
        self.direct_access_base_url = 'https://oceandata.sci.gsfc.nasa.gov/directdataaccess/Level-2/'
        self.url_download = 'https://oceandata.sci.gsfc.nasa.gov/ob/getfile/'
        self.api_download = 'https://cmr.earthdata.nasa.gov/search/granules.umm_json?page_size=20&sort_key=short_name&sort_key=start_date'
        self.time_min = 6
        self.time_max = 18
        self.sensors = {
            'AQUA': {
                'direct_access_folder': 'Aqua-MODIS',
                'nrt_wce': "AQUA_MODIS.DATET\d*\.L2\.OC\.NRT\.nc",
                'dt_wce': "AQUA_MODIS.DATET\d*\.L2\.OC\.nc",
                "nrt_cnr_server_dir": "/mnt/c/DATA_LUIS/OCTAC_WORK/CC0C-591-_100days/AQUASOURCES",
                "prefix": "AQUA_MODIS.DATE",
                "suffix_dt": ".L2.OC.nc",
                "name_cnr_prefix": "A",
                "name_cnr_suffix": ".L2_LAC_OC.nc",
                "short_name": "MODISA_L2_OC_NRT"
            },
            'VIIRSJ': {
                'direct_access_folder': 'NOAA20-VIIRS',
                'nrt_wce': "JPSS1_VIIRS.DATET\d*\.L2\.OC\.NRT\.nc",
                'dt_wce': "JPSS1_VIIRS.DATET\d*\.L2\.OC\.nc",
                "nrt_cnr_server_dir": "/mnt/c/DATA_LUIS/OCTAC_WORK/CC0C-591-_100days/VIIRSJSOURCES",
                "nrt_cnr_server_wce": "DATE_VIIRSJ_NRT_REGION_oc_proc_L2_",
                "prefix": "JPSS1_VIIRS.DATE",
                "suffix_dt": ".L2.OC.nc",
                "name_cnr_prefix": "V",
                "name_cnr_suffix": ".L2_JPSS1_OC.nc",
                "short_name": "VIIRSJ1_L2_OC_NRT"

            },
            'VIIRS': {
                'direct_access_folder': 'SNPP-VIIRS',
                'nrt_wce': "SNPP_VIIRS.DATET\d*\.L2\.OC\.NRT\.nc",
                'dt_wce': "SNPP_VIIRS.DATET\d*\.L2\.OC\.nc",
                "nrt_cnr_server_dir": "/mnt/c/DATA_LUIS/OCTAC_WORK/CC0C-591-_100days/VIIRSSOURCES",
                "nrt_cnr_server_wce": "DATE_VIIRS_NRT_REGION_oc_proc_L2_",
                "prefix": "SNPP_VIIRS.DATE",
                "suffix_dt": ".L2.OC.nc",
                "name_cnr_prefix": "V",
                "name_cnr_suffix": ".L2_SNPP_OC.nc",
                "short_name": "VIIRSN_L2_OC_NRT"
            },
            'PACE_AOP':{
                'direct_access_folder': 'PACE-OCI',
                'nrt_wce': "PACE_OCI.DATET\d*\.L2\.OC_AOP\.V2_0\.NRT\.nc",
                'dt_wce': "PACO_OCI.DATET\d*\.L2\.OC_AOP\.V2_0\.nc",
                "nrt_cnr_server_dir": "/mnt/c/DATA_LUIS/OCTAC_WORK/CC0C-591-_100days/PACESOURCES",
                "nrt_cnr_server_wce": "DATE_PACE_NRT_REGION_oc_proc_L2_",
                "prefix": "PACE_OCI.DATE",
                "suffix_dt": ".L2.OC_AOP.V2_0.nc",
                "name_cnr_prefix": "P",
                "name_cnr_suffix": ".L2_OCI_OC.nc",
                "short_name": "PACE_OCI_L2_AOP_NRT"
            }
        }

    def getscenes_by_point_EarthData_API(self, sen, date_here, insitu_lat,insitu_lon):
        short_name = self.sensors[sen]['short_name']
        lon_min = insitu_lon - 0.5
        lon_max = insitu_lon + 0.5
        lat_min = insitu_lat - 0.5
        lat_max = insitu_lat + 0.5
        bounding_box = f'{lon_min},{lat_min},{lon_max},{lat_max}'
        date_next = date_here + timedelta(hours=24)
        temporal = f'{date_here.strftime("%Y-%m-%d")},{date_next.strftime("%Y-%m-%d")}'
        #url_complete = f'{self.api_download}&short_name={short_name}&provider=OB_DAAC&bounding_box={bounding_box}&temporal={temporal}'
        url_complete = f'{self.api_download}&short_name={short_name}&bounding_box={bounding_box}&temporal={temporal}'
        #url_complete = f'{self.api_download}&short_name={short_name}&temporal={temporal}'
        http = urllib3.PoolManager()
        resp = http.request("GET", url_complete)
        data = json.loads(resp.data)
        ngranules = data['hits']
        granules = []
        if ngranules > 0:
            for item in data['items']:
                name = item['umm']['DataGranule']['ArchiveAndDistributionInformation'][0]['Name']
                date_name = dt.strptime(name.split('.')[1], '%Y%m%dT%H%M%S')
                if self.time_min <= date_name.hour <= self.time_max:
                    granules.append(item['umm']['DataGranule']['ArchiveAndDistributionInformation'][0]['Name'])
        return granules

    def get_path_orig(self, sensor, date_here):
        path = self.sensors[sensor]['nrt_cnr_server_dir']
        path_date = os.path.join(path, date_here.strftime('%Y'), date_here.strftime('%j'))
        return path_date

    def get_list_files_orig(self, sensor, date_here):
        path = self.sensors[sensor]['nrt_cnr_server_dir']
        path_date = os.path.join(path, date_here.strftime('%Y'), date_here.strftime('%j'))

        dest_list = {}

        for name in os.listdir(path_date):
            prefix = self.sensors[sensor]['name_cnr_prefix']
            suffix = self.sensors[sensor]['name_cnr_suffix']

            if not name.startswith(prefix) or not name.endswith(suffix):
                continue

            datestr = name[name.find(prefix) + len(prefix):name.find(suffix)]
            suffix_dt = self.sensors[sensor]['suffix_dt']
            name_output = self.sensors[sensor]['prefix']
            datefile = dt.strptime(datestr, '%Y%j%H%M%S')
            name_output = name_output.replace('DATE', datefile.strftime('%Y%m%dT%H%M%S'))
            name_output = f'{name_output}{suffix_dt}'
            # dest_list.append(name_output)
            dest_list[name] = name_output

        return dest_list

    def get_date_with01(self, name_file, sensor):
        prefix = self.sensors[sensor]['prefix']
        suffix = self.sensors[sensor]['suffix_dt']
        datestr = name_file[prefix.find('DATE'):name_file.find(suffix)]
        datestr = dt.strptime(datestr, '%Y%m%dT%H%M%S').replace(second=1).strftime('%Y%m%dT%H%M%S')
        name_file_out = prefix.replace('DATE', datestr)
        name_file_out = f'{name_file_out}{suffix}'
        return name_file_out

    def get_url_date(self, sensor, date_here):
        year_str = date_here.strftime('%Y')
        jday_str = date_here.strftime('%j')
        url = os.path.join(self.direct_access_base_url, self.sensors[sensor]['direct_access_folder'], year_str,
                           jday_str)
        return url

    def get_url_download(self, name_file):
        url = os.path.join(self.url_download, name_file)
        return url

    def download_file(self, name_file, file_out):
        url = self.get_url_download(name_file)
        r = requests.get(url, allow_redirects=True)
        open(file_out, 'wb').write(r.content)

    def get_wce_nrt_cnr_server(self, sensor, date_here, region):
        wce = self.sensors[sensor]['nrt_cnr_server_wce']
        wce = wce.replace('DATE', date_here.strftime('%Y%j'))
        wce = wce.replace('REGION', region.upper())
        return wce

    def get_folder_nrt_cnr_server(self, sensor, date_here):
        path_base = self.sensors[sensor]['nrt_cnr_server_dir']
        folder = os.path.join(path_base, date_here.strftime('%Y'), date_here.strftime('%j'))
        return folder

    def get_expected_prefix_files_from_nrt_cnr_server(self, sensor, date_here, region):
        path_log = self.get_folder_nrt_cnr_server(sensor, date_here)
        wce = self.get_wce_nrt_cnr_server(sensor, date_here, region)
        # print(path_log)
        # print(wce)
        lprefix = []
        for name in os.listdir(path_log):
            if name.startswith(wce):
                date_str_file = name.split('_')[7][1:].split('.')[0]
                date_file = dt.strptime(date_str_file, '%Y%j%H%M%S')
                str_date_prefix = date_file.strftime('%Y%m%dT%H%M')
                prefix = self.sensors[sensor]['prefix']
                prefix = prefix.replace('DATE', str_date_prefix)
                lprefix.append(prefix)
        if len(lprefix) == 0:
            return None
        return lprefix

    def check_dt_files(self, date_here, sensor):
        datestr = date_here.strftime('%Y%m%d')
        ssl._create_default_https_context = ssl._create_unverified_context
        url_date = self.get_url_date(sensor, date_here)


        wce = self.sensors[sensor]['dt_wce']
        wce = wce.replace('DATE', datestr)

        this_try = 1
        while this_try<=5:
            try:
                r = requests.get(url_date,timeout=10)
            except:
                this_try = this_try + 1
                time.sleep(10)
                continue
            if r.status_code==200:
                list = re.findall(wce,r.text)
                r.close()
                return len(list)
            this_try = this_try + 1
            time.sleep(10)
        return -1

    def get_list_files(self, date_here, sensor, region, mode):

        datestr = date_here.strftime('%Y%m%d')
        ssl._create_default_https_context = ssl._create_unverified_context
        url_date = self.get_url_date(sensor, date_here)
        # print(url_date)
        request = urllib.request.Request(url_date)
        response = urllib.request.urlopen(request)
        page = response.read()
        page = page.decode('utf-8')
        if mode.upper() == 'NRT':
            wce = self.sensors[sensor]['nrt_wce']
            suffix = 'L2.OC.NRT.nc'
        elif mode.upper() == 'DT':
            wce = self.sensors[sensor]['dt_wce']
            suffix = 'L2.OC.nc'
        wce = wce.replace('DATE', datestr)

        all_scenes = list(re.findall(wce, page))
        # print(all_scenes)

        if region is None:
            return all_scenes

        prefixes = self.get_expected_prefix_files_from_nrt_cnr_server(sensor, date_here, region)

        scenes = []
        for scene in all_scenes:
            for prefix in prefixes:
                if scene.startswith(prefix) and scene.endswith(suffix):
                    scenes.append(scene)
                    break
        scenes = list(set(scenes))

        print(' '.join(scenes))
