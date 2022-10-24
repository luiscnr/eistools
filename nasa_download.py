from datetime import datetime as dt
import os, re
import urllib.request
import urllib.parse
import ssl


class NASA_DOWNLOAD:
    def __init__(self):
        self.direct_access_base_url = 'https://oceandata.sci.gsfc.nasa.gov/directdataaccess/Level-2/'
        self.sensors = {
            'AQUA': {
                'direct_access_folder': 'Aqua-MODIS'
            },
            'VIIRSJ': {
                'direct_access_folder': 'NOAA20-VIIRS',
                'nrt_wce': "JPSS1_VIIRS.DATET\d*\.L2\.OC\.NRT\.nc",
                'dt_wce': "JPSS1_VIIRS.DATET\d*\.L2\.OC\.nc",
                "nrt_cnr_server_dir": "/store3/OC/VIIRSJ/daily",
                "nrt_cnr_server_wce": "DATE_VIIRSJ_NRT_REGION_oc_proc_L2_",
                "prefix": "JPSS1_VIIRS.DATE"

            },
            'VIIRS': {
                'direct_access_folder': 'SNPP-VIIRS',
                'nrt_wce': "SNPP_VIIRS.DATET\d*\.L2\.OC\.NRT\.nc",
                'dt_wce': "SNPP_VIIRS.DATET\d*\.L2\.OC\.nc",
                "nrt_cnr_server_dir": "/store3/OC/VIIRS/daily",
                "nrt_cnr_server_wce": "DATE_VIIRS_NRT_REGION_oc_proc_L2_",
                "prefix": "SNPP_VIIRS.DATE"
            }
        }

    def get_url_date(self, sensor, date_here):
        year_str = date_here.strftime('%Y')
        jday_str = date_here.strftime('%j')
        url = os.path.join(self.direct_access_base_url, self.sensors[sensor]['direct_access_folder'], year_str,
                           jday_str)
        return url

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
        #print(path_log)
        #print(wce)
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

    def get_list_files(self, date_here, sensor, region, mode):

        datestr = date_here.strftime('%Y%m%d')
        ssl._create_default_https_context = ssl._create_unverified_context
        url_date = self.get_url_date(sensor, date_here)
        #print(url_date)
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
        #print(all_scenes)
        prefixes = self.get_expected_prefix_files_from_nrt_cnr_server(sensor, date_here, region)

        scenes = []
        for scene in all_scenes:
            for prefix in prefixes:
                if scene.startswith(prefix) and scene.endswith(suffix):
                    scenes.append(scene)
                    break
        scenes = list(set(scenes))

        print(' '.join(scenes))

        # scenes = []
        # for scene in all_scenes:
        #     #print(scene)
        #     if scene.endswith(suffix):
        #         scenes.append(scene)

        # print(scenes)
