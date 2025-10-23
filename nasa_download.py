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
        import __init__
        main_path = os.path.dirname(__init__.__file__)
        config_file = os.path.join(main_path,'config_download_tool.ini')
        if not os.path.exists(config_file):
            config_file = os.path.join(os.path.dirname(main_path),'config_download_tool.ini')
        if not os.path.exists(config_file):
            config_filem = os.path.join(main_path, 'config_download_tool.ini')
            print(f'[WARNING] Configuration file {config_filem} or {config_file} does not exist. Some charteristics could not be loaded')

        self.apikey = '22da4a89034645c3653f75dcd49ea11639976cef'
        self.apikey_list = {}
        self.sensors_info = {}
        self.sensors_names = {}
        self.sites_info = {}
        self.sites_names = {}
        if os.path.exists(config_file):
            self.load_configuration(config_file)

        self.direct_access_base_url = 'https://oceandata.sci.gsfc.nasa.gov/directdataaccess/Level-2/'
        self.direct_access_base_url_level1b = 'https://oceandata.sci.gsfc.nasa.gov/directdataaccess/Level-1B/'
        self.url_download = 'https://oceandata.sci.gsfc.nasa.gov/ob/getfile/'
        self.api_download = 'https://cmr.earthdata.nasa.gov/search/granules.umm_json?page_size=20&sort_key=short_name&sort_key=start_date'
        self.time_min = 6
        self.time_max = 18




    def load_configuration(self,fconfig):
        import configparser
        options = configparser.ConfigParser()
        options.read(fconfig)
        for section in options.sections():
            if section=='nasa_credentials':
                for opt in options[section].keys():
                    self.apikey_list[opt] = options[section][opt].strip()
                if 'default' in self.apikey_list:
                    self.apikey = self.apikey_list['default']
            else:
                if not 'type' in options[section].keys():
                    continue
                if options[section]['type']=='sensor':
                    self.load_sensor(section.upper(),options[section])
                if options[section]['type']=='site':
                    self.load_site(section.upper(),options[section])

    def load_site(self,site_name,site_section):
        self.sites_info[site_name]={
            'site_type': 'area',
            'geo': None,
        }
        self.sites_names[site_name] = site_name
        if 'alt_names' in site_section.keys():
            alt_names = site_section['alt_names'].strip()
            for x in alt_names.split(','):
                self.sites_names[x] = site_name

        for opt in site_section.keys():
            if opt=='geo':
                try:
                    geo = [float(x) for x in site_section[opt].strip().split(',')]
                    if len(geo)==2 or len(geo)==4:
                        self.sites_info[site_name]['geo'] = geo
                        if len(geo)==2:
                            self.sites_info[site_name]['site_type'] = 'point'
                except:
                    pass
            else:
                self.sites_info[site_name][opt] = site_section[opt]

    def load_sensor(self,sensor_name,sensor_section):
        self.sensors_info[sensor_name]={}
        self.sensors_names[sensor_name] = sensor_name
        if 'alt_names' in sensor_section.keys():
            alt_names = sensor_section['alt_names'].strip()
            for x in alt_names.split(','):
                self.sensors_names[x] = sensor_name

        for opt in sensor_section.keys():
            self.sensors_info[sensor_name][opt] = sensor_section[opt]

    def check_sensor(self,s):
        if s.upper() not in self.sensors_names.keys():
            print(f'[ERROR] Sensor {s} is not available in the configuration file')
            return None
        return self.sensors_names[s.upper()]

    def check_site(self,s):
        if s.upper() not in self.sites_names:
            print(f'[ERROR] Site {s} is not available in the configuration file')
            return None

        site_ref = self.sites_names[s.upper()]

        if self.sites_info[site_ref]['geo'] is None:
            print(f'[ERROR] Geo information for site {s} is not valid')
            return None
        return site_ref

    def check_scenes_med_bs_API(self,sen,date_here):
        ##check first dt
        short_name = self.sensors_info[sen]['short_name']
        short_name_dt = short_name.replace('_NRT', '')
        geo_limits = [30.0,48.0,-6.0,42.0]
        bounding_box = f'{geo_limits[2]},{geo_limits[0]},{geo_limits[3]},{geo_limits[1]}'
        date_next = date_here + timedelta(hours=24)
        temporal = f'{date_here.strftime("%Y-%m-%d")},{date_next.strftime("%Y-%m-%d")}'
        url_complete = f'{self.api_download}&short_name={short_name_dt}&bounding_box={bounding_box}&temporal={temporal}'
        retries = urllib3.util.Retry(connect=5, read=1, redirect=1)
        http = urllib3.PoolManager(timeout=3,retries=retries)
        try:
            resp = http.request("GET", url_complete)
        except:
            print(f'[ERROR] API fails in getting a response. Please review your network connection.' )
            return 'NO_RESPONSE'
        data = json.loads(resp.data)
        ngranules = data['hits']
        if ngranules>0:
            return 'DT'
        ##check nrt if dt is falso
        url_complete = f'{self.api_download}&short_name={short_name}&bounding_box={bounding_box}&temporal={temporal}'
        #http = urllib3.PoolManager()
        try:
            resp = http.request("GET", url_complete)
        except:
            print(f'[ERROR] API fails in getting a response. Please review your network connection.' )
            return 'NO_RESPONSE'
        data = json.loads(resp.data)
        ngranules = data['hits']
        if ngranules>0:
            return 'NRT'
        else:
            return 'N/A'


    def getscenes_by_region_EarthData_API(self,sen, date_here,geo_limits,is_dt):
        short_name = self.sensors_info[sen]['short_name']


        if is_dt: short_name = short_name.replace('_NRT','')
        bounding_box = f'{geo_limits[2]},{geo_limits[0]},{geo_limits[3]},{geo_limits[1]}'
        date_next = date_here + timedelta(hours=24)
        temporal = f'{date_here.strftime("%Y-%m-%d")},{date_next.strftime("%Y-%m-%d")}'

        url_complete = f'{self.api_download}&short_name={short_name}&bounding_box={bounding_box}&temporal={temporal}'
        #print(url_complete)
        http = urllib3.PoolManager()
        resp = http.request("GET", url_complete)
        data = json.loads(resp.data)
        ngranules = data['hits']
        #print(ngranules)
        granules = []
        if ngranules > 0:
            for item in data['items']:
                #print('---------------------------------------------------------')
                #print(item)
                name = item['umm']['DataGranule']['ArchiveAndDistributionInformation'][0]['Name']
                date_name = dt.strptime(name.split('.')[1], '%Y%m%dT%H%M%S')
                if self.time_min <= date_name.hour <= self.time_max:
                    granules.append(item['umm']['DataGranule']['ArchiveAndDistributionInformation'][0]['Name'])
        return granules

    def getscenes_by_point_EarthData_API(self, sen, date_here, insitu_lat,insitu_lon,is_dt):

        short_name = self.sensors_info[sen]['short_name']
        if is_dt: short_name = short_name.replace('_NRT', '')

        lon_min = insitu_lon - 0.5
        lon_max = insitu_lon + 0.5
        lat_min = insitu_lat - 0.5
        lat_max = insitu_lat + 0.5
        bounding_box = f'{lon_min},{lat_min},{lon_max},{lat_max}'
        date_next = date_here + timedelta(hours=24)
        temporal = f'{date_here.strftime("%Y-%m-%d")},{date_next.strftime("%Y-%m-%d")}'
        print(f'[INFO] Retrieving list for sensor: {short_name}')
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

    def getscenes_by_site_EarthData_API(self,sensor,date,site,is_dt):
        if 'time_min' in self.sites_info[site].keys() and 'time_max' in self.sites_info[site].keys():
            try:
                tmin = int(self.sites_info[site]['time_min'].strip())
                tmax = int(self.sites_info[site]['time_max'].strip())
                self.time_min = tmin
                self.time_max = tmax
            except:
                print(f'[WARNING] time_mix and time_max options for site {site} are not valid. Default options (6-18) will be used')
        geo_info = self.sites_info[site]['geo']
        site_type = self.sites_info[site]['site_type']
        list = None
        if site_type=='point':
            list = self.getscenes_by_point_EarthData_API(sensor,date,geo_info[0],geo_info[1],is_dt)
        elif site_type=='area':
            list = self.getscenes_by_region_EarthData_API(sensor,date,geo_info,is_dt)
        return list


    def get_path_orig(self, sensor, date_here):
        path = self.sensors_info[sensor]['nrt_cnr_server_dir']
        path_date = os.path.join(path, date_here.strftime('%Y'), date_here.strftime('%j'))
        return path_date

    def get_list_files_orig(self, sensor, date_here):
        path = self.sensors_info[sensor]['nrt_cnr_server_dir']
        path_date = os.path.join(path, date_here.strftime('%Y'), date_here.strftime('%j'))

        dest_list = {}

        for name in os.listdir(path_date):
            prefix = self.sensors_info[sensor]['name_cnr_prefix']
            suffix = self.sensors_info[sensor]['name_cnr_suffix']

            if not name.startswith(prefix) or not name.endswith(suffix):
                continue

            datestr = name[name.find(prefix) + len(prefix):name.find(suffix)]
            suffix_dt = self.sensors_info[sensor]['suffix_dt']
            name_output = self.sensors_info[sensor]['prefix']
            datefile = dt.strptime(datestr, '%Y%j%H%M%S')
            name_output = name_output.replace('DATE', datefile.strftime('%Y%m%dT%H%M%S'))
            name_output = f'{name_output}{suffix_dt}'
            # dest_list.append(name_output)
            dest_list[name] = name_output

        return dest_list

    def get_date_with01(self, name_file, sensor):
        prefix = self.sensors_info[sensor]['prefix']
        suffix = self.sensors_info[sensor]['suffix_dt']
        datestr = name_file[prefix.find('DATE'):name_file.find(suffix)]
        datestr = dt.strptime(datestr, '%Y%m%dT%H%M%S').replace(second=1).strftime('%Y%m%dT%H%M%S')
        name_file_out = prefix.replace('DATE', datestr)
        name_file_out = f'{name_file_out}{suffix}'
        return name_file_out

    def get_url_date(self, sensor, date_here):
        year_str = date_here.strftime('%Y')
        #jday_str = date_here.strftime('%j')
        jday_str = date_here.strftime('%d-%b-%Y')
        baseurl = self.direct_access_base_url_level1b if sensor=='PACE_SCI' else self.direct_access_base_url
        url = os.path.join(baseurl, self.sensors_info[sensor]['direct_access_folder'], year_str,
                           jday_str)
        return url

    def get_url_download(self, name_file):
        url = os.path.join(self.url_download, name_file)
        return url

    def download_granule(self, granule, path_out,overwrite):
        import obdaac_download as od
        file_out = os.path.join(path_out,granule)
        if os.path.exists(file_out) and not overwrite:
            print(f'[WARNING] File {file_out} already exist. Skipping...')
            return
        url = self.get_url_download(granule)
        od.do_download(url, path_out, self.apikey)

        #open(file_out, 'wb').write(r.content)

    def get_wce_nrt_cnr_server(self, sensor, date_here, region):
        wce = self.sensors_info[sensor]['nrt_cnr_server_wce']
        wce = wce.replace('DATE', date_here.strftime('%Y%j'))
        wce = wce.replace('REGION', region.upper())
        return wce

    def get_folder_nrt_cnr_server(self, sensor, date_here):
        path_base = self.sensors_info[sensor]['nrt_cnr_server_dir']
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
                prefix = self.sensors_info[sensor]['prefix']
                prefix = prefix.replace('DATE', str_date_prefix)
                lprefix.append(prefix)
        if len(lprefix) == 0:
            return None
        return lprefix

    def check_dt_files(self, date_here, sensor):
        datestr = date_here.strftime('%Y%m%d')
        ssl._create_default_https_context = ssl._create_unverified_context
        url_date = self.get_url_date(sensor, date_here)

        wce = self.sensors_info[sensor]['dt_wce']
        wce = wce.replace('DATE', datestr)

        this_try = 1
        while this_try<=5:
            try:
                print(f'try: {this_try}')
                print(url_date)
                r = requests.get(url_date,timeout=10)
            except:
                this_try = this_try + 1
                time.sleep(5)
                continue
            print(r.status_code)
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
            wce = self.sensors_info[sensor]['nrt_wce']
            suffix = 'L2.OC.NRT.nc'
        elif mode.upper() == 'DT':
            wce = self.sensors_info[sensor]['dt_wce']
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


    def get_list_date_with_options(self,options,sensor, site_name, region, lat_point, lon_point, date_here):
        timeliness = 'DT' if 'timeliness' not in options.keys() else options['timeliness']
        version = None if 'version' not in options.keys() else options['version']
        if timeliness is None:
            print(f'[ERROR] Timeliness {timeliness} is not valid. Download can not be started')
            return

        if version is not None:
            version_valid = True if version.startswith('V') else False
            if version_valid:
                try:
                    float(version[1:].replace('_','.'))
                except:
                    version_valid = False
            if not version_valid:
                print(f'[ERROR] Version {version} is not valid. It should be in the format V#_# (e.g. V3_1). Download can not be started')
                return


        list_dt = []
        list_nrt = []
        if timeliness=='DT' or timeliness=='ANY':
            list_dt = self.get_list_date(sensor,site_name,region,lat_point,lon_point,date_here,False)
        if timeliness=='NRT' or timeliness=='ANY':
            list_nrt = self.get_list_date(sensor,site_name,region,lat_point,lon_point,date_here,True)

        final_list = []
        for granule in list_dt:
            if version is not None and granule.find(version)==-1:
                print(f'[WARNING] Granule {granule} is not in the correct version {version} required for downloading')
                continue
            final_list.append(granule)
        for granule in list_nrt:
            if version is not None and granule.find(version)==-1:
                print(f'[WARNING] Granule {granule} is not in the correct version {version} required for downloading')
                continue
            final_list.append(granule)

        return final_list




    def get_list_date(self,sensor, site_name, region, lat_point, lon_point, date_here,use_nrt):
        import os
        import obdaac_download as od
        sensor = self.check_sensor(sensor)
        if sensor is None:
            return

        is_dt = True if not use_nrt else False

        listr = []
        if site_name is not None:
            listr = self.getscenes_by_site_EarthData_API(sensor, date_here, site_name, is_dt)
        elif region is not None:
            listr = self.getscenes_by_region_EarthData_API(sensor,date_here,region,is_dt)
        elif lat_point is not None and lon_point is not None:
            listr = self.getscenes_by_point_EarthData_API(sensor, date_here, lat_point, lon_point, is_dt)

        if is_dt and len(listr) == 0:  ##check also with nrt mode
            is_dt = False
            if site_name is not None:
                listr = self.getscenes_by_site_EarthData_API(sensor, date_here, site_name, is_dt)
            elif lat_point is not None and lon_point is not None:
                listr = self.getscenes_by_point_EarthData_API(sensor, date_here, lat_point, lon_point, is_dt)

        return listr

    def download_list(self,output_path,sensor,listr,date_here,overwrite):
        import obdaac_download as od
        if len(listr)==0:
            print(f'[INFO] No granules were found to be downloaded for date: {date_here}')
        if output_path is not None:
            self.sensors_info[sensor]['nrt_cnr_server_dir'] = output_path
        path_out = self.get_path_orig(sensor, date_here)

        print(f'[INFO] {len(list)} granules identified for date {date_here}')

        for granule in list:
            print(f'[INFO] Downloading granule: {granule}')
            file_granule = os.path.join(path_out, granule)
            if os.path.exists(file_granule) and not overwrite:
                print(f'[WARNING] Ouput file: {file_granule} already exists. Skipping....')
                return
            url = self.get_url_download(granule)
            od.do_download(url, path_out, self.apikey)

    def download_date(self,output_path,sensor, site_name,region, lat_point, lon_point, date_here,use_nrt,overwrite):
        listr = self.get_list_date(sensor,site_name,region,lat_point,lon_point,date_here,use_nrt)
        self.download_list(output_path,sensor,listr,date_here,overwrite)
        # import os
        # import obdaac_download as od
        # sensor = self.check_sensor(sensor)
        # if sensor is None:
        #     return
        # if output_path is not None:
        #     self.sensors_info[sensor]['nrt_cnr_server_dir'] = output_path
        # path_out = self.get_path_orig(sensor, date_here)
        #
        # is_dt = True if not use_nrt else False
        #
        # list = []
        # if site_name is not None:
        #     list = self.getscenes_by_site_EarthData_API(sensor, date_here, site_name, is_dt)
        # elif lat_point is not None and lon_point is not None:
        #     list = self.getscenes_by_point_EarthData_API(sensor, date_here, lat_point, lon_point, is_dt)
        #
        # if is_dt and len(list)==0: ##check also with nrt mode
        #     is_dt = False
        #     if site_name is not None:
        #         list = self.getscenes_by_site_EarthData_API(sensor, date_here, site_name, is_dt)
        #     elif lat_point is not None and lon_point is not None:
        #         list = self.getscenes_by_point_EarthData_API(sensor, date_here, lat_point, lon_point, is_dt)
        #
        # if len(list) > 0:
        #     print(f'[INFO] {len(list)} granules identified for date {date_here}')
        #     for granule in list:
        #         print(f'[INFO] Downloading granule: {granule}')
        #         file_granule = os.path.join(path_out, granule)
        #         if os.path.exists(file_granule) and not overwrite:
        #             print(f'[WARNING] Ouput file: {file_granule} already exists. Skipping....')
        #             return
        #
        #         url = self.get_url_download(granule)
        #         od.do_download(url, path_out, self.apikey)
        # else:
        #     print(f'[INFO] No granules were found to be downloaded for date: {date_here}')

