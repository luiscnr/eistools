import calendar
import os
import json
from datetime import datetime as dt
from datetime import timedelta
from source_info import SourceInfo
import pandas as pd
from netCDF4 import Dataset


class ProductInfo:
    def __init__(self):
        sdir = os.path.abspath(os.path.dirname(__file__))
        self.path2info = os.path.join(os.path.dirname(sdir), 'PRODUCT_INFO')
        self.path_reformat_script = os.path.join(os.path.dirname(sdir), 'reformatting_file_cmems2_202211.sh')

        self.product_name = ''
        self.dataset_name = ''
        self.pinfo = {}
        self.dinfo = {}

        self.modes = ['my', 'nrt']
        self.basins = ['bal', 'blk', 'med']
        self.levels = ['l3', 'l4']
        self.dataset_types = ['optics', 'plankton', 'reflectance', 'transp']
        self.sensors = ['olci', 'multi', 'gapfree-multi', 'multi-climatology']
        self.dict_info = {}
        self.start_my_dictionary()
        self.start_nrt_dictionary()

        self.MODE = 'UPLOAD'  # UPLOAD, REFORMAT, NONE

        self.params_slurm = {
            'nodes': '1',
            'ntasks': '1',
            'p': 'octac',
            'mail-type': 'BEGIN,END,FAIL',
            'mail-user': 'lgonzalezvilas@artov.ismar.cnr.it'
        }

        # self.parameter_code = {
        #     'plankton':{
        #         'code': 'PLANKTON',
        #         'parameter': 'Chlorophyll-a concentration and Phytoplankton Functional Types'
        #     },
        #     'transp':{
        #         'code': 'KD490',
        #         'parameter': 'OLCI Diffuse Attenuation Coefficient at 490nm'
        #     },
        #     'reflectance':{
        #         'code': 'RRS',
        #         'parameter': 'Remote Sensing Reflectance'
        #     },
        #     'optics':{
        #         'code': 'IOPs',
        #         'parameter': 'Inherent Optical Properties'
        #     }
        #
        # }

    def get_dataset_summary(self):
        if self.dataset_name is not None:
            path_summary = os.path.join(self.path2info, 'SUMMARY')
            if not os.path.exists(path_summary):
                os.mkdir(path_summary)
            fsummary = os.path.join(path_summary, self.dataset_name + '_summary.json')
            finvalid = os.path.join(path_summary, self.dataset_name + '_invalid.csv')
            return fsummary, finvalid
        return None

    def get_dataset_name(self, mode, basin, level, dtype, sensor):
        if sensor.lower() == 'gapfree_multi':
            sensor = 'gapfree-multi'
        res = '1km'
        if sensor.lower() == 'olci':
            res = '300m'
        dataset_name = f'cmems_obs-oc_{basin.lower()}_bgc-{dtype.lower()}_{mode.lower()}_{level.lower()}-{sensor.lower()}-{res}_P1D'
        try:
            dinfo = self.dict_info[mode.lower()][basin.lower()][level.lower()][dtype.lower()][sensor.lower()]
        except:
            print(f'[ERROR] Dataset: {mode} {basin} {level} {dtype} {sensor} is not available')
            return None, None

        if dinfo['frequency'] == 'm':
            dataset_name = dataset_name.replace('P1D', 'P1M')

        if dinfo['dataset'] == dataset_name:
            product_name = dinfo['product']

        return product_name, dataset_name

    def get_product_name(self, mode, basin, level):
        dproduct = self.dict_info[mode.lower()][basin.lower()][level.lower()]
        for dtype in dproduct.keys():
            for sensor in dproduct[dtype].keys():
                product_name = dproduct[dtype][sensor]['product']
                return product_name

    ##GETTERES
    def get_dinfo_param(self, param):
        if len(self.dinfo) == 0:
            return None
        if not param in self.dinfo.keys():
            return None
        return self.dinfo[param]

    def get_region(self):
        return self.get_dinfo_param('region')

    def get_sensor(self):
        return self.get_dinfo_param('sensor')

    def get_level(self):
        return self.get_dinfo_param('level')

    def get_frequency(self):
        return self.get_dinfo_param('frequency')

    def get_number_expected_files_between_two_dates(self, start_date, end_date):
        frequency = self.get_frequency()
        if frequency is None:
            return -1
        if frequency.lower() == 'd':
            return (end_date - start_date).days + 1
        elif frequency.lower() == 'm':
            return ((end_date.year - start_date.year) * 12 + end_date.month - start_date.month) + 1
        return -1

    def get_expected_files_between_two_dates(self, start_date, end_date):
        frequency = self.get_frequency()
        if frequency is None:
            return None
        name_files = []
        if frequency.lower() == 'd':
            date_here = start_date
            while date_here <= end_date:
                date_here_str = date_here.strftime('%Y%m%d')
                name_file = f'{date_here_str}_{self.dataset_name}.nc'
                name_files.append(name_file)
                date_here = date_here + timedelta(hours=24)

        return name_files

    def get_dtype(self):
        return self.get_dinfo_param('dataset')

    def get_sources(self):
        if len(self.dinfo) == 0:
            return None
        try:
            return self.dinfo['sources']
        except:
            return None

    def get_sources_dt(self):
        if len(self.dinfo) == 0:
            return None
        try:
            return self.dinfo['sources_dt']
        except:
            return None

    def get_reprocessing_cmd(self, mode):
        if mode == 'NRT':
            return self.get_dinfo_param('CMD_NRT_reprocessing')
        if mode == 'DT':
            return self.get_dinfo_param('CMD_DT_reprocessing')

    def get_last_nrt_date(self):
        datenow = dt.now().replace(hour=12, minute=0, second=0, microsecond=0)
        sensor = self.get_sensor()
        if sensor.lower() == 'multi':
            date_dt = (datenow - timedelta(days=20)).replace(hour=23, minute=59, second=59)
        if sensor.lower() == 'gapfree_multi':
            date_dt = (datenow - timedelta(days=24)).replace(hour=23, minute=59, second=59)
        if sensor.lower() == 'olci':
            date_dt = (datenow - timedelta(days=8)).replace(hour=23, minute=59, second=59)
        return date_dt

    def check_dataset_namesin_dict(self):
        check = True
        for m in self.modes:
            for b in self.basins:
                for l in self.levels:
                    for d in self.dataset_types:
                        for s in self.sensors:
                            try:
                                dset = self.dict_info[m][b][l][d][s]
                                pname, dname = self.get_dataset_name(m, b, l, d, s)
                                if not dset['dataset'] == dname:
                                    print('[ERROR] Error in the name of the dataset...')
                                    check = False
                            except:
                                pass
        return check

    def set_product_info(self, product_name):
        self.product_name = product_name
        fproduct = os.path.join(self.path2info, product_name + '.json')
        valid = False
        if os.path.exists(fproduct):
            f = open(fproduct, "r")
            self.pinfo = json.load(f)
            f.close()
            valid = True
        return valid

    def get_product_info_file(self, product_name):
        fproduct = os.path.join(self.path2info, product_name + '.json')
        return fproduct

    def set_dataset_info(self, product_name, dataset_name):
        self.product_name = product_name
        self.dataset_name = dataset_name
        fproduct = os.path.join(self.path2info, product_name + '.json')
        valid = False

        if os.path.exists(fproduct):
            f = open(fproduct, "r")
            try:
                self.pinfo = json.load(f)
                if dataset_name in self.pinfo.keys():
                    self.dinfo = self.pinfo[dataset_name]
                    valid = True
                else:
                    print(f'[ERROR] Dataset {dataset_name} is not available in: {fproduct}')
            except:
                print(f'[ERROR] JSON file {fproduct} is not valid, could not be parsed')

            f.close()
        else:
            print(f'[ERROR] Product file {fproduct} does not exist')
        return valid

    def set_param(self, pinfo_out, param_name, param_value):
        if pinfo_out is None:
            pinfo_out = self.pinfo
        pinfo_out[self.dataset_name][param_name] = param_value
        return pinfo_out

    def update_json(self, pinfo_out):
        if pinfo_out is None:
            pinfo_out = self.pinfo
        fproduct = os.path.join(self.path2info, self.product_name + '.json')
        fout = open(fproduct, 'w')
        fout.write('{')
        datasets = list(pinfo_out.keys())
        for dataset in pinfo_out:
            self.write_new_line(fout, f'"{dataset}":')
            self.write_new_line(fout, '{')
            keys = list(pinfo_out[dataset].keys())
            for key in pinfo_out[dataset]:
                val = pinfo_out[dataset][key]
                if key == keys[-1]:
                    self.write_new_line(fout, f'"{key}":"{val}"')
                else:
                    self.write_new_line(fout, f'"{key}":"{val}",')
            if dataset == datasets[-1]:
                self.write_new_line(fout, '}')
            else:
                self.write_new_line(fout, '},')
        self.write_new_line(fout, '}')
        fout.close()

    def write_new_line(self, fout, newline):
        fout.write('\n')
        fout.write(newline)

    def get_list_all_products(self):
        product_names = []
        for name in os.listdir(self.path2info):
            if name.endswith('.json'):
                name = name[:-5]
                if name == 'SOURCES':
                    continue
                if name not in product_names:
                    product_names.append(name)
        return product_names

    def get_list_all_datasets(self):
        all_products = self.get_list_all_products()
        product_names = None
        dataset_names = None
        for p in all_products:
            product_names_here, dataset_names_here = self.get_list_datasets(p, None)
            if product_names is None:
                product_names = product_names_here
                dataset_names = dataset_names_here
            else:
                product_names = product_names + product_names_here
                dataset_names = dataset_names + dataset_names_here
        return product_names, dataset_names

    def get_list_datasets(self, product_name, frequency):
        product_names = []
        dataset_names = []
        fproduct = os.path.join(self.path2info, product_name + '.json')
        if os.path.exists(fproduct):
            f = open(fproduct, "r")
            pinfo = json.load(f)
            f.close()
            for dataset in pinfo.keys():
                if not frequency is None:
                    if pinfo[dataset]['frequency'].lower() != frequency.lower():
                        continue
                product_names.append(product_name)
                dataset_names.append(dataset)

        return product_names, dataset_names

    def get_list_datasets_with_sensor(self, product_name, sensor, frequency):
        product_names = []
        dataset_names = []
        fproduct = os.path.join(self.path2info, product_name + '.json')
        if os.path.exists(fproduct):
            f = open(fproduct, "r")
            pinfo = json.load(f)
            f.close()
            for dataset in pinfo.keys():
                if not frequency is None:
                    if not pinfo[dataset]['frequency'].lower() == frequency.lower():
                        continue
                if pinfo[dataset]['sensor'].lower() == sensor.lower():
                    product_names.append(product_name)
                    dataset_names.append(dataset)

        return product_names, dataset_names

    def get_list_datasets_with_dataset(self, product_name, dtype, frequency):
        product_names = []
        dataset_names = []
        fproduct = os.path.join(self.path2info, product_name + '.json')
        if os.path.exists(fproduct):
            f = open(fproduct, "r")
            pinfo = json.load(f)
            f.close()
            for dataset in pinfo.keys():
                if not frequency is None:
                    if pinfo[dataset]['frequency'].lower() != frequency.lower():
                        continue
                if pinfo[dataset]['dataset'].lower() == dtype.lower():
                    product_names.append(product_name)
                    dataset_names.append(dataset)

        return product_names, dataset_names

    def get_list_datasets_params(self, mode, basin, level, dtype, sensor, frequency):
        product_names = []
        dataset_names = []
        if mode is not None and basin is not None and level is not None and dtype is not None and sensor is not None:
            product_name, dataset_name = self.get_dataset_name(mode, basin, level, dtype, sensor)
            if product_name is not None and dataset_name is not None:
                product_names.append(product_name)
                dataset_names.append(dataset_name)
        elif mode is not None and basin is not None and level is not None:
            product_name = self.get_product_name(mode, basin, level)
            if dtype is None and sensor is None:
                product_names, dataset_names = self.get_list_datasets(product_name, frequency)
            elif dtype is None and sensor is not None:
                product_names, dataset_names = self.get_list_datasets_with_sensor(product_name, sensor, frequency)
            elif dtype is not None and sensor is None:
                product_names, dataset_names = self.get_list_datasets_with_dataset(product_name, dtype, frequency)

        return product_names, dataset_names

    def set_dataset_info_fromparam(self, mode, basin, level, dtype, sensor):
        product_name, dataset_name = self.get_dataset_name(mode, basin, level, dtype, sensor)
        self.set_dataset_info(product_name, dataset_name)

    def get_tag_print(self):
        if self.MODE == 'REFORMAT':
            tagprint = '[WARNING]'
        elif self.MODE == 'NONE':
            tagprint = None
        else:
            tagprint = '[ERROR]'
        return tagprint

    def get_path_orig(self, year):
        if len(self.dinfo) == 0:
            return None
        if year > 0:
            path_orig = os.path.join(self.dinfo['path_origin'], dt(year, 1, 1).strftime('%Y'))
        else:
            path_orig = self.dinfo['path_origin']
        if os.path.exists(path_orig):
            return path_orig
        else:
            tagprint = self.get_tag_print()
            if tagprint is not None:
                print(f'{tagprint} Expected year path {path_orig} does not exist')
            return None

    def check_path_reformat(self):
        if 'path_reformat' in self.dinfo.keys():
            path = self.dinfo['path_reformat']
            if os.path.exists(path) and os.path.isdir(path):
                return path
            else:
                return None
        return None

    def get_file_path_orig_reformat(self, datehere):
        tagprint = self.get_tag_print()
        if 'path_reformat' in self.dinfo.keys():
            path = self.dinfo['path_reformat']
            name_file = self.dinfo['name_origin']
            date_file_str = datehere.strftime(self.dinfo['format_date_origin'])
            file_path = os.path.join(path, name_file.replace('DATE', date_file_str))
            if not os.path.exists(file_path):
                if tagprint is not None:
                    print(f'{tagprint} Expected file orig path {file_path} does not exist')
                return None
            return file_path
        else:
            return None

    def get_file_path_orig_reformat_name(self, datehere):
        if 'path_reformat' in self.dinfo.keys():
            path = self.dinfo['path_reformat']
            name_file = self.dinfo['name_origin']
            date_file_str = datehere.strftime(self.dinfo['format_date_origin'])
            file_path = os.path.join(path, name_file.replace('DATE', date_file_str))
            return file_path
        else:
            return None

    def get_file_path_orig(self, path, datehere):
        tagprint = self.get_tag_print()
        if path is None:
            path = self.get_path_orig(datehere.year)
        if path is None:
            return None
        path_jday = os.path.join(path, datehere.strftime('%j'))
        if not os.path.exists(path_jday):
            if tagprint is not None:
                print(f'{tagprint} Expected jday path {path_jday} does not exist')
            return None
        name_file = self.dinfo['name_origin']
        date_file_str = datehere.strftime(self.dinfo['format_date_origin'])
        file_path = os.path.join(path_jday, name_file.replace('DATE', date_file_str))
        if not os.path.exists(file_path):
            if tagprint is not None:
                print(f'{tagprint} Expected file orig path {file_path} does not exist')
            return None
        return file_path

    # same as get_file_path_orig, but it returns the complete path despite of it doen't exist, it's used for checking
    # also it is used for reformat.
    def get_file_path_orig_name(self, path, datehere):
        if path is None:
            path = os.path.join(self.dinfo['path_origin'], datehere.strftime('%Y'))
        path_jday = os.path.join(path, datehere.strftime('%j'))
        name_file = self.dinfo['name_origin']
        date_file_str = datehere.strftime(self.dinfo['format_date_origin'])
        file_path = os.path.join(path_jday, name_file.replace('DATE', date_file_str))
        return file_path

    def check_processed_files(self, datehere):
        path = os.path.join(self.dinfo['path_origin'], datehere.strftime('%Y'))
        path_jday = os.path.join(path, datehere.strftime('%j'))
        nTot = -1
        nAva = 0
        missing_files = []

        if not 'names_processed' in self.dinfo:
            return path_jday, nTot, nAva, missing_files

        names_processed = self.dinfo['names_processed']
        names_list = names_processed.split(',')
        nTot = len(names_list)
        if not os.path.exists(path_jday):
            missing_files = names_list
            return path_jday, nTot, nAva, missing_files

        for name_file in names_list:
            name_file = name_file.strip()
            date_file_str = datehere.strftime(self.dinfo['format_date_processed'])
            file_path = os.path.join(path_jday, name_file.replace('DATE', date_file_str))
            if os.path.exists(file_path):
                nAva = nAva + 1
            else:
                missing_files.append(name_file)
        return path_jday, nTot, nAva, missing_files

    def get_session_id(self, mode, date):
        sinfo = SourceInfo('202207')
        if self.dinfo['sensor'] == 'MULTI' or self.dinfo['sensor'] == 'gapfree_multi':
            sinfo.start_source('MULTI')
            sinfo.get_last_session_id(mode, self.dinfo['region'], date)
        return sinfo.sessionid

    def get_size_file_path_orig_olci_monthly(self, path, datehere, dtype):
        tamgb = -1
        if path is None:
            path = self.get_path_orig(datehere.year)
        if path is None:
            return tamgb
        y = datehere.year
        m = datehere.month
        day_ini = 1
        day_fin = calendar.monthrange(y, m)[1]
        date_here_ini = dt(y, m, day_ini)
        date_here_fin = dt(y, m, day_fin)
        ystr = date_here_ini.strftime('%Y')
        dini = date_here_ini.strftime('%j')
        dfin = date_here_fin.strftime('%j')
        datestr = f'{ystr}{dini}{dfin}'
        if dtype == 'rrs':
            varlist = ['rrs400', 'rrs412_5', 'rrs442_5', 'rrs490', 'rrs510', 'rrs560', 'rrs620', 'rrs665', 'rrs673_75',
                       'rrs681_25', 'rrs708_75']
        if dtype == 'plankton':
            varlist = ['chl']
        if dtype == 'transp':
            varlist = ['kd490']
        area = self.dinfo['region'].lower()
        if area == 'blk':
            area = 'bs'
        tam = 0

        for var in varlist:
            fname = f'O{datestr}-{var}_monthly-{area}-fr.nc'
            fpath = os.path.join(path, fname)
            if os.path.exists(fpath):
                tam = tam + os.path.getsize(fpath)
            else:
                tamgb = -1
                break
        if tam > 0:
            tamkb = tam / 1024
            tammb = tamkb / 1024
            tamgb = tammb / 1024
            print('final: ', tamgb)

        return tamgb

    def get_size_file_path_orig_olci(self, path, datehere, dtype):
        tagprint = self.get_tag_print()
        tamgb = -1
        if path is None:
            path = self.get_path_orig(datehere.year)
        if path is None:
            return tamgb
        path_jday = os.path.join(path, datehere.strftime('%j'))
        if not os.path.exists(path_jday):
            if tagprint is not None:
                print(f'{tagprint} Expected jday path {path_jday} does not exist')
            return tamgb

        if dtype == 'rrs':
            varlist = ['rrs400', 'rrs412_5', 'rrs442_5', 'rrs490', 'rrs510', 'rrs560', 'rrs620', 'rrs665', 'rrs673_75',
                       'rrs681_25', 'rrs708_75']
        if dtype == 'plankton':
            varlist = ['chl']
        if dtype == 'transp':
            varlist = ['kd490']
        datestr = datehere.strftime('%Y%j')
        area = self.dinfo['region'].lower()
        if area == 'blk':
            area = 'bs'
        tam = 0

        for var in varlist:
            fname = f'O{datestr}-{var}-{area}-fr.nc'
            fpath = os.path.join(path_jday, fname)
            # print(fpath,'->',os.path.exists(fpath))
            if os.path.exists(fpath):
                tam = tam + os.path.getsize(fpath)
                # print(tam)
            else:
                tamgb = -1
                break
        if tam > 0:
            tamkb = tam / 1024
            tammb = tamkb / 1024
            tamgb = tammb / 1024
            print('final: ', tamgb)

        return tamgb

    def get_size_file_path_orig_multi(self, path, datehere, dtype):
        tagprint = self.get_tag_print()
        tamgb = -1
        if path is None:
            path = self.get_path_orig(datehere.year)
        if path is None:
            return tamgb
        path_jday = os.path.join(path, datehere.strftime('%j'))
        if not os.path.exists(path_jday):
            if tagprint is not None:
                print(f'{tagprint} Expected jday path {path_jday} does not exist')
            return tamgb

        if dtype == 'rrs':
            varlist = ['rrs412', 'rrs443', 'rrs490', 'rrs510', 'rrs555', 'rrs670']
        if dtype == 'plankton':
            varlist = ['chl', 'pft']
        if dtype == 'gapfree':
            varlist = ['chl_interp']
        if dtype == 'transp':
            varlist = ['kd490']
        if dtype == 'optics':
            varlist = ['adg443', 'bbp443', 'aph443']
        datestr = datehere.strftime('%Y%j')
        area = self.dinfo['region'].lower()
        if area == 'blk':
            area = 'bs'
        tam = 0

        for var in varlist:
            fname = f'X{datestr}-{var}-{area}-hr.nc'
            fpath = os.path.join(path_jday, fname)
            # print(fpath,'->',os.path.exists(fpath))
            if os.path.exists(fpath):
                tam = tam + os.path.getsize(fpath)
                # print(tam)
            else:
                tamgb = -1
                break
        if tam > 0:
            tamkb = tam / 1024
            tammb = tamkb / 1024
            tamgb = tammb / 1024
            print('final: ', tamgb)

        return tamgb

    def get_size_file_path_orig_multi_monthly(self, path, datehere, dtype):
        tamgb = -1
        if path is None:
            path = self.get_path_orig(datehere.year)
        if path is None:
            return tamgb
        y = datehere.year
        m = datehere.month
        day_ini = 1
        day_fin = calendar.monthrange(y, m)[1]
        date_here_ini = dt(y, m, day_ini)
        date_here_fin = dt(y, m, day_fin)
        ystr = date_here_ini.strftime('%Y')
        dini = date_here_ini.strftime('%j')
        dfin = date_here_fin.strftime('%j')
        datestr = f'{ystr}{dini}{dfin}'
        if dtype == 'rrs':
            varlist = ['rrs400', 'rrs412_5', 'rrs442_5', 'rrs490', 'rrs510', 'rrs560', 'rrs620', 'rrs665', 'rrs673_75',
                       'rrs681_25', 'rrs708_75']
        if dtype == 'plankton':
            varlist = ['chl']
        if dtype == 'transp':
            varlist = ['kd490']
        area = self.dinfo['region'].lower()
        if area == 'blk':
            area = 'bs'
        tam = 0

        for var in varlist:
            fname = f'X{datestr}-{var}_monthly-{area}-hr.nc'
            fpath = os.path.join(path, fname)
            if os.path.exists(fpath):
                tam = tam + os.path.getsize(fpath)
            else:
                tamgb = -1
                break
        if tam > 0:
            tamkb = tam / 1024
            tammb = tamkb / 1024
            tamgb = tammb / 1024
            print('final: ', tamgb)

        return tamgb

    def get_list_file_path_orig(self, start_date, end_date):
        filelist = []
        for y in range(start_date.year, end_date.year + 1):
            path_ref = self.get_path_orig(y)
            for m in range(start_date.month, end_date.month + 1):
                day_ini = 1
                day_fin = calendar.monthrange(y, m)[1]
                if m == start_date.month:
                    day_ini = start_date.day
                if m == end_date.month:
                    day_fin = end_date.day

                for d in range(day_ini, day_fin + 1):
                    datehere = dt(y, m, d)
                    file = self.get_file_path_orig(path_ref, datehere)
                    filelist.append(file)
        return filelist

    def check_size_file_orig(self, start_date, end_date, opt, verbose):
        df = pd.DataFrame(columns=['N', 'Size'], index=list(range(1, 13)))
        for m in list(range(1, 13)):
            df.loc[m, 'N'] = 0
            df.loc[m, 'Size'] = 0

        for y in range(start_date.year, end_date.year + 1):
            path_ref = self.get_path_orig(y)
            mini = 1
            mfin = 12
            if y == start_date.year:
                mini = start_date.month
            if y == end_date.year:
                mfin = end_date.month
            for m in range(mini, mfin + 1):
                if verbose:
                    print(f'[INFO] Checking size for year: {y} Month: {m}')
                day_ini = 1
                day_fin = calendar.monthrange(y, m)[1]
                if m == start_date.month and y == start_date.year:
                    day_ini = start_date.day
                if m == end_date.month and y == end_date.year:
                    day_fin = end_date.day

                for d in range(day_ini, day_fin + 1):
                    datehere = dt(y, m, d)
                    print(datehere)
                    if opt is None:
                        file = self.get_file_path_orig(path_ref, datehere)
                        if not file is None and os.path.exists(file):
                            df.loc[m, 'N'] = df.loc[m, 'N'] + 1
                            tbytes = os.path.getsize(file)
                            tkb = tbytes / 1024
                            tmb = tkb / 1024
                            tgb = tmb / 1024
                            df.loc[m, 'Size'] = df.loc[m, 'Size'] + tgb
                    else:
                        if opt == 'olci_rrs':
                            tgb = self.get_size_file_path_orig_olci(path_ref, datehere, 'rrs')
                        elif opt == 'olci_plankton':
                            tgb = self.get_size_file_path_orig_olci(path_ref, datehere, 'plankton')
                        elif opt == 'olci_transp':
                            tgb = self.get_size_file_path_orig_olci(path_ref, datehere, 'transp')
                        elif opt == 'multi_rrs':
                            tgb = self.get_size_file_path_orig_multi(path_ref, datehere, 'rrs')
                        elif opt == 'multi_plankton':
                            tgb = self.get_size_file_path_orig_multi(path_ref, datehere, 'plankton')
                        elif opt == 'multi_transp':
                            tgb = self.get_size_file_path_orig_multi(path_ref, datehere, 'transp')
                        elif opt == 'multi_optics':
                            tgb = self.get_size_file_path_orig_multi(path_ref, datehere, 'optics')
                        elif opt == 'multi_gapfree':
                            tgb = self.get_size_file_path_orig_multi(path_ref, datehere, 'gapfree')
                        if tgb > 0:
                            df.loc[m, 'N'] = df.loc[m, 'N'] + 1
                            df.loc[m, 'Size'] = df.loc[m, 'Size'] + tgb
        return df

    def check_size_file_orig_monthly(self, start_date, end_date, opt, verbose):
        df = pd.DataFrame(columns=['N', 'Size'], index=list(range(1, 13)))
        for m in list(range(1, 13)):
            df.loc[m, 'N'] = 0
            df.loc[m, 'Size'] = 0

        for y in range(start_date.year, end_date.year + 1):
            path_ref = self.get_path_orig(y)
            mini = 1
            mfin = 12
            if y == start_date.year:
                mini = start_date.month
            if y == end_date.year:
                mfin = end_date.month
            for m in range(mini, mfin + 1):
                if verbose:
                    print(f'[INFO] Checking size for year: {y} Month: {m}')
                datehere = dt(y, m, 15)
                if opt == 'olci_m_rrs':
                    tgb = self.get_size_file_path_orig_olci_monthly(path_ref, datehere, 'rrs')
                elif opt == 'olci_m_plankton':
                    tgb = self.get_size_file_path_orig_olci_monthly(path_ref, datehere, 'plankton')
                elif opt == 'olci_m_transp':
                    tgb = self.get_size_file_path_orig_olci_monthly(path_ref, datehere, 'transp')
                elif opt == 'multi_m_plankton':
                    tgb = self.get_size_file_path_orig_multi_monthly(path_ref, datehere, 'plankton')
                if tgb > 0:
                    df.loc[m, 'N'] = df.loc[m, 'N'] + 1
                    df.loc[m, 'Size'] = df.loc[m, 'Size'] + tgb
        return df

    # Note: dinfo must be defined
    def check_sources(self, date):
        if len(self.dinfo) == 0:
            return
        if self.dinfo['sensor'] == 'MULTI':
            self.check_multi_sources(date)

    def check_multi_sources(self, date):
        print('checking multi sources...')

    def delete_list_file_path_orig(self, start_date, end_date, verbose):
        for y in range(start_date.year, end_date.year + 1):
            mini = 1
            mfin = 12
            if y == start_date.year:
                mini = start_date.month
            if y == end_date.year:
                mfin = end_date.month
            path_ref = self.get_path_orig(y)

            for m in range(mini, mfin + 1):
                day_ini = 1
                day_fin = calendar.monthrange(y, m)[1]
                if m == start_date.month:
                    day_ini = start_date.day
                if m == end_date.month:
                    day_fin = end_date.day
                for d in range(day_ini, day_fin + 1):
                    datehere = dt(y, m, d)
                    if verbose:
                        print('----------------------------------------------------------------------------------')
                        print(f'[INFO] Checking date: {datehere}')
                    file = self.get_file_path_orig(path_ref, datehere)
                    if not file is None and os.path.exists(file):
                        if verbose:
                            print(f'[INFO] Removing file {file}')
                        os.remove(file)

    def delete_list_file_path_orig_monthly(self, start_date, end_date, verbose):
        for y in range(start_date.year, end_date.year + 1):
            path_ref = self.get_path_orig(y)
            mini = 1
            mfin = 12
            if y == start_date.year:
                mini = start_date.month
            if y == end_date.year:
                mfin = end_date.month
            for m in range(mini, mfin + 1):
                datehere = dt(y, m, 15)
                if verbose:
                    print('----------------------------------------------------------------------------------')
                    print(f'[INFO] Checking date: {datehere}')
                file = self.get_file_path_orig_monthly(path_ref, datehere)
                if not file is None and os.path.exists(file):
                    if verbose:
                        print(f'[INFO] Removing file {file}')
                    os.remove(file)

    def get_file_path_orig_monthly(self, path, datehere):
        if path is None:
            path = self.get_path_orig(datehere.year)
        if path is None:
            return None
        name_file = self.dinfo['name_origin']
        yearstr = datehere.strftime('%Y')
        dateinimonth = datehere.replace(day=1).strftime('%j')
        last_day = calendar.monthrange(datehere.year, datehere.month)[1]
        datefinmonth = datehere.replace(day=last_day).strftime('%j')
        date_file_str = f'{yearstr}{dateinimonth}{datefinmonth}'
        file_path = os.path.join(path, name_file.replace('DATE', date_file_str))
        if not os.path.exists(file_path):
            print(f'[ERROR] Expected file orig path {file_path} does not exist')
            return None
        return file_path

    def get_file_path_orig_monthly_noreformat(self, path, datehere):
        if path is None:
            path = self.get_path_orig(datehere.year)
        if path is None:
            return None
        name_file = self.dinfo['name_origin']
        name_file = name_file.replace('CMEMS2_', '')
        yearstr = datehere.strftime('%Y')
        dateinimonth = datehere.replace(day=1).strftime('%j')
        last_day = calendar.monthrange(datehere.year, datehere.month)[1]
        datefinmonth = datehere.replace(day=last_day).strftime('%j')
        date_file_str = f'{yearstr}{dateinimonth}{datefinmonth}'
        file_path = os.path.join(path, name_file.replace('DATE', date_file_str))
        if not os.path.exists(file_path):
            print(f'[ERROR] Expected file orig path {file_path} does not exist')
            return None
        return file_path

    def get_list_file_path_orig_monthly(self, start_date, end_date):
        filelist = []
        for y in range(start_date.year, end_date.year + 1):
            path_ref = self.get_path_orig(y)
            for m in range(start_date.month, end_date.month + 1):
                datehere = dt(y, m, 15)
                file = self.get_file_path_orig_monthly(path_ref, datehere)
                filelist.append(file)
        return filelist

    def get_file_path_orig_climatology(self, path, datehere, reformatted, noneifnoexist):

        key_name_origin = 'name_origin'
        if not reformatted:
            key_name_origin = 'name_origin_noreformatted'

        if path is None:
            path_month = os.path.join(self.dinfo['path_origin'], datehere.strftime('%m'))
            if os.path.exists(path_month):
                path = path_month
            else:
                path = self.dinfo['path_origin']
        if not os.path.exists(path):
            if noneifnoexist:
                print(f'[ERROR] Expected  orig path {path} does not exist')
                return None
        name_file = self.dinfo[key_name_origin]
        date_file_str = datehere.strftime(self.dinfo['format_date_origin'])
        file_path = os.path.join(path, name_file.replace('DATE', date_file_str))
        if not os.path.exists(file_path):
            if noneifnoexist:
                print(f'[ERROR] Expected file orig path {file_path} does not exist')
                return None
        return file_path

    def get_global_atts(self):
        file_attributes = self.dinfo['global_attributes_file']
        if not os.path.exists(file_attributes):
            return None

        import configparser
        try:
            options = configparser.ConfigParser()
            options.read(file_attributes)
        except:
            return None
        if not options.has_section('GLOBAL_ATTRIBUTES'):
            return None
        at_dict = dict(options['GLOBAL_ATTRIBUTES'])

        return at_dict

    def get_remote_path(self, year, month):
        dtref = dt(year, month, 1)
        rpath = os.path.join(os.sep, self.product_name, self.dinfo['remote_dataset'] + self.dinfo['remote_dataset_tag'])
        sdir = os.path.join(dtref.strftime('%Y'), dtref.strftime('%m'))
        return rpath, sdir

    def get_remote_path_monthly(self, year):
        dtref = dt(year, 1, 1)
        rpath = os.path.join(os.sep, self.product_name, self.dinfo['remote_dataset'] + self.dinfo['remote_dataset_tag'])
        sdir = os.path.join(dtref.strftime('%Y'))
        return rpath, sdir

    def get_remote_path_normal(self, year, month):
        dtref = dt(year, month, 1)
        rpath = os.path.join(os.sep, 'Core', self.product_name, self.dinfo['remote_dataset'])
        sdir = os.path.join(dtref.strftime('%Y'), dtref.strftime('%m'))
        return rpath, sdir

    def get_remote_path_monthly_normal(self, year):
        dtref = dt(year, 1, 1)
        rpath = os.path.join(os.sep, 'Core', self.product_name, self.dinfo['remote_dataset'])
        sdir = os.path.join(dtref.strftime('%Y'))
        return rpath, sdir

    def get_remote_path_climatology(self):
        rpath = os.path.join(os.sep, self.product_name, self.dinfo['remote_dataset'] + self.dinfo['remote_dataset_tag'])
        sdir = ''
        return rpath, sdir

    def get_tagged_dataset(self):
        tagged_dataset = self.dinfo['remote_dataset'] + self.dinfo['remote_dataset_tag']
        return tagged_dataset

    def check_file(self, file):
        if file is None:
            print(f'[ERROR] File variable is none')
            return False
        if not os.path.exists(file):
            print(f'[ERROR] File {file} does not exist')
            return False
        try:
            nc = Dataset(file)
            check = False
            if nc.title == self.dataset_name and nc.cmems_product_id == self.product_name:
                check = True
            else:
                if nc.title != self.dataset_name:
                    print(f'[ERROR] title atribute: {nc.title} should be equal to dataset name: {self.dataset_name}')
                if nc.cmems_product_id == self.product_name:
                    print(
                        f'[ERROR] cmems_product_id {nc.cmems_product_id} should be equal to product name {self.product_name}')
            if check:
                variable_list = self.dinfo['variables'].split(',')
                for variable in variable_list:
                    if not variable in nc.variables:
                        print(f'[ERROR] Variable: {variable} was not found in reformatted file: {file}')
                        check = False

            nc.close()
            return check
        except:
            return False

    def get_remote_file_name(self, datehere):
        name_file_base = self.dinfo['remote_file_name']
        date_file_str = datehere.strftime(self.dinfo['remote_date_format'])
        name = name_file_base.replace('DATE', date_file_str)
        return name

    def get_remote_file_name_monthly(self, datehere):
        name_file_base = self.dinfo['remote_file_name']
        dateinimonth = datehere.replace(day=1)
        last_day = calendar.monthrange(datehere.year, datehere.month)[1]
        datefinmonth = datehere.replace(day=last_day)
        date_file_ini_str = dateinimonth.strftime(self.dinfo['remote_date_format'])
        date_file_fin_str = datefinmonth.strftime(self.dinfo['remote_date_format'])
        name = name_file_base.replace('DATE1', date_file_ini_str)
        name = name.replace('DATE2', date_file_fin_str)
        return name

    def get_remote_file_name_climatology(self, datehere):
        name_file_base = self.dinfo['remote_file_name']
        date_file_str = datehere.strftime(self.dinfo['remote_date_format'])
        name = name_file_base.replace('DATE', date_file_str)
        return name

    def get_pinfomy_equivalent(self):
        if len(self.dinfo) == 0:
            return None
        if "myproduct" in self.dinfo.keys() and "mydataset" in self.dinfo.keys():
            pmyinfo = ProductInfo()
            pmyinfo.path2info = self.path2info
            pmyinfo.set_dataset_info(self.dinfo['myproduct'], self.dinfo['mydataset'])
            return pmyinfo
        else:
            return None

    def get_file_slurm_reformat_202411(self, datehere):
        f = self.dinfo['f-option']

        file_slurm = None

        if f == 'D' or f == 'INTERP':
            path = os.path.join(self.dinfo['path_origin'], datehere.strftime('%Y'), datehere.strftime('%j'))
            if not os.path.exists(path):
                print(f'[ERROR] Input path {path} does not exist. Reformat can not be done')
                return [None]*2
            file_slurm = os.path.join(path, f'reformat_{self.dataset_name}.slurm')

        return path, file_slurm

    def get_files_reformat(self, path, datehere):
        if not 'files_reformat' in self.dinfo.keys(): return None
        str_r = self.dinfo['files_reformat']
        format_date_r = self.dinfo['format_date_files_reformat']
        include_qi_bands = True if self.dinfo['include_qi_bands']=="1" else False

        ldictfiles = [x.strip() for x in str_r.split(';')]

        first_file = None
        var_to_delete = []
        rename_qi_to = None
        no_qi_bands = []
        extra_files = {}
        added_lat_lon_time = [False] * 3

        for ifile in range(len(ldictfiles)):
            ldict = ldictfiles[ifile]
            info = [x.strip() for x in ldict.split(',')]
            name_file = info[0]
            name_file_date = name_file.replace('DATE', datehere.strftime(format_date_r))
            file_date = os.path.join(path, name_file_date)
            if not os.path.exists(file_date):
                print(f'[ERROR] {file_date} does not exist. Please review ')
                return [None] * 5
            var_config = []
            if (len(info) == 2 and info[1] != '*') or len(info) > 2:
                for ivar in range(1, len(info)):
                    var_config.append(info[ivar])

            dataset = Dataset(file_date)
            variables = list(dataset.variables)
            dataset.close()
            if ifile == 0:
                first_file = file_date
            var_to_include = {}
            for var_name in variables:
                if var_name.lower().startswith('lat'):
                    if ifile == 0: added_lat_lon_time[0] = True
                    continue
                if var_name.lower().startswith('lon'):
                    if ifile == 0: added_lat_lon_time[1] = True
                    continue
                if var_name.lower().startswith('time'):
                    if ifile==0:added_lat_lon_time[2] = True
                    continue

                if include_qi_bands and var_name.startswith('QI'): continue
                if not include_qi_bands and var_name.startswith('QI') and ifile == 0: var_to_delete.append(var_name)

                if len(info) == 2 and info[1] == '*':
                    var_to_include[var_name] = None
                else:
                    if var_name in var_config:
                        var_to_include[var_name] = None
                    else:
                        if ifile == 0: var_to_delete.append(var_name)

            if include_qi_bands:
                if ifile == 0:
                    if 'QI' in variables:
                        if len(var_to_include) == 1:
                            rename_qi_to = f'QI_{list(var_to_include.keys())[0]}'
                        else:
                            print(f'[ERROR] Error in file {first_file}: ')
                            print(
                            f'[ERROR] Variable QI could not be correctly assigned to a data variable, as more than one variable ({list(var_to_include.keys())}) are available')
                            return [None] * 5
                    else:
                        for var_name in var_to_include:
                            qi_var = f'QI_{var_name}'
                            if not qi_var in variables:
                                no_qi_bands.append(var_name)
                else:
                    if 'QI' in variables:
                        if len(var_to_include) == 1:
                            name_var = list(var_to_include.keys())[0]
                            var_to_include[name_var] = 'QI'
                        else:
                            print(f'[ERROR] Error in file {file_date}: ')
                            print(
                                f'[ERROR] Variable QI could not be correctly assigned to a data variable, as more than one variable ({list(var_to_include.keys())}) are available')
                            return [None] * 5

                    for var_name in var_to_include:
                        var_qi = f'QI_{var_name}'
                        if var_qi in variables:
                            var_to_include[var_name] = var_qi

            if ifile > 0:
                extra_files[file_date] = var_to_include

        if sum(added_lat_lon_time) < 3:
            print(f'[ERROR] Lat, lon or time variable are not defined in file {first_file}')
            return [None] * 5
        return first_file, var_to_delete, rename_qi_to, no_qi_bands, extra_files

    def get_reformat_cmd_202411(self, datehere):
        path, file_slurm = self.get_file_slurm_reformat_202411(datehere)
        if path is None:
            return None



        #f = self.dinfo['f-option']
        first_file, var_to_delete, rename_qi_to, no_qi_bands, extra_files = self.get_files_reformat(path, datehere)
        file_tmp = os.path.join(path, 'Temp.nc')

        fw = open(file_slurm, 'w')
        fw.write('#!/bin/bash')
        for param_slurm in self.params_slurm:
            if self.params_slurm[param_slurm] is not None:
                line = f'SBATCH --{param_slurm}=self.params_slurm[param_slurm]'
                self.add_new_line(fw, line)
        self.add_new_line(fw, '')
        ##self.add_new_line(fw,'source /home/$USER/load_miniconda3.source')
        ##self.add_new_line(fw,'conda activate op_proc_202211v2')

        ##copy first file as file out
        file_out = self.get_file_path_orig_name(None, datehere)


        self.add_new_line(fw, f'cp {first_file} {file_out}')
        self.add_new_line(fw, '')

        # delete variables not to be included in the reformat
        if len(var_to_delete) > 0:
            self.add_new_line(fw, '##Deleting variables...')
            for var_name in var_to_delete:
                self.add_new_line(fw, f'cp {file_out} {file_tmp}')
                self.add_new_line(fw, f'ncks -h  -C -O -x -v {var_name} {file_tmp} {file_out}')
                self.add_new_line(fw, f'rm {file_tmp}')
                self.add_new_line(fw, '')
        self.add_new_line(fw, '')

        ##rename qi
        if rename_qi_to is not None:#
            self.add_new_line(fw, f'##Renaming QI variable to {rename_qi_to}...')
            self.add_new_line(fw, f'ncrename --no_abc -h -a -v QI,{rename_qi_to}  {file_out} >/dev/null')
            self.add_new_line(fw,'')
        self.add_new_line(fw,'')

        #global parameters
        self.add_new_line(fw, f'##Setting global attributes...')
        self.add_new_line(fw, f'ncatted -h -a title,global,o,c,\"{self.dataset_name}\" {file_out}')
        self.add_new_line(fw, f'ncatted -h -a cmems_product_id,global,o,c,\"{self.product_name}\" {file_out}')
        self.add_new_line(fw, f'ncatted -h -a parameter_code,global,o,c,\"{self.dinfo["parameter"]}\" {file_out}')
        self.add_new_line(fw, f'ncatted -h -a parameter,global,o,c,\"{self.dinfo["parameter_code"]}\" {file_out}')
        self.add_new_line(fw,'')
        self.add_new_line(fw, '')

        #extra variables
        for file_in in extra_files:
            self.add_new_line(fw, f'## Addding variables from file {file_in}:')
            var_to_include = extra_files[file_in]
            for var in var_to_include:
                self.add_new_line(fw, f'## ----- {var}')
                var_qi = var_to_include[var]
                if var_qi is not None:
                    var_qi_expected = f'QI_{var}'
                    self.add_new_line(fw, f'cp {file_in} {file_tmp}')
                    if var_qi_expected!=var_qi: ##rename must be done first
                        self.add_new_line(fw,f'ncrename --no_abc -h -a -v {var_qi},{var_qi_expected}  {file_tmp} >/dev/null')
                    self.add_new_line(fw,f'ncks -h  -A -v {var} {file_tmp} {file_out}')
                    self.add_new_line(fw, f'ncks -h  -A -v {var_qi_expected} {file_tmp} {file_out}')
                    self.add_new_line(fw, f'rm {file_tmp}')
                else:
                    self.add_new_line(fw, f'ncks -h  -A -v {var} {file_in} {file_out}')
                    no_qi_bands.append(var)
                self.add_new_line(fw, '## ------')
            self.add_new_line(fw,'')

        self.add_new_line(fw,'')
        self.add_new_line(fw,'## Adding no_qi global attribute')
        noqi_id = ",".join(no_qi_bands)
        self.add_new_line(fw,f'ncatted -h -a noQI,global,o,c,\"{noqi_id}\" {file_out}')

        fw.close()

        ##creating cmd
        cmd = f'sbatch --wait {file_slurm}'

        return cmd

    def add_new_line(self, fw, line):
        fw.write('\n')
        fw.write(line)

    def get_reformat_cmd(self, datehere):
        cmd = None
        # if self.dinfo['dataset'] == 'climatology':
        #     print(f'[WARNING] Reformat code for climatology is not implemented...')
        #     return cmd
        regions_dict = {
            'BAL': 'BAL',
            'BLK': 'BS',
            'MED': 'MED'
        }
        res = self.dinfo['res']
        m = self.dinfo['mode']
        r = regions_dict[self.dinfo['region']]
        f = self.dinfo['f-option']
        p = self.dinfo['dataset']

        if f == 'D' or f == 'INTERP':
            path = os.path.join(self.dinfo['path_origin'], datehere.strftime('%Y'), datehere.strftime('%j'))
            if not os.path.exists(path):
                print(f'[ERROR] Input path {path} does not exist. Reformat can not be done')
                return None
            if self.path_reformat_script == '/home/lois/PycharmProjects/reformatting_file_cmems2_202211.sh':
                cmd = f'{self.path_reformat_script} -res {res} -m {m} -r {r} -f {f} -p {p} -path {path}'
            else:
                cmd = f'sh {self.path_reformat_script} -res {res} -m {m} -r {r} -f {f} -p {p} -path {path}'

        if f == 'C':  # climatology
            path = self.dinfo['path_origin']
            cmd = f'sh {self.path_reformat_script} -res {res} -m {m} -r {r} -f {f} -p {p} -path {path}'

        if f == 'M':
            path = os.path.join(self.dinfo['path_origin'], datehere.strftime('%Y'))
            if not os.path.exists(path):
                print(f'[ERROR] Input path {path} does not exist. Reformat can not be done')
                return None
            d = datehere.strftime('%Y-%m')

            cmd = f'sh {self.path_reformat_script} -res {res} -m {m} -r {r} -f {f} -p {p} -path {path} -d {d}'

        return cmd

    def start_my_dictionary(self):
        self.dict_info['my'] = {
            'bal': {
                'l3': {
                    'plankton': {
                        'multi': {
                            'product': 'OCEANCOLOUR_BAL_BGC_L3_MY_009_133',
                            'dataset': 'cmems_obs-oc_bal_bgc-plankton_my_l3-multi-1km_P1D',
                            'frequency': 'd'
                        }
                    },
                    'reflectance': {
                        'multi': {
                            'product': 'OCEANCOLOUR_BAL_BGC_L3_MY_009_133',
                            'dataset': 'cmems_obs-oc_bal_bgc-reflectance_my_l3-multi-1km_P1D',
                            'frequency': 'd'
                        }
                    },
                    'transp': {
                        'multi': {
                            'product': 'OCEANCOLOUR_BAL_BGC_L3_MY_009_133',
                            'dataset': 'cmems_obs-oc_bal_bgc-transp_my_l3-multi-1km_P1D',
                            'frequency': 'd'
                        }
                    }

                }
            },
            'med': {
                'l3': {
                    'plankton': {
                        'multi': {
                            'product': 'OCEANCOLOUR_MED_BGC_L3_MY_009_143',
                            'dataset': 'cmems_obs-oc_med_bgc-plankton_my_l3-multi-1km_P1D',
                            'frequency': 'd'
                        }
                    },
                    'reflectance': {
                        'multi': {
                            'product': 'OCEANCOLOUR_MED_BGC_L3_MY_009_143',
                            'dataset': 'cmems_obs-oc_med_bgc-reflectance_my_l3-multi-1km_P1D',
                            'frequency': 'd'
                        }
                    },
                    'transp': {
                        'multi': {
                            'product': 'OCEANCOLOUR_MED_BGC_L3_MY_009_143',
                            'dataset': 'cmems_obs-oc_med_bgc-transp_my_l3-multi-1km_P1D',
                            'frequency': 'd'
                        }
                    },
                    'optics': {
                        'multi': {
                            'product': 'OCEANCOLOUR_MED_BGC_L3_MY_009_143',
                            'dataset': 'cmems_obs-oc_med_bgc-optics_my_l3-multi-1km_P1D',
                            'frequency': 'd'
                        }
                    }
                },
                'l4': {
                    'plankton': {
                        'multi': {
                            'product': 'OCEANCOLOUR_MED_BGC_L4_MY_009_144',
                            'dataset': 'cmems_obs-oc_med_bgc-plankton_my_l4-multi-1km_P1M',
                            'frequency': 'm'
                        },
                        'gapfree_multi': {
                            'product': 'OCEANCOLOUR_MED_BGC_L4_MY_009_144',
                            'dataset': 'cmems_obs-oc_med_bgc-plankton_my_l4-gapfree-multi-1km_P1D',
                            'frequency': 'd'

                        },
                        'multi-climatology': {
                            'product': 'OCEANCOLOUR_MED_BGC_L4_MY_009_144',
                            'dataset': 'cmems_obs-oc_med_bgc-plankton_my_l4-multi-climatology-1km_P1D',
                            'frequency': 'd'
                        }
                    }
                }
            },
            'blk': {
                'l3': {
                    'plankton': {
                        'multi': {
                            'product': 'OCEANCOLOUR_BLK_BGC_L3_MY_009_153',
                            'dataset': 'cmems_obs-oc_blk_bgc-plankton_my_l3-multi-1km_P1D',
                            'frequency': 'd'
                        }
                    },
                    'reflectance': {
                        'multi': {
                            'product': 'OCEANCOLOUR_BLK_BGC_L3_MY_009_153',
                            'dataset': 'cmems_obs-oc_blk_bgc-reflectance_my_l3-multi-1km_P1D',
                            'frequency': 'd'
                        }
                    },
                    'transp': {
                        'multi': {
                            'product': 'OCEANCOLOUR_BLK_BGC_L3_MY_009_153',
                            'dataset': 'cmems_obs-oc_blk_bgc-transp_my_l3-multi-1km_P1D',
                            'frequency': 'd'
                        }
                    },
                    'optics': {
                        'multi': {
                            'product': 'OCEANCOLOUR_BLK_BGC_L3_MY_009_153',
                            'dataset': 'cmems_obs-oc_blk_bgc-optics_my_l3-multi-1km_P1D',
                            'frequency': 'd'
                        }
                    }
                },
                'l4': {
                    'plankton': {
                        'multi': {
                            'product': 'OCEANCOLOUR_BLK_BGC_L4_MY_009_154',
                            'dataset': 'cmems_obs-oc_blk_bgc-plankton_my_l4-multi-1km_P1M',
                            'frequency': 'm'
                        },
                        'gapfree_multi': {
                            'product': 'OCEANCOLOUR_BLK_BGC_L4_MY_009_154',
                            'dataset': 'cmems_obs-oc_blk_bgc-plankton_my_l4-gapfree-multi-1km_P1D',
                            'frequency': 'd'
                        },
                        'multi-climatology': {
                            'product': 'OCEANCOLOUR_BLK_BGC_L4_MY_009_154',
                            'dataset': 'cmems_obs-oc_blk_bgc-plankton_my_l4-multi-climatology-1km_P1D',
                            'frequency': 'd'
                        }
                    }
                }
            }
        }

    def start_nrt_dictionary(self):
        self.dict_info['nrt'] = {
            'bal': {
                'l3': {
                    'plankton': {
                        'olci': {
                            'product': 'OCEANCOLOUR_BAL_BGC_L3_NRT_009_131',
                            'dataset': 'cmems_obs-oc_bal_bgc-plankton_nrt_l3-olci-300m_P1D',
                            'frequency': 'd'
                        }
                    },
                    'reflectance': {
                        'olci': {
                            'product': 'OCEANCOLOUR_BAL_BGC_L3_NRT_009_131',
                            'dataset': 'cmems_obs-oc_bal_bgc-reflectance_nrt_l3-olci-300m_P1D',
                            'frequency': 'd'
                        }
                    },
                    'transp': {
                        'olci': {
                            'product': 'OCEANCOLOUR_BAL_BGC_L3_NRT_009_131',
                            'dataset': 'cmems_obs-oc_bal_bgc-transp_nrt_l3-olci-300m_P1D',
                            'frequency': 'd'
                        }
                    },
                    'optics': {
                        'olci': {
                            'product': 'OCEANCOLOUR_BAL_BGC_L3_NRT_009_131',
                            'dataset': 'cmems_obs-oc_bal_bgc-optics_nrt_l3-olci-300m_P1D',
                            'frequency': 'd'
                        }
                    }

                },
                'l4': {
                    'plankton': {
                        'olci': {
                            'product': 'OCEANCOLOUR_BAL_BGC_L4_NRT_009_132',
                            'dataset': 'cmems_obs-oc_bal_bgc-plankton_nrt_l4-olci-300m_P1M',
                            'frequency': 'm'
                        }
                    }
                }
            },
            'med': {
                'l3': {
                    'plankton': {
                        'multi': {
                            'product': 'OCEANCOLOUR_MED_BGC_L3_NRT_009_141',
                            'dataset': 'cmems_obs-oc_med_bgc-plankton_nrt_l3-multi-1km_P1D',
                            'frequency': 'd'
                        },
                        'olci': {
                            'product': 'OCEANCOLOUR_MED_BGC_L3_NRT_009_141',
                            'dataset': 'cmems_obs-oc_med_bgc-plankton_nrt_l3-olci-300m_P1D',
                            'frequency': 'd'
                        }
                    },
                    'reflectance': {
                        'multi': {
                            'product': 'OCEANCOLOUR_MED_BGC_L3_NRT_009_141',
                            'dataset': 'cmems_obs-oc_med_bgc-reflectance_nrt_l3-multi-1km_P1D',
                            'frequency': 'd'
                        },
                        'olci': {
                            'product': 'OCEANCOLOUR_MED_BGC_L3_NRT_009_141',
                            'dataset': 'cmems_obs-oc_med_bgc-reflectance_nrt_l3-olci-300m_P1D',
                            'frequency': 'd'
                        }
                    },
                    'transp': {
                        'multi': {
                            'product': 'OCEANCOLOUR_MED_BGC_L3_NRT_009_141',
                            'dataset': 'cmems_obs-oc_med_bgc-transp_nrt_l3-multi-1km_P1D',
                            'frequency': 'd'
                        },
                        'olci': {
                            'product': 'OCEANCOLOUR_MED_BGC_L3_NRT_009_141',
                            'dataset': 'cmems_obs-oc_med_bgc-transp_nrt_l3-olci-300m_P1D',
                            'frequency': 'd'
                        }
                    },
                    'optics': {
                        'multi': {
                            'product': 'OCEANCOLOUR_MED_BGC_L3_NRT_009_141',
                            'dataset': 'cmems_obs-oc_med_bgc-optics_nrt_l3-multi-1km_P1D',
                            'frequency': 'd'
                        }
                    }
                },
                'l4': {
                    'plankton': {
                        'multi': {
                            'product': 'OCEANCOLOUR_MED_BGC_L4_NRT_009_142',
                            'dataset': 'cmems_obs-oc_med_bgc-plankton_nrt_l4-multi-1km_P1M',
                            'frequency': 'm'
                        },
                        'olci': {
                            'product': 'OCEANCOLOUR_MED_BGC_L4_NRT_009_142',
                            'dataset': 'cmems_obs-oc_med_bgc-plankton_nrt_l4-olci-300m_P1M',
                            'frequency': 'm'
                        },
                        'gapfree_multi': {
                            'product': 'OCEANCOLOUR_MED_BGC_L4_NRT_009_142',
                            'dataset': 'cmems_obs-oc_med_bgc-plankton_nrt_l4-gapfree-multi-1km_P1D',
                            'frequency': 'd'
                        }
                    },
                    'transp': {
                        'multi': {
                            'product': 'OCEANCOLOUR_MED_BGC_L4_NRT_009_142',
                            'dataset': 'cmems_obs-oc_med_bgc-transp_nrt_l4-multi-1km_P1M',
                            'frequency': 'm'
                        },
                        'olci': {
                            'product': 'OCEANCOLOUR_MED_BGC_L4_NRT_009_142',
                            'dataset': 'cmems_obs-oc_med_bgc-transp_nrt_l4-olci-300m_P1M',
                            'frequency': 'm'
                        }
                    }
                }
            },
            'blk': {
                'l3': {
                    'plankton': {
                        'multi': {
                            'product': 'OCEANCOLOUR_BLK_BGC_L3_NRT_009_151',
                            'dataset': 'cmems_obs-oc_blk_bgc-plankton_nrt_l3-multi-1km_P1D',
                            'frequency': 'd'
                        },
                        'olci': {
                            'product': 'OCEANCOLOUR_BLK_BGC_L3_NRT_009_151',
                            'dataset': 'cmems_obs-oc_blk_bgc-plankton_nrt_l3-olci-300m_P1D',
                            'frequency': 'd'
                        }
                    },
                    'reflectance': {
                        'multi': {
                            'product': 'OCEANCOLOUR_BLK_BGC_L3_NRT_009_151',
                            'dataset': 'cmems_obs-oc_blk_bgc-reflectance_nrt_l3-multi-1km_P1D',
                            'frequency': 'd'
                        },
                        'olci': {
                            'product': 'OCEANCOLOUR_BLK_BGC_L3_NRT_009_151',
                            'dataset': 'cmems_obs-oc_blk_bgc-reflectance_nrt_l3-olci-300m_P1D',
                            'frequency': 'd'
                        }
                    },
                    'transp': {
                        'multi': {
                            'product': 'OCEANCOLOUR_BLK_BGC_L3_NRT_009_151',
                            'dataset': 'cmems_obs-oc_blk_bgc-transp_nrt_l3-multi-1km_P1D',
                            'frequency': 'd'
                        },
                        'olci': {
                            'product': 'OCEANCOLOUR_BLK_BGC_L3_NRT_009_151',
                            'dataset': 'cmems_obs-oc_blk_bgc-transp_nrt_l3-olci-300m_P1D',
                            'frequency': 'd'
                        }
                    },
                    'optics': {
                        'multi': {
                            'product': 'OCEANCOLOUR_BLK_BGC_L3_NRT_009_151',
                            'dataset': 'cmems_obs-oc_blk_bgc-optics_nrt_l3-multi-1km_P1D',
                            'frequency': 'd'
                        }
                    }
                },
                'l4': {
                    'plankton': {
                        'multi': {
                            'product': 'OCEANCOLOUR_BLK_BGC_L4_NRT_009_152',
                            'dataset': 'cmems_obs-oc_blk_bgc-plankton_nrt_l4-multi-1km_P1M',
                            'frequency': 'm'
                        },
                        'olci': {
                            'product': 'OCEANCOLOUR_BLK_BGC_L4_NRT_009_152',
                            'dataset': 'cmems_obs-oc_blk_bgc-plankton_nrt_l4-olci-300m_P1M',
                            'frequency': 'm'
                        },
                        'gapfree_multi': {
                            'product': 'OCEANCOLOUR_BLK_BGC_L4_NRT_009_152',
                            'dataset': 'cmems_obs-oc_blk_bgc-plankton_nrt_l4-gapfree-multi-1km_P1D',
                            'frequency': 'd'
                        }
                    },
                    'transp': {
                        'multi': {
                            'product': 'OCEANCOLOUR_BLK_BGC_L4_NRT_009_152',
                            'dataset': 'cmems_obs-oc_blk_bgc-transp_nrt_l4-multi-1km_P1M',
                            'frequency': 'm'
                        },
                        'olci': {
                            'product': 'OCEANCOLOUR_BLK_BGC_L4_NRT_009_152',
                            'dataset': 'cmems_obs-oc_blk_bgc-transp_nrt_l4-olci-300m_P1M',
                            'frequency': 'm'
                        }
                    }
                }
            },
            'arc': {
                'l3': {
                    'plankton': {
                        'olci': {
                            'product': 'OCEANCOLOUR_ARC_BGC_L3_NRT_009_121',
                            'dataset': 'cmems_obs-oc_arc_bgc-plankton_nrt_l3-olci-300m_P1D_202303',
                            'frequency': 'd'
                        }
                    },
                    'reflectance': {
                        'olci': {
                            'product': 'OCEANCOLOUR_ARC_BGC_L3_NRT_009_121',
                            'dataset': 'cmems_obs-oc_arc_bgc-reflectance_nrt_l3-olci-300m_P1D_202303',
                            'frequency': 'd'
                        }
                    },
                    'transp': {
                        'olci': {
                            'product': 'OCEANCOLOUR_ARC_BGC_L3_NRT_009_121',
                            'dataset': 'cmems_obs-oc_arc_bgc-transp_nrt_l3-olci-300m_P1D_202303',
                            'frequency': 'd'
                        }
                    }
                }
            }
        }
