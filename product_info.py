import calendar
import datetime
import os
import json
from datetime import datetime as dt

import pandas as pd
from netCDF4 import Dataset


class ProductInfo:
    def __init__(self):
        sdir = os.path.abspath(os.path.dirname(__file__))
        # path2script = "/".join(sdir.split("/")[0:-1])
        self.path2info = os.path.join(os.path.dirname(sdir), 'PRODUCT_INFO')
        if self.path2info == '/home/lois/PycharmProjects/PRODUCT_INFO':
            self.path2info = '/mnt/c/DATA_LUIS/OCTAC_WORK/EiSJuly2022/PRODUCT_INFO'

        self.path_reformat_script = '/store3/simone/tmp/reformatting_file_cmems2_lois.sh'

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
        # self.start_nrt_dictionary()

        self.MODE = 'UPLOAD' # UPLOAD O REFORMAT

    def get_dataset_summary(self):
        if self.dataset_name is not None:
            path_summary = os.path.join(self.path2info,'SUMMARY')
            if not os.path.exists(path_summary):
                os.mkdir(path_summary)
            fsummary = os.path.join(path_summary,self.dataset_name+'_summary.json')
            finvalid = os.path.join(path_summary,self.dataset_name+'_invalid.csv')
            return fsummary, finvalid
        return None

    def get_dataset_name(self, mode, basin, level, dtype, sensor):
        res = '1km'
        if sensor.lower() == 'olci':
            res = '300m'
        dataset_name = f'cmems_obs-oc_{basin.lower()}_bgc-{dtype.lower()}_{mode.lower()}_{level.lower()}-{sensor.lower()}-{res}_P1D'
        dinfo = self.dict_info[mode.lower()][basin.lower()][level.lower()][dtype.lower()][sensor.lower()]

        product_name = None
        if dinfo['dataset'] == dataset_name:
            product_name = dinfo['product']

        return product_name, dataset_name

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
        if os.path.exists(fproduct):
            f = open(fproduct, "r")
            self.pinfo = json.load(f)
            f.close()

    def set_dataset_info(self, product_name, dataset_name):
        self.product_name = product_name
        self.dataset_name = dataset_name
        fproduct = os.path.join(self.path2info, product_name + '.json')
        #print(fproduct)
        if os.path.exists(fproduct):
            f = open(fproduct, "r")
            pinfo = json.load(f)
            if dataset_name in pinfo.keys():
                self.dinfo = pinfo[dataset_name]
            f.close()
        else:
            print(f'[ERROR] Product file {fproduct} does not exist')

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

    def get_size_file_path_orig_olcirrs(self,path,datehere):
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
        rrslist = ['400','412_5','442_5','490','510','560','620','665','673_75','681_21','708_75']
        datestr = datehere.strftime('%Y%j')
        area = self.dinfo['format_date_origin'].lower()
        if area=='blk':
            area = 'bs'
        tam = 0
        tamgb = -1
        for rrs in rrslist:
            fname = f'O{datestr}-rrs{rrs}-{area}-fr.nc'
            fpath = os.path.join(path_jday,fname)
            print(fpath)
            if os.path.exists(fpath):
                tam = tam + os.path.getsize(fpath)
            else:
                tam = -1
                break
        if tam>0:
            tamkb = tam/1024
            tammb = tamkb/1024
            tamgb = tammb(1024)

        return tamgb

    def get_list_file_path_orig(self, start_date, end_date):
        filelist = []
        for y in range(start_date.year, end_date.year + 1):
            path_ref = self.get_path_orig(y)
            for m in range(start_date.month, end_date.month + 1):
                day_ini = 1
                day_fin = calendar.monthrange(y, m)[1]
                if m==start_date.month:
                    day_ini = start_date.day
                if m==end_date.month:
                    day_fin = end_date.day

                for d in range(day_ini, day_fin + 1):
                    datehere = dt(y, m, d)
                    file = self.get_file_path_orig(path_ref, datehere)
                    filelist.append(file)
        return filelist

    def check_size_file_orig(self, start_date, end_date, opt, verbose):
        df = pd.DataFrame(columns=['N','Size'],index=list(range(1,13)))
        for m in list(range(1,13)):
            df.loc[m, 'N'] = 0
            df.loc[m, 'Size'] = 0

        for y in range(start_date.year, end_date.year + 1):
            path_ref = self.get_path_orig(y)
            for m in range(start_date.month, end_date.month + 1):
                if verbose:
                    print(f'[INFO] Checking size for year: {y} Month: {m}')
                day_ini = 1
                day_fin = calendar.monthrange(y, m)[1]
                if m==start_date.month:
                    day_ini = start_date.day
                if m==end_date.month:
                    day_fin = end_date.day

                for d in range(day_ini, day_fin + 1):
                    datehere = dt(y, m, d)
                    if opt is None:
                        file = self.get_file_path_orig(path_ref, datehere)
                        if not file is None and os.path.exists(file):
                            df.loc[m,'N'] = df.loc[m,'N']+1
                            tbytes = os.path.getsize(file)
                            tkb =  tbytes/1024
                            tmb = tkb/1024
                            tgb = tmb/1024
                            df.loc[m, 'Size'] = df.loc[m, 'Size'] + tgb
                        elif opt=='olci_rrs':
                            tgb = self.get_size_file_path_orig_olcirrs(path_ref,datehere)
                            if tgb>0:
                                df.loc[m, 'N'] = df.loc[m, 'N'] + 1
                                df.loc[m, 'Size'] = df.loc[m, 'Size'] + tgb


        return df

    def delete_list_file_path_orig(self, start_date, end_date, verbose):
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
                    if verbose:
                        print('----------------------------------------------------------------------------------')
                        print(f'[INFO] Checking date: {datehere}')
                    file = self.get_file_path_orig(path_ref, datehere)
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

    def get_list_file_path_orig_monthly(self,start_date,end_date):
        filelist = []
        for y in range(start_date.year,end_date.year+1):
            path_ref = self.get_path_orig(y)
            for m in range(start_date.month,end_date.month+1):
                datehere = dt(y,m,15)
                file = self.get_file_path_orig_monthly(path_ref,datehere)
                filelist.append(file)
        return filelist

    def get_file_path_orig_climatology(self, path, datehere):
        if path is None:
            return None
        name_file = self.dinfo['name_origin']
        date_file_str = datehere.strftime(self.dinfo['format_date_origin'])
        file_path = os.path.join(path, name_file.replace('DATE', date_file_str))
        if not os.path.exists(file_path):
            print(f'[ERROR] Expected file orig path {file_path} does not exist')
            return None
        return file_path

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

    def get_remote_path_climatology(self):
        rpath = os.path.join(os.sep, self.product_name, self.dinfo['remote_dataset'] + self.dinfo['remote_dataset_tag'])
        sdir = ''
        return rpath, sdir

    def get_tagged_dataset(self):
        tagged_dataset = self.dinfo['remote_dataset'] + self.dinfo['remote_dataset_tag']
        return tagged_dataset

    def check_file(self, file):
        if file is None:
            return False
        if not os.path.exists(file):
            return False
        try:
            nc = Dataset(file)
            check = False
            if nc.title == self.dataset_name and nc.cmems_product_id == self.product_name:
                check = True
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

    def get_reformat_cmd(self, datehere):
        cmd = None
        if self.dinfo['dataset'] == 'climatology':
            print(f'[WARNING] Reformat code for climatology is not implemented...')
            return cmd
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
            cmd = f'sh {self.path_reformat_script} -res {res} -m {m} -r {r} -f {f} -p {p} -path {path}'

        if f == 'M':
            path = os.path.join(self.dinfo['path_origin'], datehere.strftime('%Y'))
            if not os.path.exists(path):
                print(f'[ERROR] Input path {path} does not exist. Reformat can not be done')
                return None
            d = datehere.strftime('%Y-%m')
            cmd = f'sh {self.path_reformat_script} -res {res} -m {m} -r {r} -f {f} -p {p} -path {path} -d {d}'

        return cmd

    def start_nrt_dictionary(self):
        self.dict_info['nrt']['bal']['l3']['plackton']['olci'] = {
            'product': 'OCEANCOLOUR_BAL_BGC_L3_NRT_009_131',
            'dataset': 'cmems_obs-oc_bal_bgc-plankton_nrt_l3-olci-300m_P1D',
            'frequency': 'd'
        }
        self.dict_info['nrt']['bal']['l3']['reflectance']['olci'] = {
            'product': 'OCEANCOLOUR_BAL_BGC_L3_NRT_009_131',
            'dataset': 'cmems_obs-oc_bal_bgc-reflectance_nrt_l3-olci-300m_P1D',
            'frequency': 'd'
        }
        self.dict_info['nrt']['bal']['l3']['transp']['olci'] = {
            'product': 'OCEANCOLOUR_BAL_BGC_L3_NRT_009_131',
            'dataset': 'cmems_obs-oc_bal_bgc-transp_nrt_l3-olci-300m_P1D',
            'frequency': 'd'
        }
        self.dict_info['nrt']['bal']['l3']['optics']['olci'] = {
            'product': 'OCEANCOLOUR_BAL_BGC_L3_NRT_009_131',
            'dataset': 'cmems_obs-oc_bal_bgc-optics_nrt_l3-olci-300m_P1D',
            'frequency': 'd'
        }
        self.dict_info['nrt']['bal']['l4']['plackton']['olci'] = {
            'product': 'OCEANCOLOUR_BAL_BGC_L4_NRT_009_132',
            'dataset': 'cmems_obs-oc_bal_bgc-plankton_nrt_l4-olci-300m_P1M',
            'frequency': 'm'
        }

        # MED
        self.dict_info['nrt']['med']['l3']['plackton']['olci'] = {
            'product': 'OCEANCOLOUR_MED_BGC_L3_NRT_009_141',
            'dataset': 'cmems_obs-oc_med_bgc-plankton_nrt_l3-olci-300m_P1D',
            'frequency': 'd'
        }
        self.dict_info['nrt']['med']['l3']['reflectance']['olci'] = {
            'product': 'OCEANCOLOUR_MED_BGC_L3_NRT_009_141',
            'dataset': 'cmems_obs-oc_med_bgc-reflectance_nrt_l3-olci-300m_P1D',
            'frequency': 'd'
        }
        self.dict_info['nrt']['med']['l3']['transp']['olci'] = {
            'product': 'OCEANCOLOUR_MED_BGC_L3_NRT_009_141',
            'dataset': 'cmems_obs-oc_med_bgc-transp_nrt_l3-olci-300m_P1D',
            'frequency': 'd'
        }

        self.dict_info['nrt']['med']['l3']['plackton']['multi'] = {
            'product': 'OCEANCOLOUR_MED_BGC_L3_NRT_009_141',
            'dataset': 'cmems_obs-oc_med_bgc-plankton_nrt_l3-multi-1km_P1D',
            'frequency': 'd'
        }
        self.dict_info['nrt']['med']['l3']['reflectance']['multi'] = {
            'product': 'OCEANCOLOUR_MED_BGC_L3_NRT_009_141',
            'dataset': 'cmems_obs-oc_med_bgc-reflectance_nrt_l3-multi-1km_P1D',
            'frequency': 'd'
        }
        self.dict_info['nrt']['med']['l3']['transp']['multi'] = {
            'product': 'OCEANCOLOUR_MED_BGC_L3_NRT_009_141',
            'dataset': 'cmems_obs-oc_med_bgc-transp_nrt_l3-multi-1km_P1D',
            'frequency': 'd'
        }
        self.dict_info['nrt']['med']['l3']['optics']['multi'] = {
            'product': 'OCEANCOLOUR_MED_BGC_L3_NRT_009_141',
            'dataset': 'cmems_obs-oc_med_bgc-optics_nrt_l3-multi-1km_P1D',
            'frequency': 'd'
        }

        self.dict_info['nrt']['med']['l4']['plackton']['multi'] = {
            'product': 'OCEANCOLOUR_MED_BGC_L4_NRT_009_142',
            'dataset': 'cmems_obs-oc_med_bgc-plankton_nrt_l4-multi-1km_P1D',
            'frequency': 'm'
        }
        self.dict_info['nrt']['med']['l4']['plackton']['olci'] = {
            'product': 'OCEANCOLOUR_MED_BGC_L4_NRT_009_142',
            'dataset': 'cmems_obs-oc_med_bgc-plankton_nrt_l4-olci-300m_P1M',
            'frequency': 'm'
        }
        self.dict_info['nrt']['med']['l4']['plackton']['gapfree-multi'] = {
            'product': 'OCEANCOLOUR_MED_BGC_L4_NRT_009_142',
            'dataset': 'cmems_obs-oc_med_bgc-plankton_nrt_l4-gapfree-multi-1km_P1D',
            'frequency': 'd'
        }
        self.dict_info['nrt']['med']['l4']['transp']['multi'] = {
            'product': 'OCEANCOLOUR_MED_BGC_L4_NRT_009_142',
            'dataset': 'cmems_obs-oc_med_bgc-transp_nrt_l4-multi-1km_P1D',
            'frequency': 'm'
        }
        self.dict_info['nrt']['med']['l4']['transp']['olci'] = {
            'product': 'OCEANCOLOUR_MED_BGC_L4_NRT_009_142',
            'dataset': 'cmems_obs-oc_med_bgc-transp_nrt_l4-olci-300m_P1M',
            'frequency': 'm'
        }

        # BLK
        self.dict_info['nrt']['blk']['l3']['plackton']['olci'] = {
            'product': 'OCEANCOLOUR_BLK_BGC_L3_NRT_009_151',
            'dataset': 'cmems_obs-oc_blk_bgc-plankton_nrt_l3-olci-300m_P1D',
            'frequency': 'd'
        }
        self.dict_info['nrt']['blk']['l3']['reflectance']['olci'] = {
            'product': 'OCEANCOLOUR_BLK_BGC_L3_NRT_009_151',
            'dataset': 'cmems_obs-oc_blk_bgc-reflectance_nrt_l3-olci-300m_P1D',
            'frequency': 'd'
        }
        self.dict_info['nrt']['blk']['l3']['transp']['olci'] = {
            'product': 'OCEANCOLOUR_BLK_BGC_L3_NRT_009_151',
            'dataset': 'cmems_obs-oc_blk_bgc-transp_nrt_l3-olci-300m_P1D',
            'frequency': 'd'
        }

        self.dict_info['nrt']['blk']['l3']['plackton']['multi'] = {
            'product': 'OCEANCOLOUR_BLK_BGC_L3_NRT_009_151',
            'dataset': 'cmems_obs-oc_blk_bgc-plankton_nrt_l3-multi-1km_P1D',
            'frequency': 'd'
        }
        self.dict_info['nrt']['blk']['l3']['reflectance']['multi'] = {
            'product': 'OCEANCOLOUR_BLK_BGC_L3_NRT_009_151',
            'dataset': 'cmems_obs-oc_blk_bgc-reflectance_nrt_l3-multi-1km_P1D',
            'frequency': 'd'
        }
        self.dict_info['nrt']['blk']['l3']['transp']['multi'] = {
            'product': 'OCEANCOLOUR_BLK_BGC_L3_NRT_009_151',
            'dataset': 'cmems_obs-oc_blk_bgc-transp_nrt_l3-multi-1km_P1D',
            'frequency': 'd'
        }
        self.dict_info['nrt']['blk']['l3']['optics']['multi'] = {
            'product': 'OCEANCOLOUR_BLK_BGC_L3_NRT_009_151',
            'dataset': 'cmems_obs-oc_blk_bgc-optics_nrt_l3-multi-1km_P1D',
            'frequency': 'd'
        }

        self.dict_info['nrt']['blk']['l4']['plackton']['multi'] = {
            'product': 'OCEANCOLOUR_BLK_BGC_L4_NRT_009_152',
            'dataset': 'cmems_obs-oc_blk_bgc-plankton_nrt_l4-multi-1km_P1D',
            'frequency': 'm'
        }
        self.dict_info['nrt']['blk']['l4']['plackton']['olci'] = {
            'product': 'OCEANCOLOUR_BLK_BGC_L4_NRT_009_152',
            'dataset': 'cmems_obs-oc_blk_bgc-plankton_nrt_l4-olci-300m_P1M',
            'frequency': 'm'
        }
        self.dict_info['nrt']['blk']['l4']['plackton']['gapfree-multi'] = {
            'product': 'OCEANCOLOUR_BLK_BGC_L4_NRT_009_152',
            'dataset': 'cmems_obs-oc_blk_bgc-plankton_nrt_l4-gapfree-multi-1km_P1D',
            'frequency': 'd'
        }
        self.dict_info['nrt']['blk']['l4']['transp']['multi'] = {
            'product': 'OCEANCOLOUR_BLK_BGC_L4_NRT_009_152',
            'dataset': 'cmems_obs-oc_blk_bgc-transp_nrt_l4-multi-1km_P1D',
            'frequency': 'm'
        }
        self.dict_info['nrt']['blk']['l4']['transp']['olci'] = {
            'product': 'OCEANCOLOUR_BLK_BGC_L4_NRT_009_152',
            'dataset': 'cmems_obs-oc_blk_bgc-transp_nrt_l4-olci-300m_P1M',
            'frequency': 'm'
        }

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
                            'dataset': 'cmems_obs-oc_med_bgc-plankton_my_l4-multi-1km_P1D',
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
                            'dataset': 'cmems_obs-oc_blk_bgc-plankton_my_l4-multi-1km_P1D',
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
