import os
import json
from datetime import datetime as dt
from netCDF4 import Dataset


class ProductInfo:
    def __init__(self):
        sdir = os.path.abspath(os.path.dirname(__file__))
        # path2script = "/".join(sdir.split("/")[0:-1])
        self.path2info = os.path.join(os.path.dirname(sdir), 'PRODUCT_INFO')
        if self.path2info == '/home/lois/PycharmProjects/PRODUCT_INFO':
            self.path2info = '/mnt/c/DATA_LUIS/OCTAC_WORK/EiSJuly2022/PRODUCT_INFO'
        print(self.path2info)
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
        #self.start_nrt_dictionary()

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
        print(fproduct)
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
        print(product_name,dataset_name)
        self.set_dataset_info(product_name, dataset_name)

    def get_path_orig(self, year):
        if len(self.dinfo) == 0:
            return None
        path_orig = os.path.join(self.dinfo['path_origin'], dt(year, 1, 1).strftime('%Y'))
        if os.path.exists(path_orig):
            return path_orig
        else:
            print(f'[ERROR] Expected year path {path_orig} does not exist')
            return None

    def get_file_path_orig(self, path, datehere):
        if path is None:
            path = self.get_path_orig(datehere.year)
        if path is None:
            return None
        path_jday = os.path.join(path, datehere.strftime('%j'))
        if not os.path.exists(path_jday):
            print(f'[ERROR] Expected jday path {path_jday} does not exist')
            return None
        name_file = self.dinfo['name_origin']
        date_file_str = datehere.strftime(self.dinfo['format_date_origin'])
        file_path = os.path.join(path_jday, name_file.replace('DATE', date_file_str))
        if not os.path.exists(file_path):
            print(f'[ERROR] Expected file orig path {file_path} does not exist')
            return None
        return file_path

    def get_remote_path(self, year, month):
        dtref = dt(year, month, 1)
        rpath = os.path.join(os.sep, self.product_name, self.dinfo['remote_dataset'] + self.dinfo['remote_dataset_tag'])
        sdir = os.path.join(dtref.strftime('%Y'), dtref.strftime('%m'))
        return rpath, sdir

    def get_tagged_dataset(self):
        tagged_dataset = self.dinfo['remote_dataset'] + self.dinfo['remote_dataset_tag']
        return tagged_dataset

    def check_file(self, file):
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
