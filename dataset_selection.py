import os
import pandas as pd
from product_info import ProductInfo


class DatasetSelection():

    def __init__(self, mode):
        sdir = os.path.abspath(os.path.dirname(__file__))

        self.path2info = os.path.join(os.path.dirname(sdir), 'PRODUCT_INFO')
        print(f'[INFO] Dataset selection path: {self.path2info}')

        self.dfselection = None
        self.params = {
            'REGION': None,
            'LEVEL': None,
            'DATASET': None,
            'SENSOR': None,
            'FREQUENCY': None
        }

        file = None

        if mode.upper() == 'NRT' or mode.upper() == 'DT':
            file = os.path.join(self.path2info, 'NRTDictionary.csv')
        if mode.upper() == 'MY' or mode.upper() == 'MYINT':
            file = os.path.join(self.path2info, 'MYDictionary.csv')

        if file is not None and os.path.exists(file):
            try:
                self.dfselection = pd.read_csv(file, sep=';')
            except:
                self.dfselection = None

    def set_params(self, region, level, dataset_type, sensor, frequency):
        params_here = [region, level, dataset_type, sensor, frequency]
        keys = list(self.params.keys())
        for idx in range(5):
            if params_here[idx] is not None:
                params_here[idx] = params_here[idx].lower()
                if idx == 0 and params_here[idx] == 'bs':
                    params_here[idx] = 'blk'
            key = keys[idx]
            self.params[key] = params_here[idx]

    def get_list_product_datasets_from_params(self):
        product_names = []
        datasets_names = []
        if self.dfselection is None:
            return product_names, datasets_names
        n_params = 0
        for k in self.params.keys():
            if self.params[k] is not None:
                n_params = n_params + 1
        if n_params == 0:
            return product_names, datasets_names

        for idx, row in self.dfselection.iterrows():
            add = True
            for k in self.params.keys():
                v = self.params[k]
                if v is not None:
                    if v != row[k]:
                        add = False
                        # print(k, v, row[k])
            if add:
                product_names.append(row['PNAME'])
                datasets_names.append(row['DNAME'])

        return product_names, datasets_names

    def get_list_product_datasets_from_product_nane(self, product_name):
        return self.get_list_product_datasets_from_param_value('PNAME', product_name)

    def get_list_product_datasets_from_dataset_nane(self, dataset_name):
        return self.get_list_product_datasets_from_param_value('DNAME', dataset_name)



    def get_list_product_datasets_from_param_value(self, param, value):
        product_names = []
        datasets_names = []
        if self.dfselection is None:
            return product_names, datasets_names

        for idx, row in self.dfselection.iterrows():
            add = False
            if row[param] == value:
                add = True

            if add:
                product_names.append(row['PNAME'])
                datasets_names.append(row['DNAME'])

        return product_names, datasets_names

    def get_unavailabe_datasets(self):
        product_names = []
        datasets_names = []
        if self.dfselection is None:
            return product_names, datasets_names
        pinfo = ProductInfo()
        for idx, row in self.dfselection.iterrows():
            name_product = row['PNAME']
            name_dataset = row['DNAME']
            if not pinfo.set_dataset_info(name_product, name_dataset):
                product_names.append(name_product)
                datasets_names.append(name_dataset)

        return product_names, datasets_names

    def get_params_dataset_fromdict(self, name_dataset):
        params_here = []
        if self.dfselection is None:
            return params_here
        for idx, row in self.dfselection.iterrows():
            name_dataset_here = row['DNAME']
            if name_dataset == name_dataset_here:
                for param in self.params:
                    params_here.append(row[param])
        return params_here

    def check_name_dataset(self, name_dataset, params_dataset):
        # 0:region; 1: level; 2: dataset; 3:sensor; 4:frequency
        region = params_dataset[0].lower()
        level = params_dataset[1].lower()
        dataset = params_dataset[2].lower()
        sensor = params_dataset[3].lower()
        freq = params_dataset[4].upper()
        if sensor == 'olci':
            res = '300m'
        else:
            res = '1km'
        mode = '-'
        if name_dataset.find('nrt') > 0:
            mode = 'nrt'
        elif name_dataset.find('my') > 0:
            mode = 'my'
        # cmems_obs-oc_arc_bgc-plankton_nrt_l3-multi-1km_P1D

        expected_name = f'cmems_obs-oc_{region}_bgc-{dataset}_{mode}_{level}-{sensor}-{res}_P1{freq}'
        if name_dataset == expected_name:
            return True
        else:
            print(
                f'[ERROR] Dataset name {name_dataset} differs from expeced file name: {expected_name} according to params')
            return False
