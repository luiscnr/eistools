import json

from dataset_selection import DatasetSelection
import general_options as goptions
from product_info import ProductInfo


class CONFIG_JSON():
    def __init__(self):
        self.common_params = ["level", "mode", "region", "sensor", "frequency", "start_date", "end_date", "variables",
                              "path_origin", "name_origin", "format_date_origin", "names_processed",
                              "format_date_processed", "remote_dataset", "remote_dataset_tag", "remote_file_name",
                              "remote_date_format", "res", "f-option", "dataset"]

        self.nrt_params = ["myproduct", "mydataset", "sources", "CMD_NRT_reprocessing", "CMD_DT_reprocessing"]

        self.my_params = ["myint_date"]

        self.pinfo = {}

        self.dinfo = {}

    def save_product_info(self,name_product,name_dataset):
        if len(self.dinfo)==0:
            return

        self.pinfo[name_dataset] = self.dinfo
        pinfo = ProductInfo()
        foutput = pinfo.get_product_info_file(name_product)
        f = open(foutput,'w')
        json.dump(self.pinfo,f,ensure_ascii=False, indent=1)
        f.close()
        print(f'[INFO] Product/dataset info saved to: {foutput}')

    def get_product_info(self, name_product):
        pinfoh = ProductInfo()
        valid = pinfoh.set_product_info(name_product)
        if valid:
            self.pinfo = pinfoh.pinfo




    def get_dataset_info(self, name_product,name_dataset, params_here_values):
        # 'REGION': None,
        # 'LEVEL': None,
        # 'DATASET': None,
        # 'SENSOR': None,
        # 'FREQUENCY': None

        dvalues = []
        params_here = ['region', 'level', 'dataset', 'sensor', 'frequency']
        #print(params_here_values)
        for param in self.common_params:
            try:
                idx = params_here.index(param)
                value = params_here_values[idx]
            except ValueError:
                value = ''
            if param == 'region' or param == 'sensor' or param == 'level':
                value = value.upper()
            dvalues.append((param, value))

        mode_here = ''
        if name_dataset.find('nrt') > 0:
            mode_here = 'NRT'
            for param in self.nrt_params:
                value = ''
                dvalues.append((param, value))

        if name_dataset.find('my') > 0:
            mode_here = 'MY'
            for param in self.my_params:
                value = ''
                dvalues.append((param, value))

        self.dinfo.update(dvalues)

        start_date = ''
        end_date = ''
        res = ''
        path_origin = ''
        name_origin = ''
        myint_date = ''
        sources = ''
        if self.dinfo['sensor'] == 'MULTI':
            res = 'HR'
            path_origin = '/store2/OC/X/daily'
            name_origin = 'CMEMS2_XDATE-'
            sources = 'AQUA, VIIRS, VIIRSJ, OLCI'
            if mode_here == 'NRT':
                start_date = '2022-01-01'
                end_date = '-1'
            if mode_here == 'MY':
                start_date = '1997-09-16'
                end_date = '2021-12-31'
                myint_date = '2022-01-01'
        if self.dinfo['sensor'] == 'OLCI':
            res = 'FR'
            path_origin = '/store2/OC/O/daily'
            name_origin = 'CMEMS2_ODATE-'
            sources = 'S3A_FR, S3B_FR'
            if mode_here == 'NRT':
                start_date = '2022-11-01'
                end_date = '-1'
            if mode_here == 'MY':
                start_date = '2016-04-26'
                end_date = '2022-10-31'
                myint_date = '2022-11-01'

        reg_here = self.dinfo['region'].lower()
        dat_here = self.dinfo['dataset']
        if reg_here == 'blk':
            reg_here = 'bs'
        name_origin = f'{name_origin}{dat_here}-{reg_here}-{res.lower()}.nc'
        self.dinfo['mode'] = mode_here
        self.dinfo['start_date'] = start_date
        self.dinfo['end_date'] = end_date
        self.dinfo['path_origin'] = path_origin
        self.dinfo['name_origin'] = name_origin
        self.dinfo['format_date_origin'] = '%Y%j'
        self.dinfo['format_date_processed'] = '%Y%j'
        self.dinfo['remote_dataset'] = name_dataset
        eis = goptions.get_eis()
        self.dinfo['remote_dataset_tag'] = f'_{eis}'
        self.dinfo['remote_file_name'] = f'DATE_{name_dataset}.nc'
        self.dinfo['remote_date_format'] = '%Y%m%d'
        self.dinfo['res'] = res
        self.dinfo['f-option'] = self.dinfo['frequency'].upper()

        if mode_here == 'MY':
            self.dinfo['myint_date'] = myint_date
        if mode_here == 'NRT':
            self.dinfo['sources'] = sources
            self.dinfo['mydataset'] = name_dataset.replace('nrt','my')
            myproduct = name_product.replace('NRT','MY')
            strend = myproduct.split('_')[-1]
            newstrend = str(int(strend)+2)
            myproduct = myproduct.replace(f'_{strend}',f'_{newstrend}')
            self.dinfo['myproduct'] = myproduct

        #print(self.dinfo)



def main():
    print('STARTED')
    dsel = DatasetSelection('NRT')
    product_names, dataset_names = dsel.get_unavailabe_datasets()
    if len(product_names) == 0:
        print(f'[INFO] All the datasets have already been configured')
        return

    for idx in range(len(product_names)):
        name_product = product_names[idx]
        name_dataset = dataset_names[idx]
        params_here = dsel.get_params_dataset_fromdict(name_dataset)
        if dsel.check_name_dataset(name_dataset, params_here):
            jconfig = CONFIG_JSON()
            jconfig.get_product_info(name_product)
            jconfig.get_dataset_info(name_product,name_dataset, params_here)
            jconfig.save_product_info(name_product,name_dataset)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
