from tkinter import *  # Carga módulo tk (widgets estándar)


# import boto3
# import botocore
# from botocore.client import Config

class GUI():
    def __init__(self):
        print('[INFO] Staring params')

        from product_info import ProductInfo
        self.pinfo = ProductInfo()
        root = Tk()
        root.geometry('1200x700')

        #combo products
        frame_cb = Frame(root)
        frame_cb.grid(row=0,column=0,pady=10,padx=10,columnspan=6)
        all_products = self.pinfo.get_list_all_products()
        self.combo_product = self.set_combo_pd(frame_cb,'Product:',all_products)

        #combo datasets
        frame_cd = Frame(root)
        frame_cd.grid(row=2,column=0,pady=10,padx=10,columnspan=6)
        all_pnames, all_dataset_names = self.pinfo.get_list_all_datasets()
        self.combo_dataset = self.set_combo_pd(frame_cd,'Dataset:',all_dataset_names)



        #combos options
        frame_op = Frame(root,bg="white")
        frame_op.grid(row=4,column=0,pady=10,padx=10,columnspan=6,rowspan=3)

        #buttons dates
        frame_dates = Frame(root,bg="white")
        frame_dates.grid(row=8, column=0, pady=10, padx=10, columnspan=6)

        ##button optons
        frame_bo = Frame(root,bg="white")
        frame_bo.grid(row=9,column=0,pady=10,padx=10,columnspan=6)

        #list dataset
        frame_ld = Frame(root)
        frame_ld.grid(row=0,column=6,pady=10,padx=10,columnspan=6,rowspan=4)
        self.listbox_datasets = Listbox(frame_ld, bg='white', fg='black', width=75)
        self.listbox_datasets.grid(row=0, column=0, padx=10, pady=10, columnspan=6, rowspan=4)

        #dates
        frame_edates = Frame(root,bg="white")
        frame_edates.grid(row=4,column=6,pady=10,padx=10,columnspan=6)
        self.entrys,self.entrye = self.set_frame_dates(frame_edates)

        #files
        frame_files = Frame(root,bg="white")
        frame_files.grid(row=5,column=6,pady=10,padx=10,columnspan=6,rowspan=5)

        #buttons
        frame_buttons = Frame(root,bg="white")
        frame_buttons.grid(row=10,column=6,pady=10,padx=10,columnspan=6)

        root.mainloop()


    def get_list_datasets_from_list(self):
        return self.listbox_datasets.get(0,END)

    def replace_product_datasets_to_list(self):
        self.listbox_datasets.delete(0,END)
        self.add_product_datasets_to_list()

    def add_product_datasets_to_list(self):
        prev_list = self.get_list_datasets_from_list()
        index_list = self.listbox_datasets.size()
        pname = self.combo_product.get()
        if len(pname)>0:
            self.pinfo.set_product_info(pname)
            pnames,dnames = self.pinfo.get_list_datasets(pname,None)
            for dname in dnames:
                if dname not in prev_list:
                    self.listbox_datasets.insert(index_list,dname)
                    index_list = index_list+1

    def replace_datataset_to_list(self):
        self.listbox_datasets.delete(0, END)
        self.add_dataset_to_list()

    def add_dataset_to_list(self):
        prev_list = self.get_list_datasets_from_list()
        index_list = self.listbox_datasets.size()
        dname = self.combo_dataset.get()
        if len(dname) > 0:
            if dname not in prev_list:
                self.listbox_datasets.insert(index_list, dname)


    def set_combo_pd(self,frame,str_label,values):
        from tkinter import ttk  # Carga ttk (para widgets nuevos 8.5+)
        label = Label(frame, text=str_label)
        label.grid(row=0, column=0)
        combo = ttk.Combobox(frame, state="readonly", values=values, width=60)
        combo.grid(row=0, column=1, columnspan=3)
        if str_label=='Product:':
            button_add = Button(frame, text='A', command=self.add_product_datasets_to_list)
            button_replace = Button(frame, text='R', command=self.replace_product_datasets_to_list)
        elif str_label=='Dataset:':
            button_add = Button(frame, text='A', command=self.add_dataset_to_list)
            button_replace = Button(frame, text='R', command=self.replace_datataset_to_list)

        button_add.grid(row=0, column=4, padx=10)
        button_replace.grid(row=0, column=5, padx=10)
        return combo

    def set_frame_dates(self,frame):
        from tkinter import ttk  # Carga ttk (para widgets nuevos 8.5+)
        from datetime import datetime as dt
        from datetime import timedelta
        date_str = (dt.now()-timedelta(days=1)).strftime('%Y-%m-%d')
        labels = Label(frame,text='Start date:')
        labels.grid(row=0,column=0)
        entrys = ttk.Entry(frame,width=11)
        entrys.grid(row=0,column=1,columnspan=2)
        #entrys.insert(date_str)
        labele = Label(frame, text=' End date:')
        labele.grid(row=0, column=3)
        entrye = ttk.Entry(frame,width=11)
        entrye.grid(row=0, column=4, columnspan=2)
        #entrye.insert(date_str)

        return entrys,entrye

    def get_list_files(self, files, product, dataset, start_date, end_date):
        self.pinfo.set_dataset_info(product, dataset)
        from s3buckect import S3Bucket
        sb = S3Bucket()
        b = sb.update_params_from_pinfo(self.pinfo)
        if not b:
            print('la caga al update los params')
            return
        b = sb.star_client()
        if not b:
            print('la caga al start il client')
            return
        if files is None:
            files = {}
        ##daily
        if self.pinfo.dinfo['frequency'] == 'd':
            list_months = self.get_list_months(start_date, end_date)
            for l in list_months:
                files = sb.list_files(files, l, start_date, end_date)
                # print(files)

            # for file in files:
            #     print(file, files[file]['TimeStr'], files[file]['SizeStr'], files[file]['ETag'],
            #           files[file]['LastModified'].strftime('%Y-%m-%d %H:%M'))

        return files

    def get_list_months(self, start_date, end_date):
        from datetime import timedelta
        work_date = start_date
        list_months = []
        while work_date <= end_date:
            ym = work_date.strftime('%Y/%m')
            if not ym in list_months:
                list_months.append(ym)
            work_date = work_date + timedelta(hours=24)
        return list_months


def main():
    print('started')
    gui = GUI()
    # from datetime import datetime as dt
    # sd = dt(2024,2,27)
    # ed = dt(2024,3,14)
    # gui.get_list_files(None,'OCEANCOLOUR_MED_BGC_L3_MY_009_143','cmems_obs-oc_med_bgc-plankton_my_l3-multi-1km_P1D',sd,ed)
    # root = Tk()
    # root.geometry('300x200')
    # combo = ttk.Combobox( state="readonly",values=['a','b','c','d'])
    # combo.place(x=50, y=50)
    # root.mainloop()
    # S3_ENDPOINT = "https://s3.waw3-1.cloudferro.com"
    # # s3r = boto3.resource('s3',
    # #                      aws_access_key_id='lgonzalezvilas',
    # #                      aws_secret_access_key='MegaRoma17!')
    # #boto3.session.Session(aws_access_key_id='lgonzalezvilas',aws_secret_access_key='MegaRoma17!')
    # s3 = boto3.client("s3",endpoint_url=S3_ENDPOINT, config = Config(signature_version=botocore.UNSIGNED))
    # print(type(s3))

    # print(s3.create_session())
    # s3.close()
    # import copernicusmarine
    # res = copernicusmarine.describe(include_datasets=True,contains=['OCEANCOLOUR_BLK_BGC_L3_MY_009_153'])
    # product = res['products'][0]
    #
    # for key in product:
    #     if key=='datasets':
    #         continue
    #     print(f'{key}:{product[key]}')
    #
    # dataset_id_search = 'cmems_obs-oc_blk_bgc-plankton_my_l3-multi-1km_P1D'
    # dataset_info = None
    # for dataset_dict in product['datasets']:
    #     print(dataset_dict['dataset_id'])
    #     if dataset_dict['dataset_id']==dataset_id_search:
    #         dataset_info = dataset_dict
    #
    # for key in dataset_info:
    #     print('-->',key)
    #
    # print(dataset_info['dataset_id'])
    # print(dataset_info['dataset_name'])
    # print(len(dataset_info['versions']))
    # version = dataset_info['versions'][0]
    # print(version['label'])
    # print('----------------------------------------')
    # parts = version['parts']
    #
    #
    # print(parts[0]['name'])
    # print('NServices:',len(parts[0]['services']))
    #
    # for service in parts[0]['services']:
    #     #service_type
    #     #uri
    #     #variables
    #     print(service['service_type']['service_name'])
    #     print(service['uri'])
    #     print('***********************')

    # print(version['parts'])

    # from s3buckect import S3Bucket
    # sb = S3Bucket()
    # sb.star_client()
    # sb.list_files('2023/03')
    # sb.close_client()
    # from product_info import ProductInfo
    # from dataset_selection import DatasetSelection
    # pinfo = ProductInfo()
    # # pnames = pinfo.get_list_all_products()
    # pnames, dnames = pinfo.get_list_all_datasets()
    #
    # for pname, dname in zip(pnames, dnames):
    #     print(pname, '->', dname)


##type: current or target
def getting_s3_buckect_boto3(use_dta, type):
    from product_info import ProductInfo
    from s3buckect import S3Bucket
    pinfo = ProductInfo()
    pnames = pinfo.get_list_all_products()
    sb = S3Bucket()
    pbuckets = sb.get_buckets_from_url(pnames, use_dta, type)
    for pname in pnames:
        if pname == 'OCEANCOLOUR_ARC_BGC_L4_MY_009_122':
            continue
        print('[INFO] -----------------------------------------')
        print('[INFO] Product: ', pname)
        remote_end_point = sb.S3_ENDPOINT
        remote_buckect_name = pbuckets[pname]
        list_p, list_d = pinfo.get_list_datasets(pname, None)
        pinfo.set_product_info(pname)
        pinfo_out = pinfo.pinfo
        update_pinfo = False
        for dataset_id_search in list_d:
            pinfo.set_dataset_info(pname, dataset_id_search)
            local_end_point = pinfo.get_dinfo_param('s3_endpoint')
            local_bucket_name = pinfo.get_dinfo_param('s3_bucket_name')
            if local_end_point is None or local_end_point != remote_end_point:
                print(f'[WARNING] Local end point set to: {remote_end_point}')
                pinfo_out = pinfo.set_param(pinfo_out, 's3_endpoint', remote_end_point)
                update_pinfo = True
            if local_bucket_name is None or local_bucket_name != remote_buckect_name:
                print(f'[WARNING] Local bucket name set to: {remote_buckect_name}')
                pinfo_out = pinfo.set_param(pinfo_out, 's3_bucket_name', remote_buckect_name)
                update_pinfo = True

        if update_pinfo:
            print(f'[WARNING] Updating JSON file for product: {pname}')
            pinfo.update_json(pinfo_out)


def getting_s3_buckets_copernicus_marine():
    import copernicusmarine
    from product_info import ProductInfo
    pinfo = ProductInfo()

    pnames = pinfo.get_list_all_products()
    for pname in pnames:
        if pname == 'OCEANCOLOUR_ARC_BGC_L4_MY_009_122':
            continue
        print('[INFO] -----------------------------------------')
        print('[INFO] Product: ', pname)
        res = copernicusmarine.describe(include_datasets=True, contains=[pname])
        product = res['products'][0]
        list_p, list_d = pinfo.get_list_datasets(pname, None)
        pinfo.set_product_info(pname)
        pinfo_out = pinfo.pinfo
        update_pinfo = False
        for dataset_id_search in list_d:
            pinfo.set_dataset_info(pname, dataset_id_search)
            for dataset_dict in product['datasets']:
                if dataset_dict['dataset_id'] == dataset_id_search:
                    dataset_info = dataset_dict
                    version = dataset_info['versions'][0]
                    remote_tag = version['label']
                    local_tag = pinfo.get_dinfo_param('remote_dataset_tag')
                    if local_tag is not None:
                        local_tag = local_tag[1:]
                    if local_tag != remote_tag:
                        print(f'[WARNING] Local tag set to: {remote_tag}')
                        pinfo_out = pinfo.set_param(pinfo_out, 'remote_dataset_tag', f'_{remote_tag}')
                        update_pinfo = True
                    parts = version['parts'][0]
                    for service in parts['services']:
                        if service['service_type']['service_name'] == 'original-files':
                            uri = service['uri'].split('/')
                            remote_end_point = '/'.join(uri[0:3])
                            remote_buckect_name = uri[3]

                            local_end_point = pinfo.get_dinfo_param('s3_endpoint')
                            local_bucket_name = pinfo.get_dinfo_param('s3_bucket_name')

                            if local_end_point is None or local_end_point != remote_end_point:
                                print(f'[WARNING] Local end point set to: {remote_end_point}')
                                pinfo_out = pinfo.set_param(pinfo_out, 's3_endpoint', remote_end_point)
                                update_pinfo = True
                            if local_bucket_name is None or local_bucket_name != remote_buckect_name:
                                print(f'[WARNING] Local bucket name set to: {remote_buckect_name}')
                                pinfo_out = pinfo.set_param(pinfo_out, 's3_bucket_name', remote_buckect_name)
                                update_pinfo = True

                    print(
                        f'[INFO]-> {dataset_id_search} Tag: {remote_tag} End point: {remote_end_point} Buckect: {remote_buckect_name}')

        # if update_pinfo:
        #     print(f'[WARNING] Updating JSON file for product: {pname}')
        #     pinfo.update_json(pinfo_out)


def check_files():
    from product_info import ProductInfo
    from datetime import datetime as dt
    gui = GUI()

    product_name = 'OCEANCOLOUR_BLK_BGC_L3_MY_009_153'
    dataset_name = 'cmems_obs-oc_blk_bgc-plankton_my_l3-multi-1km_P1D'
    # product_name = 'OCEANCOLOUR_BLK_BGC_L3_NRT_009_151'
    # dataset_name = 'cmems_obs-oc_blk_bgc-plankton_nrt_l3-multi-1km_P1D'
    date = dt(2024, 3, 4)
    date_end = dt(2024, 3, 15)
    gui.pinfo.set_dataset_info(product_name, dataset_name)
    files = gui.get_list_files(None, product_name, dataset_name, date, date_end)
    for file in files:
        name_file = files[file]['key'].split('/')[-1]
        print(name_file,files[file]['SizeStr'],files[file]['TimeStr'])


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    #main()
    # getting_s3_buckets()
    check_files()
    # getting_s3_buckect_boto3(True,None)
