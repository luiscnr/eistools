import os.path
from tkinter import *  # Carga módulo tk (widgets estándar)

# import boto3
# import botocore
# from botocore.client import Config
import argparse

parser = argparse.ArgumentParser(description='Reformat and upload to the DBS')
parser.add_argument("-gui", "--launch_gui", help="Launch GUI", action="store_true")
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
parser.add_argument("-m", "--mode", help="Mode",choices=['check_files','update_buckets','check_daily_file'])
parser.add_argument("-sd", "--start_date", help="Start date (yyyy-mm-dd)")
parser.add_argument("-ed", "--end_date", help="Start date (yyyy-mm-dd)")
parser.add_argument("-pname", "--name_product", help="Product name")
parser.add_argument("-dname", "--name_dataset", help="Product name")
parser.add_argument("-dta","--use_dta",help="Use dta buckets with mode update_buckets",action="store_true")
parser.add_argument("-pinfo", "--pinfo_folder",help="Alternative product info folder")
args = parser.parse_args()


class GUI():
    def __init__(self, launch_gui, pinfo_folder):
        print('[INFO] Starting params')

        from product_info import ProductInfo
        self.pinfo = ProductInfo()
        if pinfo_folder is not None:
            self.pinfo.path2info = pinfo_folder
        all_products = self.pinfo.get_list_all_products()
        all_pnames, all_dataset_names = self.pinfo.get_list_all_datasets()
        self.dproducts = {}
        for idx in range(len(all_pnames)):
            self.dproducts[all_dataset_names[idx]] = all_pnames[idx]

        if not launch_gui:
            return

        root = Tk()
        root.geometry('1200x500')

        # combo products
        frame_cb = Frame(root,relief=SOLID,borderwidth=1)
        frame_cb.grid(row=0, column=0, pady=1, padx=3, columnspan=6)
        self.combo_product = self.set_combo_pd(frame_cb, 'Product:', all_products)

        # combo datasets
        frame_cd = Frame(root,relief=SOLID,borderwidth=1)
        frame_cd.grid(row=1, column=0, pady=1, padx=3, columnspan=6)
        self.combo_dataset = self.set_combo_pd(frame_cd, 'Dataset:', all_dataset_names)

        # combos datasets optioins
        frame_op = Frame(root,relief=SOLID,borderwidth=1)
        frame_op.grid(row=2, column=0, pady=1, padx=3, columnspan=6, rowspan=2)
        self.combo_dataset_options = self.set_combo_options(frame_op)

        # buttons dates
        frame_dates = Frame(root, relief=SOLID,borderwidth=1)
        frame_dates.grid(row=4, column=0, pady=1, padx=3, columnspan=6)
        self.set_buttons_date(frame_dates)


        # list dataset
        frame_ld = Frame(root)
        frame_ld.grid(row=0, column=6, pady=5, padx=0, columnspan=6, rowspan=4)
        self.listbox_datasets = self.set_list_box(frame_ld, 84, 10)

        # dates
        frame_edates = Frame(root)
        frame_edates.grid(row=4, column=6, pady=10, padx=0, columnspan=6)
        self.entrys, self.entrye = self.set_frame_dates(frame_edates)

        # files
        frame_files = Frame(root)
        frame_files.grid(row=5, column=0, pady=10, padx=5, columnspan=8, rowspan=5)
        self.listbox_files = self.set_tree_view(frame_files)

        # list missing
        frame_fm = Frame(root)
        frame_fm.grid(row=5, column=8, pady=5, padx=0, columnspan=5, rowspan=5)
        self.listbox_missing = self.set_list_box(frame_fm, 68, 25)


        # label info
        self.info_var = StringVar()
        self.info_var.set('No files')
        label_info = Label(root, textvariable=self.info_var)
        label_info.grid(row=10, column=1, columnspan=10,padx=100, pady=2,sticky=W)

        ##buttons
        frame_bop = Frame(root)
        frame_bop.grid(row=11, column=0, columnspan=12)
        self.set_buttons_bop(frame_bop)

        # buttons last line
        frame_buttons = Frame(root, bg="white")
        frame_buttons.grid(row=12, column=1, pady=10, padx=10, columnspan=10)
        button_search = Button(frame_buttons, text='SEARCH', command=self.search_files)
        button_search.pack()

        root.mainloop()

    def search_files(self):
        datasets = self.get_list_datasets_from_list()
        if len(datasets) == 0:
            return
        start_date, end_date = self.get_start_and_end_dates_from_entry()
        files = {}
        nexpected_files = 0
        for dataset in datasets:
            product = self.dproducts[dataset]
            self.pinfo.set_dataset_info(product,dataset)
            nexpected_files = nexpected_files + self.pinfo.get_number_expected_files_between_two_dates(start_date,end_date)
            files = self.get_list_files(files, product, dataset, start_date, end_date)

        for i in self.listbox_files.get_children():
            self.listbox_files.delete(i)
        index = 0
        total_size = 0
        for file in files:
            time_str = files[file]['TimeStr']
            size = files[file]['SizeStr']
            total_size = total_size + float(files[file]['Size'])
            # line_str = f'{name_file}{time_str}{size}'
            self.listbox_files.insert('', index, text=file, values=(file, time_str, size))
            index = index + 1

        nfiles = len(files)
        nmissing = nexpected_files - nfiles
        total_size_str = self.get_size_str(total_size)

        line_str = f'Files retrieved: {nfiles} / {nexpected_files}. Missing files: {nmissing}. Total size: {total_size_str}'
        self.info_var.set(line_str)

        self.listbox_missing.delete(0, END)

        if nmissing>0:
            available_files = list(files.keys())
            misssing_files = []
            from datetime import timedelta
            from datetime import datetime as dt
            for dataset in datasets:
                product = self.dproducts[dataset]
                self.pinfo.set_dataset_info(product, dataset)
                frequency = self.pinfo.get_frequency()
                if frequency is None:
                    continue
                if frequency.lower()=='d':
                    check_date = start_date
                    while check_date<=end_date:
                        remote_file_name = self.pinfo.get_remote_file_name(check_date)
                        if self.pinfo.dinfo['mode'] == 'MY':
                            datemyintref = dt.strptime(self.pinfo.dinfo['myint_date'], '%Y-%m-%d')
                            if check_date >= datemyintref:
                                remote_file_name = remote_file_name.replace('my', 'myint')
                        if remote_file_name not in available_files:
                            misssing_files.append(remote_file_name)
                        check_date = check_date + timedelta(hours=24)

                if frequency.lower()=='m':
                    end_date = end_date.replace(day=15,hour=0,minute=0,second=0,microsecond=0)
                    check_date = start_date.replace(day=15,hour=0,minute=0,second=0,microsecond=0)
                    while check_date <= end_date:
                        remote_file_name = self.pinfo.get_remote_file_name_monthly(check_date)
                        if self.pinfo.dinfo['mode'] == 'MY':
                            datemyintref = dt.strptime(self.pinfo.dinfo['myint_date'], '%Y-%m-%d')
                            if check_date >= datemyintref:
                                remote_file_name = remote_file_name.replace('my', 'myint')
                        if remote_file_name not in available_files:
                            misssing_files.append(remote_file_name)
                        next_year = check_date.year
                        next_month = check_date.month+1
                        if next_month==13:
                            next_month= 1
                            next_year = next_year+1
                        check_date = check_date.replace(year=next_year,month=next_month)

            for idx, fname in enumerate(misssing_files):
                self.listbox_missing.insert(idx, fname)

    def get_size_str(self ,size):
        size_kb = size /1024
        if size_kb <1024:
            return f'{size_kb:.2f} Kb'
        else:
            size_mb = size_kb / 1024
            return f'{size_mb:.2f} Mb'

    def get_start_and_end_dates_from_entry(self):
        from datetime import datetime as dt
        sdate_str = self.entrys.get().strip()
        edate_str = self.entrye.get().strip()
        try:
            start_date = dt.strptime(sdate_str, '%Y-%m-%d')
            end_date = dt.strptime(edate_str, '%Y-%m-%d')
        except:
            return None, None
        if end_date < start_date:
            return None, None
        return start_date, end_date

    def get_list_datasets_from_list(self):
        return self.listbox_datasets.get(0, END)

    def replace_dateset_list_with_list(self,dataset_list):
        self.listbox_datasets.delete(0, END)
        for idx,dname in enumerate(dataset_list):
            self.listbox_datasets.insert(idx, dname)

    def replace_product_datasets_to_list(self):
        self.listbox_datasets.delete(0, END)
        self.add_product_datasets_to_list()

    def add_product_datasets_to_list(self):
        prev_list = self.get_list_datasets_from_list()
        index_list = self.listbox_datasets.size()
        pname = self.combo_product.get()
        if len(pname) > 0:
            self.pinfo.set_product_info(pname)
            pnames, dnames = self.pinfo.get_list_datasets(pname, None)
            for dname in dnames:
                if dname not in prev_list:
                    self.listbox_datasets.insert(index_list, dname)
                    index_list = index_list + 1

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

    def set_list_box(self, frame, width, height):
        listbox = Listbox(frame, bg='white', fg='black', width=width, height=height, relief=GROOVE)
        listbox.pack(side=LEFT, fill=BOTH)
        scrollbar = Scrollbar(frame, orient=VERTICAL, width=17)
        scrollbar.pack(side=RIGHT, fill=BOTH)
        listbox.config(yscrollcommand=scrollbar.set)
        scrollbar.config(command=listbox.yview)
        return listbox

    def set_tree_view(self, frame):
        from tkinter import ttk
        tree = ttk.Treeview(frame, column=("c1", "c2", "c3"), show='headings', height=18)
        tree.column("# 1", anchor=W, width=450)
        tree.heading("# 1", text="File")
        tree.column("# 2", anchor=W, width=130)
        tree.heading("# 2", text="Datetime")
        tree.column("# 3", anchor=W, width=80)
        tree.heading("# 3", text="Size")
        tree.pack(side=LEFT)
        scrollbar = Scrollbar(frame, width=17, orient=VERTICAL)
        scrollbar.pack(side=RIGHT, fill=BOTH)
        tree.config(xscrollcommand=scrollbar.set)
        scrollbar.config(command=tree.yview)

        return tree

    def set_combo_pd(self, frame, str_label, values):
        from tkinter import ttk  # Carga ttk (para widgets nuevos 8.5+)
        label = Label(frame, text=str_label)
        label.grid(row=0, column=0)
        combo = ttk.Combobox(frame, state="readonly", values=values, width=57)
        combo.grid(row=0, column=1, columnspan=3)
        if str_label == 'Product:':
            button_add = Button(frame, text='A', command=self.add_product_datasets_to_list)
            button_replace = Button(frame, text='R', command=self.replace_product_datasets_to_list)
        elif str_label == 'Dataset:':
            button_add = Button(frame, text='A', command=self.add_dataset_to_list)
            button_replace = Button(frame, text='R', command=self.replace_datataset_to_list)

        button_add.grid(row=0, column=4, padx=5)
        button_replace.grid(row=0, column=5, padx=5)
        return combo

    def set_combo_options(self,frame):
        from tkinter import ttk
        labels = [None]*6
        combos = [None]*6
        labels_texts = ['Timeliness','Region','Frequency','Level','Sensor','Dataset']
        rows = [0,0,0,1,1,1]
        columns = [0,2,4,0,2,4]
        combo_values= {
            'Timeliness': ['Any','NRT','MY'],
            'Region': ['Any','MED','BLK','BAL','ARC'],
            'Frequency': ['Any','Daily','Monthly','Clima'],
            'Level': ['Any','L3','L4'],
            'Sensor': ['Any','Multi','Olci','Gap-free'],
            'Dataset': ['Reflectance','Plankton','Transp','Optics']
        }
        for idx in range(len(labels_texts)):
            label = labels_texts[idx]
            labels[idx] = Label(frame, text=f'{label}:')
            labels[idx].grid(row=rows[idx],column=columns[idx],padx=3,pady=3,sticky=W)
            combos[idx] = ttk.Combobox(frame, state="readonly", values=combo_values[label],width=12)
            combos[idx].grid(row=rows[idx],column=columns[idx]+1,padx=3,pady=3,sticky=W)

        button_add = Button(frame, text='A', command=self.add_product_datasets_to_list)
        button_replace = Button(frame, text='R', command=self.replace_product_datasets_to_list)
        button_add.grid(row=0,column=6,pady=3,padx=5)
        button_replace.grid(row=1,column=6,pady=3,padx=5)

        return combos

    def set_buttons_date(self,frame):
        label = Label(frame, text='Dates:')
        label.grid(row=0, column=0,pady=3)
        #command = lambda: action(someNumber)
        button1 = Button(frame,text="-1", command = lambda: self.set_date_from_button(-1,-1))
        button1.grid(row=0, column=1, pady=3,padx=17)
        button2 = Button(frame, text="-8", command=lambda: self.set_date_from_button(-8, -8))
        button2.grid(row=0, column=2, pady=3,padx=18)
        button3 = Button(frame, text="-12", command=lambda: self.set_date_from_button(-12, -12))
        button3.grid(row=0, column=3, pady=3,padx=18)
        button4 = Button(frame, text="-180", command=lambda: self.set_date_from_button(-180, -180))
        button4.grid(row=0, column=4,pady=3,padx=18)
        button5 = Button(frame, text="LAST8", command=lambda: self.set_date_from_button(-8, -1))
        button5.grid(row=0, column=5,pady=3,padx=18)
        button6 = Button(frame, text="LAST7", command=lambda: self.set_date_from_button(-7, -1))
        button6.grid(row=0, column=6,pady=3,padx=17)


    def set_date_from_button(self,start,end):
        from datetime import datetime as dt
        from datetime import timedelta

        start_date = dt.now()+timedelta(days=start)
        end_date = dt.now()+timedelta(days=end)
        start_date_s = start_date.strftime('%Y-%m-%d')
        end_date_s = end_date.strftime('%Y-%m-%d')
        self.entrys.delete(0,END)
        self.entrys.insert(0,start_date_s)
        self.entrye.delete(0,END)
        self.entrye.insert(0,end_date_s)

    def set_buttons_bop(self,frame):
        text_b = ['NRTd','DTd8','DTd12','NRTm','DTm','X','Y','Z']

        button0 = Button(frame,text=text_b[0],command=lambda: self.set_dataset_and_dates_options(0))
        button0.grid(row=0,column=0,padx=25)
        button1 = Button(frame, text=text_b[1], command=lambda: self.set_dataset_and_dates_options(1))
        button1.grid(row=0, column=1, padx=25)
        button2 = Button(frame, text=text_b[2], command=lambda: self.set_dataset_and_dates_options(2))
        button2.grid(row=0, column=2, padx=25)
        button3 = Button(frame, text=text_b[3], command=lambda: self.set_dataset_and_dates_options(3))
        button3.grid(row=0, column=3, padx=25)
        button4 = Button(frame, text=text_b[4], command=lambda: self.set_dataset_and_dates_options(4))
        button4.grid(row=0, column=4, padx=25)

        ##no implemented buttons
        button5 = Button(frame, text=text_b[5], command=lambda: self.set_dataset_and_dates_options(5))
        button5.grid(row=0, column=5, padx=25)

        button6 = Button(frame, text=text_b[6], command=lambda: self.set_dataset_and_dates_options(6))
        button6.grid(row=0, column=6, padx=25)

        button7 = Button(frame, text=text_b[7], command=lambda: self.set_dataset_and_dates_options(7))
        button7.grid(row=0, column=7, padx=25)

    def set_dataset_and_dates_options(self,idx):
        if idx>=5: ##NO IMPLEMENTED
            return
        text_b = ['NRTd', 'DTd8', 'DTd12', 'NRTm', 'DTm']
        dates = [-1,-8,-12,-999,-999]
        datasets = self.get_list_datasets(text_b[idx])
        self.replace_dateset_list_with_list(datasets)
        if dates[idx]==-999:
           from datetime import datetime as dt
           from datetime import timedelta
           date_prev_month = (dt.now().replace(day=15)-timedelta(days=30)).replace(day=15)
           ndays = (date_prev_month-dt.now()).days+1
           self.set_date_from_button(ndays,ndays)
        else:
           self.set_date_from_button(dates[idx],dates[idx])



    def get_list_datasets(self,key):
        ldatasets = []
        for dataset in self.dproducts:
            if key=='NRTd' and (dataset.find('nrt_l3')>0 or dataset.find('nrt_l4-gapfree')>0):
                ldatasets.append(dataset)
            if key=='DTd8' and (dataset.find('my_l3')>0):
                if dataset.find('bal')>0 and dataset.find('multi')>0:
                    pass
                else:
                    ldatasets.append(dataset)
            if key=='DTd12' and (dataset.find('my_l4-gapfree')>0):
                ldatasets.append(dataset)
            if key=='NRTm' and dataset.find('nrt_l4')>0 and dataset.find('nrt_l4-gapfree')<0:
                ldatasets.append(dataset)
            if key == 'DTm' and dataset.find('my_l4') > 0 and dataset.find('my_l4-gapfree') < 0 and dataset.find('climatology')<0:
                if dataset.find('bal')>0 and dataset.find('multi')>0:
                    pass
                else:
                    ldatasets.append(dataset)
        return ldatasets

    def set_frame_dates(self, frame):
        from tkinter import ttk  # Carga ttk (para widgets nuevos 8.5+)
        from datetime import datetime as dt
        from datetime import timedelta
        date_str = (dt.now() - timedelta(days=1)).strftime('%Y-%m-%d')

        labels = Label(frame, text='Start date:')
        labels.grid(row=0, column=0)
        entrys = ttk.Entry(frame, width=11)
        entrys.grid(row=0, column=1, columnspan=2)
        entrys.insert(0, date_str)
        labele = Label(frame, text=' End date:')
        labele.grid(row=0, column=3)
        entrye = ttk.Entry(frame, width=11)
        entrye.grid(row=0, column=4, columnspan=2)
        entrye.insert(0, date_str)

        return entrys, entrye

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
        ##monthly
        if self.pinfo.dinfo['frequency'] == 'm':
            list_years = self.get_list_years(start_date,end_date)
            for l in list_years:
                files = sb.list_files_month(files,l,start_date,end_date)


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

    def get_list_years(self,start_date,end_date):
        from datetime import timedelta
        work_date = start_date
        list_years = []
        while work_date <= end_date:
            y = work_date.strftime('%Y/')
            if not y in list_years:
                list_years.append(y)
            work_date = work_date + timedelta(hours=24)
        return list_years

def launch_gui(pinfo_folder):
    print('[INFO] Launching GUI')
    GUI(True,pinfo_folder)


def main():
    pinfo_folder = None
    if args.pinfo_folder and os.path.isdir(args.pinfo_folder):
        pinfo_folder = args.pinfo_folder

    if args.launch_gui:
        launch_gui(pinfo_folder)
        return
    # main()
    # getting_s3_buckets()
    if args.mode=='check_files':
        from datetime import datetime as dt
        start_date = dt.strptime(args.start_date, '%Y-%m-%d')
        if args.end_date:
            end_date = dt.strptime(args.end_date, '%Y-%m-%d')
        else:
            end_date =start_date
        check_files(args.name_product, args.name_dataset, start_date, end_date,pinfo_folder)

    if args.mode=='check_daily_file':
        from datetime import datetime as dt
        start_date = dt.strptime(args.start_date, '%Y-%m-%d')
        from product_info import ProductInfo
        pinfo = ProductInfo()
        pinfo.path2info = pinfo_folder
        pinfo.set_dataset_info(args.name_product, args.name_dataset)
        from s3buckect import S3Bucket
        sb = S3Bucket()
        sb.star_client()
        sb.check_daily_file('NRT',pinfo,start_date,args.verbose)
        sb.close_client()


    if args.mode=='update_buckets':
        #getting_s3_buckect_boto3(args.use_dta,None)
        getting_s3_buckets_copernicus_marine()

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


def check_files(product_name, dataset_name, start_date, end_date,pinfo_folder):
    from product_info import ProductInfo
    from datetime import datetime as dt
    gui = GUI(False,pinfo_folder)

    # product_name = 'OCEANCOLOUR_BLK_BGC_L3_MY_009_153'
    # dataset_name = 'cmems_obs-oc_blk_bgc-plankton_my_l3-multi-1km_P1D'
    # product_name = 'OCEANCOLOUR_MED_BGC_L3_NRT_009_141'
    # dataset_name = 'cmems_obs-oc_med_bgc-transp_nrt_l3-olci-300m_P1D'
    # product_name = 'OCEANCOLOUR_BLK_BGC_L3_NRT_009_151'
    # dataset_name = 'cmems_obs-oc_blk_bgc-plankton_nrt_l3-multi-1km_P1D'
    # date = dt(2024, 3, 20)
    # date_end = dt(2024, 3, 26)
    gui.pinfo.set_dataset_info(product_name, dataset_name)
    files = gui.get_list_files(None, product_name, dataset_name, start_date, end_date)
    for file in files:
        name_file = files[file]['key'].split('/')[-1]
        print(name_file, files[file]['SizeStr'], files[file]['TimeStr'])


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
