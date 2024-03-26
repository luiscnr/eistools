
class S3Bucket():
    def __init__(self):
        self.S3_ENDPOINT = "https://s3.waw3-1.cloudferro.com"
        self.S3_BUCKET_NAME = "mdl-native-15"
        # self.PRODUCT = "OCEANCOLOUR_BAL_BGC_L3_NRT_009_131"
        # self.DATASET = "cmems_obs-oc_bal_bgc-plankton_nrt_l3-olci-300m_P1D"
        # self.TAG = "202211"

        self.PRODUCT = "OCEANCOLOUR_BLK_BGC_L3_MY_009_153"
        self.DATASET = "cmems_obs-oc_blk_bgc-plankton_my_l3-multi-1km_P1D"
        self.TAG = "202311"

        self.buckets_dta = 'https://s3.waw3-1.cloudferro.com/mdl-metadata-dta/nativeBuckets.json'
        self.buckets_oper = 'https://s3.waw3-1.cloudferro.com/mdl-metadata/nativeBuckets.json'
        self.s3client = None

    ##List name buckets. type: current or target
    def get_buckets_from_url(self,product_names,use_dta,type):
        types = ['current','target']
        if type is None:
            type = 'current'
        if not type in types:
            return None
        url = self.buckets_oper
        if use_dta:
            url  = self.buckets_dta
        if isinstance(product_names,str):
            product_names = [product_names]
        import requests
        response = requests.get(url)
        obj = response.json()
        defaultBucket = obj['defaultBucket']
        buckets = obj['products'][type]
        outputBuckects = {}
        for p in product_names:
            if p in buckets:
                outputBuckects[p] = buckets[p]
            else:
                outputBuckects[p] = defaultBucket

        return outputBuckects






    def update_params_from_pinfo(self ,product_info):
        if product_info.dinfo is None:
            return False
        if len(product_info.product_name )==0:
            return False
        if len(product_info.dataset_name )==0:
            return False
        self.PRODUCT = product_info.product_name
        self.DATASET = product_info.dataset_name
        if 's3_endpoint' in product_info.dinfo:
            self.S3_ENDPOINT = product_info.dinfo['s3_endpoint']
        if 's3_bucket_name' in product_info.dinfo:
            self.S3_BUCKET_NAME = product_info.dinfo['s3_bucket_name']
        if 'remote_dataset_tag' in product_info.dinfo:
            self.TAG = product_info.dinfo['remote_dataset_tag'][1:]

        return True

    def star_client(self):

        try:
            import boto3
            from botocore import UNSIGNED
            from botocore.config import Config
            self.s3client = boto3.client("s3" ,endpoint_url=self.S3_ENDPOINT ,config=Config(signature_version=UNSIGNED))
            return True
        except:
            return False

    def close_client(self):
        self.s3client.close()

    def get_size_str(self ,size):
        size_kb = size /1024

        if size_kb <1024:
            return f'{size_kb:.2f} Kb'
        else:
            size_mb = size_kb / 1024
            return f'{size_mb:.2f} Mb'


    def list_files(self ,files ,subdir ,start_date ,end_date)  :  # subdir yyyy/mm
        from datetime import datetime as dt
        if files is None:
            files = {}
        paginator = self.s3client.get_paginator("list_objects_v2")
        prefix = f'native/{self.PRODUCT}/{self.DATASET}_{self.TAG}/{subdir}'
        print(f'[INFO] Listing files in {self.S3_BUCKET_NAME}/{prefix}')
        for result in paginator.paginate(Bucket=self.S3_BUCKET_NAME ,Prefix=prefix):
            if "Contents" in result:
                for object in result["Contents"]:
                    name_file = object["Key"].split('/')[-1]
                    try:
                        date_file = dt.strptime(name_file.split('_')[0] ,'%Y%m%d')
                        if start_date is not None and date_file <start_date:
                            continue
                        if end_date is not None and date_file >end_date:
                            continue
                    except:
                        pass
                    obj_head = self.s3client.head_object(Bucket=self.S3_BUCKET_NAME, Key=object["Key"], IfMatch=object["ETag"])
                    if len(obj_head['Metadata'])>0:
                        mtime_str = obj_head['Metadata']['mtime']
                        time_str = dt.utcfromtimestamp(float(mtime_str)).strftime('%Y-%m-%d %H:%M')
                    else:
                        time_str = object["LastModified"].strftime('%Y-%m-%d %H:%M')
                    files[name_file] = {
                        'key': object["Key"],
                        'LastModified': object["LastModified"],
                        'Size': object["Size"],
                        'SizeStr': self.get_size_str(float(object["Size"])),
                        'ETag': object["ETag"],
                        'TimeStr': time_str
                    }

        # for file in files:
        #     print(file,files[file]['TimeStr'],files[file]['SizeStr'],files[file]['ETag'],files[file]['LastModified'])
        return files