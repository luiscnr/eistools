from tkinter import *    # Carga módulo tk (widgets estándar)
from tkinter import ttk  # Carga ttk (para widgets nuevos 8.5+)
import boto3
from botocore.client import Config
def main():
    print('started')
    # root = Tk()
    # root.geometry('300x200')
    # combo = ttk.Combobox( state="readonly",values=['a','b','c','d'])
    # combo.place(x=50, y=50)
    # root.mainloop()
    S3_ENDPOINT = "https://s3.waw3-1.cloudferro.com"
    s3 = boto3.client("s3",endpoint_url=S3_ENDPOINT)#,config = Config(signature_version="unsigned"))
    print(type(s3))
    print(s3.meta.partition)
    #s3.list_buckets()
    s3.close()
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()