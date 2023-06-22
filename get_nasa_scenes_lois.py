# from nasa_download import NASA_DOWNLOAD
# from datetime import datetime as dt
# import argparse

# def main():
#     try:
#         date_here = dt.strptime(args.date,'%Y%m%d')
#         sensor = args.sensor
#         if args.Region=='BlackSea':
#             region = 'BS'
#         elif args.Region=='Mediterranean':
#             region = 'MED'
#         #date_here = dt(2022,9,30)
#     except:
#         print('[ERROR] Error parsing arguments')
#         exit(1)
#
#     ndownload = NASA_DOWNLOAD()
#     ndownload.get_list_files(date_here,sensor,region,'DT')


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='NASA Get Scenes. Patch to retrieve DT scenes')
    parser.add_argument("-m", "--mode", help="Mode",choices=["LIST","NRT_TO_DT"])
    parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
    parser.add_argument("-sen", "--sensor", help="Specify sensor: VIIRS, VIIRSJ, AQUA", required=True)
    parser.add_argument("-d", "--date", help="Specify a date in yyyymmdd format", required=True)
    parser.add_argument("-R", "--Region", help="specify the Region Label of the area: BlackSea or Mediterranean",
                        required=False)
    args = parser.parse_args()

    from nasa_download import NASA_DOWNLOAD
    from datetime import datetime as dt

    try:
        date_here = dt.strptime(args.date, '%Y%m%d')
        sensor = args.sensor
        region = None
        if args.Region:
            if args.Region == 'BlackSea':
                region = 'BS'
            elif args.Region == 'Mediterranean':
                region = 'MED'
        # date_here = dt(2022,9,30)
    except:
        print('[ERROR] Error parsing arguments')
        exit(1)

    if args.mode=='LIST':
        ndownload = NASA_DOWNLOAD()
        list = ndownload.get_list_files(date_here, sensor, region, 'DT')

    if args.mode=='NRT_TO_DT':
        ndownload = NASA_DOWNLOAD()
        list_nasa = ndownload.get_list_files(date_here, sensor, region, 'DT')
        list_nasa = [l.strip() for l in list_nasa]
        list_dest = ndownload.get_list_files_orig(sensor,date_here)
        print(list_dest)

        #check if files are in the ftp
        for name_orig in list_dest:
            name = list_dest[name_orig]
            if name in list_nasa:
                print('OK: ',name)
            else:
                namen = ndownload.get_date_with01(name,sensor)
                if namen in list_nasa:
                    print('OK: ',namen)
                    list_dest[name_orig] = namen
                else:
                    print('WARNING',name,'->',namen)
                    list_dest[name_orig] = 'NaN'

        print(list_dest)

        #download
        import obdaac_download as od
        import os
        appkey = '22da4a89034645c3653f75dcd49ea11639976cef'
        for name_orig in list_dest:
            name = list_dest[name_orig]
            dir_out = ndownload.get_path_orig(sensor,date_here)
            url = ndownload.get_url_download(name)
            print(f'[INFO] Downloading: {name} to {dir_out}')
            od.do_download(url,dir_out,appkey)

            file_dest = os.path.join(dir_out,name)
            file_orig = os.path.join(dir_out, name_orig)
            if os.path.exists(file_dest) and os.path.join(file_orig):
                nrtfolder = os.path.join(dir_out,'NRT')
                if not os.path.exists(nrtfolder):
                    os.mkdir(nrtfolder)
                file_orig_copy = os.path.join(nrtfolder,name_orig)
                os.rename(file_orig,file_orig_copy)
                os.rename(file_dest,file_orig)



            #ndownload.download_file(name,file_out)
            #obdaac_download(file_out --odir file_out)
