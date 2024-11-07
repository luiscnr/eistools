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

def get_lat_lon_from_site_name(site):
    sites = {
        'VEIT': {
            'lat': 45.3139,
            'lon': 12.5083
        },
        'TRIT': {
            'lat': 43.12278,
            'lon': 12.13306
        },
        'GAIT': {
            'lat': 45.577008,
            'lon': 10.579382
        },
        'LAIT': {
            'lat': 35.4935,
            'lon': 12.4678
        }
    }
    latP = None
    lonP = None
    if site in sites.keys():
        latP = sites[site]['lat']
        lonP = sites[site]['lon']
    if latP is None and lonP is None:
        print(
            f'[ERROR] Site {site} is not included in the site list. Please give coordinates with lat_p and lon_p options')
    return latP, lonP


def download_date(ndownload, sensor, region, lat_point, lon_point, date_here):
    path_out = ndownload.get_path_orig(sensor, date_here)
    if os.path.exists(path_out) and not args.overwrite:
        print(f'[WARNING] Ouput file: {path_out} already exists. Skipping....')
        return
    if region is not None:
        list = ndownload.get_list_files(date_here, sensor, region, 'DT')
    elif lat_point is not None and lon_point is not None:
        list = ndownload.getscenes_by_point_EarthData_API(sensor, date_here, lat_point, lon_point)

    if len(list) > 0:
        print(f'[INFO] {len(list)} granules identified for date {date_here}')
        for granule in list:
            print(f'[INFO] Downloading granule: {granule}')

            url = ndownload.get_url_download(granule)
            od.do_download(url, path_out, appkey)
    else:
        print(f'[INFO] No granules were found to be downloaded for date: {date_here}')


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    import argparse
    import os

    parser = argparse.ArgumentParser(description='NASA Get Scenes. Patch to retrieve DT scenes')
    parser.add_argument("-m", "--mode", help="Mode", choices=["LIST", "DOWNLOAD", "NRT_TO_DT"])
    parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
    parser.add_argument("-sen", "--sensor", help="Specify sensor: VIIRS, VIIRSJ, AQUA, PACE_AOP", required=True)
    parser.add_argument("-d", "--date",
                        help="Specify a date in yyyymmdd format, or a date list file if option -list_dates is activated",
                        required=True)
    parser.add_argument("-R", "--Region", help="specify the Region Label of the area: BlackSea or Mediterranean",
                        required=False)
    parser.add_argument("-lat_p", "--lat_point", help="Station latitude")
    parser.add_argument("-lon_p", "--lon_point", help="Stationg longitude")
    parser.add_argument("-site", "--site_name", help="Pre-defined sites: VEIT,TRIT,LAIT,GATI")
    parser.add_argument("-ed", "--end_date",
                        help="End date for DOWNLOAD option in yyyymmdd format (=date if this option is ignored")
    parser.add_argument("-list_dates", "--use_list_dates",
                        help="If this option is given, -d shoud be a text file containing a data list in format yyyy-mm-dd",
                        action="store_true")
    parser.add_argument("-o", "--path_out", help="Path out")
    parser.add_argument("-ow", "--overwrite", help="Overwrite download files")
    args = parser.parse_args()

    from nasa_download import NASA_DOWNLOAD
    from datetime import datetime as dt

    try:
        if args.use_list_dates:
            file_list_dates = args.date
            if not os.path.exists(file_list_dates):
                print(f'[ERROR] File {file_list_dates} is not available')
        else:
            date_here = dt.strptime(args.date, '%Y%m%d')
        sensor = args.sensor
        region = None
        lat_point = None
        lon_point = None
        if args.Region:
            if args.Region == 'BlackSea':
                region = 'BS'
            elif args.Region == 'Mediterranean':
                region = 'MED'
        if args.lat_point:
            lat_point = float(args.lat_point)
        if args.lon_point:
            lon_point = float(args.lon_point)
        if args.site_name:
            lat_point, lon_point = get_lat_lon_from_site_name(args.site_name)
            if lat_point is None and lon_point is None:
                exit(1)
        # date_here = dt(2022,9,30)
    except:
        print('[ERROR] Error parsing arguments')
        exit(1)

    if args.mode == 'LIST':
        ndownload = NASA_DOWNLOAD()
        if region is not None:
            list = ndownload.get_list_files(date_here, sensor, region, 'DT')
        elif lat_point is not None and lon_point is not None:
            list = ndownload.getscenes_by_point_EarthData_API(sensor, date_here, lat_point, lon_point)

    if args.mode == 'DOWNLOAD':
        ndownload = NASA_DOWNLOAD()
        path_out = None
        if args.path_out and os.path.isdir(args.path_out):
            ndownload.sensors[sensor]['nrt_cnr_server_dir'] = args.path_out

        if not args.use_list_dates:
            end_date = date_here
            if args.end_date:
                end_date = dt.strptime(args.end_date, '%Y%m%d')
        import obdaac_download as od
        import os
        from datetime import timedelta

        appkey = '22da4a89034645c3653f75dcd49ea11639976cef'

        if args.use_list_dates:
            fdates = open(file_list_dates, 'r')
            for line in fdates:
                try:
                    date_here = dt.strptime(line.strip(), '%Y-%m-%d')
                    download_date(ndownload, sensor, region, lat_point, lon_point, date_here)
                except:
                    print(f'[WARNING] Date {line.strip()} is not in the correct format. Skipping...')
            fdates.close()
            exit(0)

        while date_here <= end_date:
            download_date(ndownload, sensor, region, lat_point, lon_point, date_here)

            date_here = date_here + timedelta(hours=24)

    if args.mode == 'NRT_TO_DT':
        ndownload = NASA_DOWNLOAD()
        list_nasa = ndownload.get_list_files(date_here, sensor, region, 'DT')
        list_nasa = [l.strip() for l in list_nasa]
        list_dest = ndownload.get_list_files_orig(sensor, date_here)
        print(list_dest)

        # check if files are in the ftp
        for name_orig in list_dest:
            name = list_dest[name_orig]
            if name in list_nasa:
                print('OK: ', name)
            else:
                namen = ndownload.get_date_with01(name, sensor)
                if namen in list_nasa:
                    print('OK: ', namen)
                    list_dest[name_orig] = namen
                else:
                    print('WARNING', name, '->', namen)
                    list_dest[name_orig] = 'NaN'

        # download
        import obdaac_download as od
        import os

        appkey = '22da4a89034645c3653f75dcd49ea11639976cef'
        for name_orig in list_dest:
            name = list_dest[name_orig]
            dir_out = ndownload.get_path_orig(sensor, date_here)
            url = ndownload.get_url_download(name)
            print(f'[INFO] Downloading: {name} to {dir_out}')
            od.do_download(url, dir_out, appkey)

            file_dest = os.path.join(dir_out, name)
            file_orig = os.path.join(dir_out, name_orig)
            if os.path.exists(file_dest) and os.path.join(file_orig):
                nrtfolder = os.path.join(dir_out, 'NRT')
                if not os.path.exists(nrtfolder):
                    os.mkdir(nrtfolder)
                file_orig_copy = os.path.join(nrtfolder, name_orig)
                os.rename(file_orig, file_orig_copy)
                os.rename(file_dest, file_orig)
