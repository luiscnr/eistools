import argparse

parser = argparse.ArgumentParser(description='NASA Get Scenes. Patch to retrieve DT scenes')
parser.add_argument("-m", "--mode", help="Mode", choices=["LIST", "DOWNLOAD", "NRT_TO_DT", "TEST"])
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
parser.add_argument("-sen", "--sensor",
                    help="Specify sensor: VIIRS, VIIRSJ, AQUA, PACE_AOP, PACE_SCI (options in the configuration file)",
                    required=True)
parser.add_argument("-d", "--date",
                    help="Specify a date in yyyymmdd format, or a date list file if option -list_dates is activated",
                    required=True
                    )
parser.add_argument("-R", "--Region",
                    help="Specify the Region Label of the area: BlackSea or Mediterranean, for searching using the browser",
                    required=False)
parser.add_argument("-lat_p", "--lat_point", help="Station latitude")
parser.add_argument("-lon_p", "--lon_point", help="Stationg longitude")
parser.add_argument("-site", "--site_name", help="Pre-defined sites in configuration file")
parser.add_argument("-ed", "--end_date",
                    help="End date for DOWNLOAD option in yyyymmdd format (=date if this option is ignored")
parser.add_argument("-list_dates", "--use_list_dates",
                    help="If this option is given, -d shoud be a text file containing a data list in format yyyy-mm-dd",
                    action="store_true")
parser.add_argument("-nrt", "--use_nrt", help="Download/list NRT files", action="store_true")
parser.add_argument("-o", "--path_out", help="Path out")
parser.add_argument("-ow", "--overwrite", help="Overwrite download files")
args = parser.parse_args()


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


def download_date(ndownload, sensor, site_name, lat_point, lon_point, date_here):
    import os
    import obdaac_download as od

    path_out = ndownload.get_path_orig(sensor, date_here)
    is_dt = True if not args.use_nrt else False
    if site_name is not None:
        list = ndownload.getscenes_by_site_EarthData_API(sensor,date_here,site_name,is_dt)
    elif lat_point is not None and lon_point is not None:
        list = ndownload.getscenes_by_point_EarthData_API(sensor, date_here, lat_point, lon_point, is_dt)

    if len(list) > 0:
        print(f'[INFO] {len(list)} granules identified for date {date_here}')
        for granule in list:
            print(f'[INFO] Downloading granule: {granule}')
            file_granule = os.path.join(path_out, granule)
            if os.path.exists(file_granule) and not args.overwrite:
                print(f'[WARNING] Ouput file: {file_granule} already exists. Skipping....')
                return

            url = ndownload.get_url_download(granule)
            od.do_download(url, path_out, ndownload.apikey)
    else:
        print(f'[INFO] No granules were found to be downloaded for date: {date_here}')

def make_test():
    print('1')
    from nasa_download import NASA_DOWNLOAD
    ndownload = NASA_DOWNLOAD()
    from datetime import datetime as dt
    from datetime import timedelta
    work_date = dt(2025, 5, 5)
    geo_limits = [10, 15, 85, 90]
    list = ndownload.getscenes_by_region_EarthData_API('PACE_AOP', work_date, geo_limits, True)
    print(list)
    print('2')
    return True

def make_test_2():
    from nasa_download import NASA_DOWNLOAD
    ndownload = NASA_DOWNLOAD()
    from datetime import datetime as dt
    from datetime import timedelta
    fout = '/mnt/c/DATA_LUIS'
    fw = open(fout, 'w')
    work_date = dt(2019, 6, 1)
    end_date = dt(2019, 9, 30)
    geo_limits = [53, 65, 12, 31]
    fw.write('Date;NImages')
    while work_date <= end_date:
        print('DATE: ', work_date)
        # work_date_str = work_date.strftime('%Y%m%d')
        list = ndownload.getscenes_by_region_EarthData_API('AQUA', work_date, geo_limits, True)
        fw.write('\n')
        fw.write(f'{work_date.strftime("%Y-%m-%d")};{len(list)}')
        work_date = work_date + timedelta(hours=24)

    fw.close()


def main():
    ##for testing
    if args.mode == 'TEST':
        make_test()
        return

    ##Getting arguemnts
    import os
    from nasa_download import NASA_DOWNLOAD
    from datetime import datetime as dt
    ndownload = NASA_DOWNLOAD()
    site_name = None
    lat_point = None
    lon_point = None
    date_here = None
    try:
        if args.use_list_dates:
            file_list_dates = args.date
            if not os.path.exists(file_list_dates):
                print(f'[ERROR] File {file_list_dates} is not available')
                return
        else:
            date_here = dt.strptime(args.date, '%Y%m%d')
        sensor = ndownload.check_sensor(args.sensor)
        if sensor is None:
            return
        if args.lat_point and args.lon_point:
            lat_point = float(args.lat_point)
            lon_point = float(args.lon_point)
        else:
            if args.site_name:
                site_name = ndownload.check_site(args.site_name)
                if site_name is None:
                    return
            else:
                print(
                    f'[ERROR] Geographic information is not provided. Please use -site (--site_name) or provide point coordinates using -lat_p(--lat_point) and -lon_p(--lon_point) options')
                return
    except:
        print('[ERROR] Error parsing arguments')
        return

    if args.mode == 'LIST':
        if args.use_list_dates:
            print(f'[WARNING] LIST mode is not allowed with -list_date (--use_list_dates) argument')
            return
        is_dt = True if not args.use_nrt else False
        list = None
        if site_name is not None:
            list = ndownload.getscenes_by_site_EarthData_API(sensor, date_here, site_name, is_dt)
        elif lat_point is not None and lon_point is not None:
            list = ndownload.getscenes_by_point_EarthData_API(sensor, date_here, lat_point, lon_point, is_dt)
        if list is not None:
            for granule in list:
                print(granule)

    if args.mode == 'DOWNLOAD':
        if args.path_out and os.path.isdir(args.path_out):
            ndownload.sensors[sensor]['nrt_cnr_server_dir'] = args.path_out

        if not args.use_list_dates:
            if date_here is None:
                return
            end_date = date_here
            if args.end_date:
                end_date = dt.strptime(args.end_date, '%Y%m%d')

        import os
        from datetime import timedelta

        #appkey = '22da4a89034645c3653f75dcd49ea11639976cef'

        if args.use_list_dates:
            fdates = open(file_list_dates, 'r')
            for line in fdates:
                try:
                    date_here = dt.strptime(line.strip(), '%Y-%m-%d')
                    download_date(ndownload, sensor, site_name, lat_point, lon_point, date_here)
                except:
                    print(f'[WARNING] Date {line.strip()} is not in the correct format. Skipping...')
            fdates.close()
            return

        while date_here <= end_date:
            download_date(ndownload, sensor, site_name, lat_point, lon_point, date_here)

            date_here = date_here + timedelta(hours=24)

    if args.mode == 'NRT_TO_DT':
        list_nasa = ndownload.get_list_files(date_here, sensor, site_name, 'DT')
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


if __name__ == '__main__':
    main()
