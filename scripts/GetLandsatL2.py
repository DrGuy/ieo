#!/usr/bin/env python
# By Guy Serbin, EOanalytics Ltd.
# Talent Garden Dublin, Claremont Ave. Glasnevin, Dublin 11, Ireland
# email: guyserbin <at> eoanalytics <dot> ie

# version 1.5

# This script was modified from MakeESPAproclist.py.
# This script identifies and downloads Level-2 data from the USGS Landsat 
# Collection 2

# This script uses code from https://m2m.cr.usgs.gov/api/docs/example/download_landsat_c2-py

import os, sys, glob, datetime, argparse, requests, threading, re, json, time #, ieo
from osgeo import ogr, osr

try: # This is included as the module may not properly install in Anaconda.
    import ieo
except:
    # ieodir = os.getenv('IEO_INSTALLDIR')
    # if not ieodir:
    print('Error: IEO failed to load. Please input the location of the directory containing the IEO installation files.')
    ieodir = input('IEO installation path: ')
    if os.path.isfile(os.path.join(ieodir, 'ieo.py')):
        sys.path.append(ieodir)
        import ieo
    else:
        print('Error: that is not a valid path for the IEO module. Exiting.')
        sys.exit()

global proclevels, pathrowdict



# These functions copied and modified from https://m2m.cr.usgs.gov/api/docs/example/download_landsat_c2-py

# Send http request
def sendRequest(url, data, apiKey = None, exitIfNoResponse = True):  
    json_data = json.dumps(data)
    if apiKey == None:
        response = requests.post(url, json_data)
    else:
        headers = {'X-Auth-Token': apiKey}              
        response = requests.post(url, json_data, headers = headers)  
    try:
      httpStatusCode = response.status_code 
      if response == None:
          if exitIfNoResponse: 
              print('Communication error: No output from service')
              ieo.logerror('Communication error', "No output from service")
              sys.exit()
          else: return False
      output = json.loads(response.text)
      if output['errorCode'] != None:
          if exitIfNoResponse: 
              print('{}: {}').format(output['errorCode'], output['errorMessage'])
              ieo.logerror(output['errorCode'], output['errorMessage'])
              sys.exit()
          else: return False
      if  httpStatusCode == 404:
          if exitIfNoResponse: 
              print('404: Not Found')
              ieo.logerror('404', 'Not Found')
              sys.exit()
          else: return False
      elif httpStatusCode == 401: 
          if exitIfNoResponse: 
              print('401: Unauthorized')
              ieo.logerror('401', 'Unauthorized')
              sys.exit()
          else: return False
      elif httpStatusCode == 400:
          if exitIfNoResponse: 
              print(f'Error Code: {httpStatusCode}')
              ieo.logerror("Error Code", httpStatusCode)
              sys.exit()
          else: return False
    except Exception as e: 
          response.close()
          if exitIfNoResponse: 
              print(f'sendRequest: {e}')
              ieo.logerror('sendRequest', e)
              sys.exit()
          else: return False
    response.close()
    
    now = datetime.datetime.now()
    with open(os.path.join(ieo.logdir, 'GetLandsat_sendRequest_{}.txt'.format(now.strftime('%Y%m%d-%H%M%S'))), 'w') as outfile:
        json.dump(output, outfile)
    
    return output['data']

def downloadFile(url):
    sema.acquire()
    try:        
        response = requests.get(url, stream=True)
        disposition = response.headers['content-disposition']
        filename = re.findall("filename=(.+)", disposition)[0].strip("\"")
        print(f"Downloading {filename} ...\n")
        # if path != "" and path[-1] != "/":
        #     filename = "/" + filename
        f = os.path.join(ieo.ingestdir, filename)
        open(f, 'wb').write(response.content)
        print(f"Downloaded {filename}\n")
        sema.release()
    except Exception as e:
        ieo.logerror(url, f"Failed to download from {url}. Will try to re-download.")
        sema.release()
        runDownload(threads, url)
    
def runDownload(threads, url):
    thread = threading.Thread(target=downloadFile, args=(url,))
    threads.append(thread)
    thread.start()

# 

def getscenedata(layer, localscenelist):
    scenedata = {'ProductIDs' : {}}
    # if ieo.useS3:
    #     scenedata['ingested'] = []
        # bucketdict = ieo.getbucketobjects(ieo.archivebucket)
        # if len(bucketdict['landsat'].keys()) > 0:
        #     for year in bucketdict.keys():
        #         if len(year.keys()) > 0:
        #             for month in year.keys():
        #                 if len(month.keys()) > 0:
        #                     for day in month.keys():
        #                         if isinstance(day, list):
        #                             if len(day) > 0:
        #                                 for item in day:
        #                                     i = item.find('.')
        #                                     scenedata['ingested'].append(item[:i])
    for feature in layer:
        sceneID = feature.GetField("sceneID")
        ProductID = feature.GetField('LANDSAT_PRODUCT_ID_L2')
        includescene = True
        # sunEl = feature.GetField("sunElevation_L1")
        sensor = feature.GetField("SensorID")
        acqDateval = feature.GetField("acquisitionDate")
        try:
            acqDate = datetime.datetime.strptime(acqDateval, '%Y/%m/%d')
            datestr = acqDate.strftime('%Y%j')
        except:
            print('Error: "acqDate" field missing acquisition date data, attempting to correct.')
            ieo.logerror(sceneID, '"acquisitionDate" field missing acquisition date data, attempting to correct.')
            datestr = sceneID[9:16]
            acqDate = datetime.datetime.strptime(datestr, '%Y%j')
#            feature.SetField('acqDate', acqDate)
        # proclevel = feature.GetField("DATA_TYPE_L1")
        proclevel = None
        # if sceneID[2:3] == '8' and ((datestr in L8exclude) or (sensor != 'OLI_TIRS')):
        #     includescene = False
        # if sceneID[2:3] == '7' and datestr in L7exclude:
        #     includescene = False
        # if sunEl: # ignore Null values
        if includescene: # and sunEl >= args.minsunel:
            SR_file = feature.GetField('Surface_reflectance_tiles')
            scenedata['ProductIDs'][ProductID] = sceneID
            scenedata[sceneID] = {'LANDSAT_PRODUCT_ID_L2' : ProductID,
                                    'acquisitionDate' : acqDate, 
                                    'Path' : feature.GetField("path"), 
                                    'Row' : feature.GetField("row"), 
                                    'SensorID' : sensor,  
                                    'cloudCoverFull' : feature.GetField("cloudCoverFull"), 
                                    'CLOUD_COVER_LAND' : feature.GetField("CLOUD_COVER_LAND"),
                                    # 'sunElevation' : sunEl, 
                                    'Surface_reflectance_tiles' : SR_file, 
                                    'proclevel' : proclevel
                                    }
            if args.verbose:
                        print('SR_file: {}.'.format(SR_file))
            if not SR_file == None:
                if len(SR_file) > 0:
                    if args.verbose:
                        print('Adding {} to local scene list.'.format(sceneID))
                    localscenelist.append(sceneID[:16])
            elif ieo.useS3:
                if not ProductID in localscenelist:
                    if args.verbose:
                        print('Adding {} to local scene list.'.format(ProductID))
                    localscenelist.append(ProductID)
            elif SR_file and not args.usesrdir:
                if os.path.isfile(SR_file):
                    if args.verbose:
                        print('Adding {} to local scene list.'.format(sceneID))
                    localscenelist.append(sceneID[:16])
    if args.verbose:
        print('Total locally ingested scenes: {}'.format(len(localscenelist)))
    return scenedata, localscenelist

def scenesearch(scenedata, sceneID, pathrowdict): # This function is still Ireland specific
    keys = scenedata.keys()
    scout = []
    r = min(pathrowdict[scenedata[sceneID]['Path']])
#    if scenedata[sceneID]['Path'] == 207 or scenedata[sceneID]['Path'] == 208:
#        r = 21
#    else:
#        r = 22
    if scenedata[sceneID]['Surface_reflectance_tiles']:
        if os.path.exists(scenedata[sceneID]['Surface_reflectance_tiles']):
            while r <= max(pathrowdict[scenedata[sceneID]['Path']]):
                if r != scenedata[sceneID]['Row']:
                    s = '{}{:03d}{}'.format(sceneID[:6], r, sceneID[9:16])
                    sc = [y for y in keys if s in y]
                    for s in sc:
                        if not s in scout:
                            scout.append(s)
                r += 1
    return scout    

def findmissing(procdict, scenedata, localscenelist, cctype):
    keys = scenedata.keys()
    for sceneID in keys:        
        if sceneID != 'ProductIDs':
            if (not sceneID[:16] in localscenelist) or (ieo.useS3 and (not sceneID['LANDSAT_PRODUCT_ID_L2'] in localscenelist)):
                try:
                    if sceneID[2:3] == '8' and any(sceneID[9:16] in key for key in procdict['8']['scenelist']) and (not any(sceneID in key for key in procdict['8']['scenelist'])) and scenedata[sceneID][cctype] < 100.0:
                        print('Adding {} to Landsat 8 download list.'.format(sceneID))
        #                if not sceneID[9:16] in l8.keys() and any(sceneID[9:16] == key[9:16] for key in l8.keys()):
        #                    l8[sceneID[9:16]] = [sceneID]
        #                else:
                        procdict['8']['scenelist'].append(sceneID)
                    elif sceneID[2:3] == '7' and any(sceneID[9:16] in key for key in procdict['7']['scenelist']) and (not any(sceneID in key for key in procdict['7']['scenelist'])) and scenedata[sceneID][cctype] < 100.0:
                        print('Adding {} to Landsat 7 download list.'.format(sceneID))
        #                if not sceneID[9:16] in l47.keys():
        #                    l47[sceneID[9:16]] = [sceneID]
        #                else:
                        procdict['7']['scenelist'].append(sceneID) 
                    elif (sceneID[2:3] == '4' or sceneID[2:3] == '5') and any(sceneID[9:16] in key for key in procdict['4-5']['scenelist']) and (not any(sceneID in key for key in procdict['4-5']['scenelist'])) and scenedata[sceneID][cctype] < 100.0:
                        print('Adding {} to Landsat 4-5 download list.'.format(sceneID))
        #                if not sceneID[9:16] in l47.keys():
        #                    l47[sceneID[9:16]] = [sceneID]
        #                else:
                        procdict['4-5']['scenelist'].append(sceneID) 
                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(exc_type, fname, exc_tb.tb_lineno)
                    print('ERROR: {} {} {} {}.'.format(sceneID, exc_type, fname, exc_tb.tb_lineno))
                    ieo.logerror(sceneID, '{} {} {}'.format(exc_type, fname, exc_tb.tb_lineno))
#        sc = scenesearch(scenedata, sceneID)
#        if len(sc) > 0:
#            for s in sc:
#                if not scenedata[s][6]:
#                    if s[2:3] == '8' and not s in l8:
#                        print('Adding %s to Landsat 8 processing list.'%s)
#                        l8.append(s)
#                    elif not s in l47:
#                        print('Adding %s to Landsat 4-7 processing list.'%s)
#                        l47.append(s) 
    return procdict

def populatelists(procdict, scenedata, localscenelist):
    for sceneID in scenedata.keys():
        if sceneID != 'ProductsIDs':
            try:
                if args.ignorelocal:
                    print('Ignoring local files.')
                    localscenelist = []
                acqDate = scenedata[sceneID]['acquisitionDate']
                path = scenedata[sceneID]['Path']
                row = scenedata[sceneID]['Row']
                scenesensor = scenedata[sceneID]['SensorID']
                if args.ccland:
                    cc = scenedata[sceneID]['CLOUD_COVER_LAND']
                    if not cc:
                        cc = 0.0
                    maxcc = args.maxccland
                    cctype = 'CLOUD_COVER_LAND'
                else:
                    cc = scenedata[sceneID]['cloudCoverFull']
                    if not cc:
                        cc = 0.0
                    maxcc = args.maxcc
                    cctype = 'cloudCoverFull' 
                if cc == None:
                    cc = 0.0
                # sunEl = scenedata[sceneID]['sunElevation']
        #        SR = scenedata[sceneID]['Surface_reflectance_tiles']
                proclevel = scenedata[sceneID]['proclevel']
            
                if ((not sceneID[:16] in localscenelist and not ieo.useS3) or (ieo.useS3 and sceneID not in localscenelist)) and (cc <= maxcc) and (proclevel in proclevels): # and (sunEl >= args.minsunel) # Only run this for scenes that aren't present on disk or if we choose to ignore local copies.
                # if (feature.GetField("SR_path") == None or args.ignorelocal) and feature.GetField("CCFull") <= args.maxcc and feature.GetField("sunEl") >= args.minsunel:
                    # sceneID = feature.GetField("sceneID")
                    if args.landsat:
                        if args.landsat != int(sceneID[2:3]):
                            continue
                    if args.path:
                        if args.path != path:
                            continue
                        # else:
                        #     print(path)
                    if args.row:
                        if args.row != row:
                            continue
                        # else:
                        #     print(row)
                    if args.sensor: 
                        if sensor != scenesensor:
                            continue
                    
                    year = int(sceneID[9:13])
                    doy = int(sceneID[13:16])    
                    if args.startyear or args.endyear:
                        if args.startyear > args.endyear:
                            endyear = args.startyear
                            startyear = args.endyear
                        else: 
                            endyear = args.endyear
                            startyear = args.startyear
                        if year < startyear or year > endyear:
                            continue
                    if args.startdoy and args.enddoy: # This might be programmed later to restrict dates to specific dates/ times of year
                        if args.startdoy < args.enddoy:
                            if doy < args.startdoy or doy > args.enddoy:
                                continue
                        else:
                            if args.startyear: 
                                if year == startyear and doy < args.startdoy:
                                    continue
                            if args.endyear:
                                if year == endyear and doy > args.enddoy:
                                    continue
                            if doy > args.enddoy and doy < args.startdoy:
                                continue
                    
                    if (acqDate >= args.startdate) and (acqDate <= args.enddate):
        #               
                        print('Scene {}, cloud cover of {} percent, added to list.'.format(sceneID, cc))
                        if sceneID[2:3] == '4' or sceneID[2:3] == '5':
                            i = '4-5'
                        else:
                            i = sceneID[2:3]
                        if not sceneID in procdict[i]['scenelist']: #not sceneID[9:16] in L7exclude and (scenesensor == 'LANDSAT_TM' or scenesensor == 'LANDSAT_ETM' or 'LANDSAT_ETM_SLC_OFF') and 
                            procdict[i]['scenelist'].append(sceneID)
                            if args.allinpath:
                                sc = scenesearch(scenedata, sceneID, pathrowdict)
                                if len(sc) > 0:
                                    for s in sc:
                                        if not s in procdict[i]['scenelist']:
                                            print('Also adding scene {} to the processing list.'.format(sceneID))
                                            procdict[i]['scenelist'].append(s)
                        # elif sceneID[2:3] == '7' and not sceneID in procdict['7']['scenelist']: #not sceneID[9:16] in L7exclude and (scenesensor == 'LANDSAT_TM' or scenesensor == 'LANDSAT_ETM' or 'LANDSAT_ETM_SLC_OFF') and 
                        #     procdict['7']['scenelist']
                        # elif (sceneID[2:3] == '4' or sceneID[2:3] == '5') and not sceneID in procdict['4-5']['scenelist']: #not sceneID[9:16] in L7exclude and (scenesensor == 'LANDSAT_TM' or scenesensor == 'LANDSAT_ETM' or 'LANDSAT_ETM_SLC_OFF') and 
                        #     procdict['4-5']['scenelist']
                        # # if not sceneID[9:16] in l47.keys():
                        # #         l47[sceneID[9:16]] = [sceneID]
                        # #     elif not sceneID in l47[sceneID[9:16]]:
                        # #         l47[sceneID[9:16]].append(sceneID)
                        #     if args.allinpath:
                        #         sc = scenesearch(scenedata, sceneID, pathrowdict)
                        #         if len(sc) > 0:
                        #             for s in sc:
                        #                 if not s in l47[sceneID[9:16]]:
                        #                     print('Also adding scene {} to the processing list.'.format(sceneID))
                        #                     l47[sceneID[9:16]].append(s)
                            
                #        elif scenesensor=='LANDSAT_ETM':
                #            l7.append(sceneID)
                #        elif scenesensor=='LANDSAT_ETM_SLC_OFF' and not sceneID[9:16] in L7exclude:
                #            l7slcoff.append(sceneID)
                        # elif sceneID[2:3] == '8' and not sceneID[9:16] in L8exclude:
                        #     if not sceneID[9:16] in l8.keys():
                        #         l8[sceneID[9:16]] = [sceneID]
                        #     elif not sceneID in l8[sceneID[9:16]]:
                        #         l8[sceneID[9:16]].append(sceneID)
                        #     if args.allinpath:
                        #         sc = scenesearch(scenedata, sceneID, pathrowdict)
                        #         if len(sc) > 0:
                        #             for s in sc:
                        #                 if not s in l8[sceneID[9:16]]:
                        #                     print('Also adding scene {} to the processing list.'.format(sceneID))
                        #                     l8[sceneID[9:16]].append(s)
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)
                print('ERROR: {} {} {} {}.'.format(sceneID, exc_type, fname, exc_tb.tb_lineno))
                ieo.logerror(sceneID, '{} {} {}'.format(exc_type, fname, exc_tb.tb_lineno))
            
    return procdict, cctype

# Exclusion of problematic dates:

# L8exclude = []
# for i in range(21):
#     L8exclude.append('2015{:03d}'.format(30 + i))
# for i in range(9):
#     L8exclude.append('2016{:03d}'.format(50 + i))

# L7exclude = []
# for i in range(15):
#     L7exclude.append('2016{:03d}'.format(151 + i))
# L7exclude.append('2017074')
# L7exclude.append('2017075')
# L7exclude.append('2017076')
# Set various other variables

if __name__ == '__main__':     
# Parse command line arguments
    parser = argparse.ArgumentParser('Create ESPA LEDAPS/ LaSRC process list for missing scenes.')
    parser.add_argument('-u','--username', type = str, default = None, help = 'USGS/EROS Registration System (ERS) username.')
    parser.add_argument('-p', '--password', type = str, default = None, help = 'USGS/EROS Registration System (ERS) password.')
    parser.add_argument('-c', '--catalogID', type = str, default = 'EE', help = 'USGS/EROS Catalog ID (default = "EE").')
    parser.add_argument('-f', '--filetype', required=False, default = 'bundle', choices=['bundle', 'band'], help='File types to download, "bundle" for bundle files and "band" for band files')
    parser.add_argument('--path', type = int, default = None, help = 'WRS-2 Path')
    parser.add_argument('--row', type = int, default = None, help = 'WRS-2 Row. If this is specified, then --path must also be specified.')
    parser.add_argument('--maxcc', default = 100.0, type = int, help = 'Maximum cloud cover in percent')
    parser.add_argument('--maxccland', default = 30.0, type = int, help = 'Maximum cloud cover over land in percent')
    parser.add_argument('--ccland', default = True, type = bool, help = 'Use land cloud cover, not full scene (Default = True)')
    parser.add_argument('--startdate', type = str, default = '1982/01/01', help = 'Starting date, YYYY/MM/DD')
    parser.add_argument('--enddate', type = str, help = 'Ending date, YYYY/MM/DD')
    parser.add_argument('--startdoy', type = int, help = 'Starting day of year, 1-366')
    parser.add_argument('--enddoy', type = int, help = 'Ending day of year, 1-366. If less than starting day of year then this will be used to span the new year.')
    parser.add_argument('--startyear', type = int, help = 'Starting year')
    parser.add_argument('--endyear', type = int, help = 'Ending year. If less than starting starting year then these will be swapped.')
    parser.add_argument('--landsat', type = int, help = 'Landsat number (4, 5, 7, or 8 only).')
    parser.add_argument('--sensor', type = str, help = 'Landsat sensor: TM, ETM, ETM_SLC_OFF, OLI, OLI_TIRS, TIRS')
    parser.add_argument('--shp', type = str, default = ieo.landsatshp, help = 'Full path and filename of alternative shapefile.')
    parser.add_argument('-o', '--outdir', type = str, default = os.path.join(ieo.catdir, 'Landsat', 'ESPA_processing_lists'), help = 'Output directory')
    parser.add_argument('--ignorelocal', type = bool, default = False, help = 'Ignore presence of local scenes.')
    parser.add_argument('--srdir', type = str, default = ieo.srdir, help = 'Local SR scene directory')
    parser.add_argument('--usesrdir', type = bool, default = False, help = 'Use local index of scenes rather than shapefile stored data')
    parser.add_argument('--allinpath', type = bool, default = True, help = 'Include missing scenes in path, even if they are too cloudy.')
    # parser.add_argument('--minsunel', type = float, default = 15.0, help = 'Sun elevation beneath which scenes will be ignored.')
    parser.add_argument('--separate', type = bool, default = False, help = 'Separate output files for Landsats 4-7 and 8.')
    parser.add_argument('--L1GS', type = bool, default = False, help = 'Also get L1GS and L1GT scenes.')
    parser.add_argument('--L1GT', type = bool, default = False, help = 'Also get L1GT scenes but exclude L1GS.')
    parser.add_argument('--ALL', type = bool, default = True, help = 'Get any scene regardless of processing level.')
    parser.add_argument('--maxthreads', type = int, default = 5, help = 'Threads count for downloads, default = 5')
    parser.add_argument('--serviceURL', type = str, default = 'https://m2m.cr.usgs.gov/api/api/json/stable/', help = 'URL to download Landsat scenes')
    # parser.add_argument('-f', '--filetype', required = False, choices=['bundle', 'band'], help='File types to download, "bundle" for bundle files and "band" for band files')
    parser.add_argument('--idfield', required = False, default = 'displayId', type = str, choices = ['displayId', 'entityId'], help='Field to use for Landsat scene ID.')
    parser.add_argument('--verbose', action = 'store_true', help = 'Display more messages during execution.')
    args = parser.parse_args()
    
    print('Use S3: {}'.format(ieo.useS3))
    print("\nRunning Scripts...\n")
    startTime = datetime.datetime.now()
    
    username = args.username
    password = args.password
    filetype = args.filetype
    serviceUrl = args.serviceURL
    
    # Login
    payload = {'username' : username, 'password' : password}    
    apiKey = sendRequest(serviceUrl + "login", payload)    
    print("API Key: " + apiKey + "\n")
    
    # type conversions of start and end dates to datetime.datetime objects
    args.startdate = datetime.datetime.strptime(args.startdate,'%Y/%m/%d')
    if args.enddate:
        args.enddate = datetime.datetime.strptime(args.enddate,'%Y/%m/%d')
    else:
        args.enddate = datetime.datetime.today()
    
    idField = args.idfield
    
    outdir = args.outdir
    infile = args.shp
    today = datetime.datetime.today()
    todaystr = today.strftime('%Y%m%d-%H%M%S')
    sema = threading.Semaphore(value = args.maxthreads)
    label = datetime.datetime.now().strftime("%Y%m%d_%H%M%S") # Customized label using date time
    threads = []
    
    localscenelist = []
    
    if args.sensor:
        if 'TM' in args.sensor:
            sensor='LANDSAT_{}'.format(args.sensor)
        elif not ('OLI' in args.sensor or 'TIRS' in args.sensor):
            print('Error: this sensor is not supported. Acceptable sensors are: TM, ETM, ETM_SLC_OFF, OLI, OLI_TIRS, TIRS. Leaving --sensor blank will search for all sensors. Exiting.')
            exit()
        else:
            sensor = args.sensor
    else:
        sensor = ''
    
    if not args.path:
        path = 0
    else:
        path = args.path
    if not args.row:
        row = 0
    else:
        row = args.row
    
    if args.startdoy or args.enddoy:
        if not (args.startdoy and args.enddoy):
            print('Error: if used, both --startdoy and --enddoy must be defined. Exiting.')
            exit()
        
    if ieo.useS3:
        print('Seaching S3 bucket "{}" for ingested scenes.'.format(ieo.archivebucket))
        bucketdict = ieo.getbucketobjects(ieo.archivebucket)
        for key in bucketdict.keys():
            if len(bucketdict[key]) > 0:
                for item in bucketdict[key]:
                    if '/' in item:
                        i = item.rfind('/') + 1
                    else:
                        i = 0
                    j = item.find('.tar')
                    print('Found ingested scene: {}'.format(item[i:j]))
                    localscenelist.append(item[i:j])
        print('Found {} ingested scenes.'.format(len(localscenelist)))
    elif args.usesrdir:
        dirs = [args.srdir, os.path.join(args.srdir,'L1G')]
        for d in dirs:
            flist = glob.glob(os.path.join(d,'L*.hdr'))
            if len(flist) > 0:
                for f in flist:
                    parentrasters = ieo.readenvihdr(f)['parent rasters']
                    for r in parentrasters:
                        sceneid = os.path.basename(r)[:21]
                        if not sceneid in localscenelist:
                            localscenelist.append(os.path.basename(f)[:16])
        
                        
    # the next set of lines will be deprecated as we are now using Level-2 Landsat data
    proclevels = ['L1TP']
    if args.L1GS:
        proclevels = ['L1TP', 'L1GT', 'L1GS']
    elif args.L1GT:
        proclevels = ['L1TP', 'L1GT']
    elif args.ALL:
        proclevels = ['L1TP', 'L1GT', 'L1GS', None]
    
    pathrowdict = {}
    driver = ogr.GetDriverByName("GPKG")
    dataSource = driver.Open(ieo.ieogpkg, 0)
    layer = dataSource.GetLayer(ieo.WRS2)
    for feature in layer:
        path = feature.GetField('Path')
        row = feature.GetField('Row')
        if not path in pathrowdict.keys():
            pathrowdict[path] = []
        if not row in pathrowdict[path]:
            pathrowdict[path].append(row)
    dataSource = None
    for key in pathrowdict.keys():
        pathrowdict[key].sort()
    
    print('Opening {}'.format(infile))
    if args.path and args.row:
        print('Searching for scenes from WRS-2 Path {}, Row {}, with a maximum cloud cover of {:0.1f}%.'.format(args.path, args.row, args.maxcc))
    driver = ogr.GetDriverByName("GPKG")
    dataSource = driver.Open(ieo.catgpkg, 0)
    layer = dataSource.GetLayer(ieo.landsatshp)
    layer_defn = layer.GetLayerDefn()
    field_names = [layer_defn.GetFieldDefn(i).GetName() for i in range(layer_defn.GetFieldCount())]
    scenedata, localscenelist = getscenedata(layer, localscenelist)
        
    
    l8 = {}
    l47 = {}
    l7slcoff = {}
    l5 = {}
    idField = 'displayId'
    procdict = {
        '8' : {
            'datasetName' : 'landsat_ot_c2_l2',
            'scenelist' : []
            },
        '7' : {
            'datasetName' : 'landsat_etm_c2_l2',
            'scenelist' : []
            },
        '4-5' : {
            'datasetName' : 'landsat_tm_c2_l2',
            'scenelist' : []}    
        }
    
    procdict, cctype = populatelists(procdict, scenedata, localscenelist)
    
    if args.allinpath:
        print('Now searching for missing scenes from same paths and dates of locally stored scenes.')
        procdict = findmissing(procdict, scenedata, localscenelist, cctype)
    for key in procdict.keys():
        entityIds = []
        datasetName = procdict[key]['datasetName']
        if len(procdict[key]['scenelist']) > 0:
            for sceneID in procdict[key]['scenelist']:
                entityIds.append(scenedata[sceneID]['LANDSAT_PRODUCT_ID_L2'])
            print('Found {} Landsat {} scenes to download.'.format (len(entityIds), key))
            
            now = datetime.datetime.now()
            
            # Add scenes to a list
            listId = f"temp_{datasetName}_list" # customized list id
            # listId = "temp_{}_{}_list".format(datasetName, now.strftime('%Y%m%d%H%M%S')) # customized list id
            payload = {
                "listId": listId,
                'idField' : idField,
                "entityIds": entityIds,
                "datasetName": datasetName
                }
      
            # print(payload)
            
            print("Adding scenes to list...\n")
            count = sendRequest(serviceUrl + "scene-list-add", payload, apiKey)    
            print("Added", count, "scenes\n")
            
            # Get download options
            payload = {
                "listId": listId,
                "datasetName": datasetName
            }
              
            print("Getting product download options...\n")
            products = sendRequest(serviceUrl + "download-options", payload, apiKey)
            print("Got product download options\n")
            
            # print(len(products))
            
            # Select products
            downloads = []
            if filetype == 'bundle':
                # select bundle files
                for product in products:        
                    if product["bulkAvailable"]:               
                        downloads.append({"entityId":product["entityId"], "productId":product["id"]})
            elif filetype == 'band':
                # select band files
                for product in products:  
                    if product["secondaryDownloads"] is not None and len(product["secondaryDownloads"]) > 0:
                        for secondaryDownload in product["secondaryDownloads"]:
                            if secondaryDownload["bulkAvailable"]:
                                downloads.append({"entityId":secondaryDownload["entityId"], "productId":secondaryDownload["id"]})
            else:
                # select all available files
                for product in products:        
                    if product["bulkAvailable"]:               
                        downloads.append({"entityId":product["entityId"], "productId":product["id"]})
                        if product["secondaryDownloads"] is not None and len(product["secondaryDownloads"]) > 0:
                            for secondaryDownload in product["secondaryDownloads"]:
                                if secondaryDownload["bulkAvailable"]:
                                    downloads.append({"entityId":secondaryDownload["entityId"], "productId":secondaryDownload["id"]})
            
            # Remove the list
            payload = {
                "listId": listId
            }
            sendRequest(serviceUrl + "scene-list-remove", payload, apiKey)                
            
            # Send download-request
            payLoad = {
                "downloads": downloads,
                "label": label,
                'returnAvailable': True
            }
            
            print(f"Sending download request ...\n")
            results = sendRequest(serviceUrl + "download-request", payLoad, apiKey)
            print(f"Done sending download request\n") 
        
              
            for result in results['availableDownloads']:       
                print(f"Get download url: {result['url']}\n" )
                runDownload(threads, result['url'])
        
            preparingDownloadCount = len(results['preparingDownloads'])
            preparingDownloadIds = []
            if preparingDownloadCount > 0:
                for result in results['preparingDownloads']:  
                    preparingDownloadIds.append(result['downloadId'])
          
                payload = {"label" : label}                
                # Retrieve download urls
                print("Retrieving download urls...\n")
                results = sendRequest(serviceUrl + "download-retrieve", payload, apiKey, False)
                if results != False:
                    for result in results['available']:
                        if result['downloadId'] in preparingDownloadIds:
                            preparingDownloadIds.remove(result['downloadId'])
                            print(f"Get download url: {result['url']}\n" )
                            runDownload(threads, result['url'])
                        
                    for result in results['requested']:   
                        if result['downloadId'] in preparingDownloadIds:
                            preparingDownloadIds.remove(result['downloadId'])
                            print(f"Get download url: {result['url']}\n" )
                            runDownload(threads, result['url'])
                
                # Don't get all download urls, retrieve again after 30 seconds
                while len(preparingDownloadIds) > 0: 
                    print(f"{len(preparingDownloadIds)} downloads are not available yet. Waiting for 30s to retrieve again\n")
                    time.sleep(30)
                    results = sendRequest(serviceUrl + "download-retrieve", payload, apiKey, False)
                    if results != False:
                        for result in results['available']:                            
                            if result['downloadId'] in preparingDownloadIds:
                                preparingDownloadIds.remove(result['downloadId'])
                                print(f"Get download url: {result['url']}\n" )
                                runDownload(threads, result['url'])
        
    print("\nGot download urls for all downloads\n")                
    # Logout
    endpoint = "logout"  
    if sendRequest(serviceUrl + endpoint, None, apiKey) == None:        
        print("Logged Out\n")
    else:
        print("Logout Failed\n")  
     
    print("Downloading files... Please do not close the program\n")
    for thread in threads:
        thread.join()
            
    print("Complete Downloading")
    
    executionTime = round((datetime.datetime.now() - startTime), 2)
    print(f'Total time: {executionTime} seconds')
    
    # if args.separate:
    #     if len(procdict['8']['scenelist']) > 0:
    #         i = 0
    #         outfile = os.path.join(outdir, 'ESPA_L8_list{}.txt'.format(todaystr))
    #         print('Writing output to: {}'.format(outfile))
    #         keylist = list(l8.keys())
    #         keylist.sort()
    #         with open(outfile, 'w') as output:
    #             for key in keylist:
    #                 for scene in l8[key]:
    #                     if key.startswith('LC8'): # Excludes Landsat 8 scenes that do not contain both OLI and TIRS data 
    #                         output.write('{}\n'.format(scenedata[scene]['LANDSAT_PRODUCT_ID_L2']))
    #                         i += 1
    #         print('{} scenes for ESPA to process.'.format(i))
        
    #     if len(l47.keys()) > 0:
    #         i = 0
    #         outfile = os.path.join(outdir,'ESPA_L47_list{}.txt'.format(todaystr))
    #         print('Writing output to: {}'.format(outfile))
    #         keylist = list(l47.keys())
    #         keylist.sort()
    #         with open(outfile, 'w') as output:
    #             for key in keylist:
    #                 for scene in l47[key]:
    #                     if key[2:3] != '8':
    #                         output.write('{}\n'.format(scenedata[scene]['LANDSAT_PRODUCT_ID_L2']))
    #                         i += 1
    #         print('{} scenes for ESPA to process.'.format(i))
    # else:
    #     i = 0
    #     outfile = os.path.join(outdir,'ESPA_list{}.txt'.format(todaystr))
    #     print('Writing output to: {}'.format(outfile))
    #     with open(outfile, 'w') as output:
    #         for d in [l47, l8]:
    #             if len(d.keys()) > 0:
    #                 keylist = list(d.keys())
    #                 keylist.sort()
    #                 for key in keylist:
    #                     for scene in d[key]:
    #                         output.write('{}\n'.format(scenedata[scene]['LANDSAT_PRODUCT_ID_L2']))
        #                     i += 1
        # print('{} scenes for ESPA to process.'.format(i))
                    
    #        if len(l7)>0:
    #            for scene in l7:
    #                output.write('%s\n'%scene)
    #        if len(l5)>0:
    #            for scene in l5:
    #                output.write('%s\n'%scene)
    
    print('Processing complete.')