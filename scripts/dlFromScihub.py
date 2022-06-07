# -*- coding: utf-8 -*-
"""
Created on Tue Mar 22 09:50:10 2022

@author: guyse
"""

import os, sys, requests, argparse, threading, re, datetime, json, glob, time, subprocess
import xml.etree.ElementTree as et
from requests.auth import HTTPBasicAuth
from osgeo import ogr#, osr
#import pandas as pd

try: # This is included as the module may not properly install in Anaconda.
    import ieo
except:
    ieodir = os.getenv('IEO_INSTALLDIR')
    if not ieodir:
        ieodir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        # print('Error: IEO failed to load. Please input the location of the directory containing the IEO installation files.')
        # ieodir = input('IEO installation path: ')
    if os.path.isfile(os.path.join(ieodir, 'ieo.py')):
        sys.path.append(ieodir)
        import ieo, S3ObjectStorage
    else:
        print('Error: that is not a valid path for the IEO module. Exiting.')
        sys.exit()

# These functions copied and modified from https://m2m.cr.usgs.gov/api/docs/example/download_landsat_c2-py

# Send http request

def getDownloadUrl(ProductID, user, password):
    print(f'Querying {ProductID} scene metadata from Copernicus Scihub.')
    querytext = f'identifier:{ProductID}'
    baseurl = f'https://scihub.copernicus.eu/dhus/search?&q=({querytext})'
    # print(baseurl)
    response = None
    maxtries = 10
    tries = 1
    while tries <= maxtries:
        response = requests.get(baseurl, auth = HTTPBasicAuth(user, password))
        # print(response.status_code)
        if response.status_code == 200:
            root = et.fromstring(response.text)
            for child in root: 
                if child.tag.endswith('totalResults'):
                    if child.text == '0':
                        print(f'ERROR: {ProductID} is missing from Copernicus SciHub, skipping.')
                        return None
                elif child.tag.endswith('entry'):
                    for item in child:
                        if 'href' in item.attrib.keys() and not 'rel' in item.attrib.keys():
                            url = item.attrib['href']    
                            print(f"Found download URL for scene {ProductID}: {url}")
                            return url
        else:
            time.sleep(5)
            tries += 1

    print('ERROR: No response from server for 10 attempts. Skipping scene.')
    return None

def requestScene(ProductID, url, user, password, queued, maxqueries, waitdict, *args, **kwargs):
    maxnoresponses = kwargs.get('maxnoresponses', 10)
    maxwaittime = kwargs.get('maxwaittime', 45) # minutes
    responded = False
    onlineurl = url.replace('/$value', '/Online/$value')
    noresponses = 0
    tries = 1
    while not responded:
        
        response = requests.get(onlineurl, auth = HTTPBasicAuth(user, password))
        now = datetime.datetime.now()
        if response:
            timestamp = datetime.datetime.now()
            timestampstr = timestamp.strftime('%Y-%m-%d %H:%M:%S')
            print(f'{ProductID} response code at {timestampstr}: {response.status_code}')
            if response.status_code <= 202:
                if response.text == 'true':
                    online = True
                    if not ProductID in queued.keys():
                        f'Found online scene: {ProductID}'
                        queued[ProductID] = {
                                'URI' : url,
                                'ordertime' : now,
                                'online' : online,
                                'onlineurl' : onlineurl,
                                'threaded' : False,
                                }
                    else:
                        print(f'Queued product is online: {ProductID}')
                        queued[ProductID]['online'] = online
                        # sema.release()
                    response.close()
                    responded = True
                elif len(queued.keys()) < maxqueries and not ProductID in queued.keys():
                    online = False
                    response.close()
                    print(f'Found offline scene: {ProductID}')
                    response = requests.get(url, auth = HTTPBasicAuth(user, password))
                    if response:
                        if response.status_code <= 202:
                            print(f'Added product to queue: {ProductID}')
                            now = datetime.datetime.now()
                            queued[ProductID] = {
                                'URI' : url,
                                'ordertime' : now,
                                'online' : online,
                                'onlineurl' : onlineurl,
                                'threaded' : False,
                                }
                        elif response.status_code == 403:
                            maxqueries = len(queued.keys())
                            print(f'Maximum queries reached: {maxqueries}')
                        response.close()
                        responded = True
                    else:
                        print('Error: no response from server.')
                        noresponses += 1
                elif ProductID in queued.keys():
                    responded = True
                        
        else:
            print('Error: no response from server.')
            noresponses += 1
        if noresponses >= maxnoresponses or tries >= maxnoresponses:
            responded = True
            if len(queued.keys()) > 0:
                print(f'Maximum number of queries reached: {len(queued.keys())}')
                maxqueries = len(queued.keys())
        if not responded:
            # if tries >= maxnoresponses:
                # print(f'Queries for scene {ProductID} have been unresponsive. Moving to end of the queue.')
                # waitdict[ProductID] = {
                #     'URI' : url,
                #     'ordertime' : now,
                #     'online' : False,
                #     'onlineurl' : onlineurl,
                #     'threaded' : False,
                #     }
                
                # break
            time.sleep(5)
            tries += 1
        if ProductID in queued.keys():
            if (datetime.datetime.now() - queued[ProductID]['ordertime']).seconds / 60 > maxwaittime:
                print(f'Scene {ProductID} is taking too long to download. Moving to end of the queue.')
                waitdict[ProductID] = queued[ProductID]
                # del queued[ProductID]
                # maxqueries -= 1
        
    return queued, maxqueries, waitdict
                
def runRequestScene(ProductID, url, user, password, queued, maxqueries, waitdict, *args, **kwargs):
    maxnoresponses = kwargs.get('maxnoresponses', 10)
    maxwaittime = kwargs.get('maxwaittime', 45) # minutes
    thread = threading.Thread(target = requestScene, args = (ProductID, url, user, password, queued, maxqueries, waitdict, ), kwargs = {'maxnoresponses' : maxnoresponses, 'maxwaittime' : maxwaittime,})
    threads.append(thread)
    thread.start()    

def downloadFile(url, user, password):
    sema.acquire()
    try:        
        response = requests.get(url, stream = True, auth = HTTPBasicAuth(user, password))
        disposition = response.headers['content-disposition']
        filename = re.findall("filename=(.+)", disposition)[0].strip("\"")
        print(f"Downloading {filename} ...\n")
        # if path != "" and path[-1] != "/":
        #     filename = "/" + filename
        f = os.path.join(ieo.Sen2ingestdir, filename)
        open(f, 'wb').write(response.content)
        print(f"Downloaded {filename}\n")
        sema.release()
    except Exception as e:
        ieo.logerror(url, f"Failed to download from {url}. Will try to re-download.")
        sema.release()
        runDownload(threads, url, user, password)
    
def runDownload(threads, url, user, password):
    thread = threading.Thread(target = downloadFile, args = (url, user, password,))
    threads.append(thread)
    thread.start()

parser = argparse.ArgumentParser('This script imports Sentinel-2 Scihub metadata into PostGIS.')
parser.add_argument('-p', '--password', default = None, type = str, help = 'Password to log into Copernicus Scihub server.')
parser.add_argument('-u', '--user', default = None, type = str, help = 'Username to log into Copernicus Scihub server.')
# parser.add_argument('-i', '--infile', default = None, type = str, help = 'File containing scenes to be downloaded.')
parser.add_argument('-o', '--outdir', default = ieo.Sen2ingestdir, type = str, help = 'Directory to download product files.')
parser.add_argument('--maxCC', default = 70.0, type = float, help = 'Maximum scene cloud cover to download, default = 70.0.')
parser.add_argument('--maxthreads', type = int, default = 5, help = 'Threads count for downloads, default = 5')
parser.add_argument('--maxqueries', type = int, default = 50, help = 'Maximum scene queries sent to server at any time, default = 50')
parser.add_argument('--startdate', type = str, default = None, help = 'Starting date, YYYY/MM/DD, default = None')
parser.add_argument('--enddate', type = str, default = '2021/12/31', help = 'Ending date, YYYY/MM/DD, default = 2021/12/31')
parser.add_argument('--intersectlayer', type = str, default = 'Ireland_Level_0', help = 'Layer to intersect with scene polygons.')
parser.add_argument('--intersectpoly', type = str, default = 'Ireland', help = 'Polygon to use in intersect layer.')
parser.add_argument('--intersectfieldname', type = str, default = 'NAME_TAG', help = 'Field name for selecting intersect layer polygon.')
parser.add_argument('--maxwaittime', type = int, default = 45, help = 'Maximum number of minutes to wait for scene to be available for download before moving to back of the queue, default = 45.')
parser.add_argument('--maxnoresponses', type = int, default = 10, help = 'Maximum number of no responses from server before pausing scene requests.')
# parser.add_argument('--maxdownloads', type = int, default = None, help = 'Maximum number of allowed for session downloads. If not set, will download all available scenes.')
parser.add_argument('--importscenes', action = 'store_true', help = 'If set, import scenes immediately after download.')
parser.add_argument('--scenelist', type = str, default = None, help = 'Path to text file containing list of scenes or comma-delimited list of scenes to get from SciHub. Using "all" will search ieo.Sen2ingestdir for all files starting with "missing_scenes".')
args = parser.parse_args()
Ireland_layer = args.intersectlayer
IE_poly_ID = args.intersectpoly
IE_poly_fieldname = args.intersectfieldname
maxqueries = args.maxqueries
downloaded = 0
queued = {}
endDate = datetime.datetime.strptime(args.enddate + ' 23:59:59', '%Y/%m/%d %H:%M:%S')
startTime = datetime.datetime.now()
maxCC = args.maxCC
# maxdownloads = args.maxdownloads
procstarttime = datetime.datetime.fromtimestamp(0)

scenelist = []

if args.startdate:
    startdate = datetime.datetime.strptime(args.startdate, '%Y/%m/%d')

if args.scenelist:
    if args.scenelist.lower() == 'all' or os.path.isfile(args.scenelist):
        if args.scenelist.lower() == 'all':
            flist = glob.glob(os.path.join(ieo.Sen2ingestdir, 'missing_scenes_*.txt'))
        elif os.path.isfile(args.scenelist):
            flist = [args.scenelist]
        if len(flist) > 0:
            for f in flist:
                print(f'Importing scene download list from: {f} ({flist.index (f) + 1}/{len(flist)})')
                with open(f, 'r') as lines:
                    for line in lines:
                        scene = line.strip()
                        addScene = True
                        if args.startdate:
                            parts = scene.split('_')
                            datetuple = datetime.datetime.strptime(parts[2][:8], '%Y%m%d')
                            if datetuple < startdate:
                                print(f'Scene {scene} was acquired before {args.startdate}, removing from download queue.')
                                addScene = False
                        if addScene and not os.path.isfile(os.path.join(ieo.Sen2ingestdir, f'{line.strip()}.zip')):
                            print(f'Adding scene to download list: {scene}')
                            scenelist.append(scene)
    else:
         scenelist = args.scenelist.split(',')
    
    if len(scenelist) > 0:
        print(f'{len(scenelist)} scenes have been identified for download.')
    else:
        print('WARNING: No scenes have been found to download. Exiting.')
        sys.exit()

sema = threading.Semaphore(value = args.maxthreads)
label = datetime.datetime.now().strftime("%Y%m%d_%H%M%S") # Customized label using date time
threads = []

ingestedList = []
year = 2017
buckets = ['ingested', 'scihub']
for bucket in buckets:
    print(f'Scanning {bucket} bucket for downloaded scenes.')
    while year <= 2022:
        months = S3ObjectStorage.getbucketfoldercontents(bucket, f'sentinel2/{year}', '/')
        if len(months) > 0:
            for month in months:
                days = S3ObjectStorage.getbucketfoldercontents(bucket, f'sentinel2/{year}/{month}/', '/')
                for day in days:
                    files = S3ObjectStorage.getbucketfoldercontents(bucket, f'sentinel2/{year}/{month}/{day}/', '')
                    for f in files:
                        if f.endswith('.zip'):
                            ProductID = os.path.basename(f)[:-4]
                            if not ProductID in ingestedList:
                                ingestedList.append(ProductID)
        year += 1
print(f'Scanning complete. A total of {len(ingestedList)} scenes have been found in the ingested and scihub buckets.')
# if os.path.isfile(args.infile):
#     print(f'Opening product list: {args.infile}')
#     df = pd.read_csv(args.infile)
#     df.sort_values(by = ['CloudCover', 'ProductID'])
# else:
#     print(f'ERROR: File not found, exiting: {args.infile}')
#     sys.exit()

procdict = {}
dldict = {}
maxdownloads = 0
user = args.user
password = args.password
notAllDownloaded = True

localfiles = glob.glob(os.path.join(ieo.Sen2ingestdir, 'S2*.zip'))
print(f'Found {len(localfiles)} in Sentinel-2 ingest directory.')

outdir = args.outdir

if not args.scenelist:
    conn = ogr.Open(ieo.catgpkg)
    ieo_conn = ogr.Open(ieo.ieogpkg)
    
    layer = conn.GetLayer(ieo.Sen2shp)
    IE_layer = ieo_conn.GetLayer(Ireland_layer)
    IE_layer.SetAttributeFilter(f'"{IE_poly_fieldname}" = \'{IE_poly_ID}\'')
    IE_feat = IE_layer.GetNextFeature()
    IE_geom = IE_feat.GetGeometryRef()
    
    MGRSlayer = ieo_conn.GetLayer(ieo.Sen2tiles)
    MGRSlist = []
    for feat in MGRSlayer:
        tilename = feat.GetField('TILE_ID')
        if not tilename in MGRSlist:
            MGRSlist.append(tilename)
    MGRSlayer = None
    
    ds = ogr.Open(ieo.catgpkg)
    layer = ds.GetLayer(ieo.Sen2shp)
    layerDefn = layer.GetLayerDefn()
    fieldlist = []
    
    layer.ResetReading()
    layer.StartTransaction()
    layer.SetAttributeFilter(f'("Surface_reflectance_tiles" IS NULL) AND ("Cloud_Coverage_Assessment" <= {maxCC}) AND ("PRODUCT_URI" LIKE \'https://scihub%\')')
    # layer.SetSpatialFilter(IE_geom)
    
    total_scenes = layer.GetFeatureCount()
    if total_scenes == 0:
        print('No scenes have been found to download. Exiting.')
        layer.RollbackTransaction()
        sys.exit()
    
    print(f'{total_scenes} scenes have been found to potentially download.')
    lastday = datetime.datetime.strptime('20211231', '%Y%m%d')
    
    feature = layer.GetNextFeature()
    while feature:
        ProductID = feature.GetField('ProductID')
        MGRS = feature.GetField('MGRS')
        p = None
        if (MGRS in MGRSlist) and (not os.path.isfile(os.path.join(ieo.Sen2ingestdir, f'{ProductID}.zip'))) and (not ProductID in ingestedList):
            url = feature.GetField('PRODUCT_URI')
            geom = feature.GetGeometryRef()
            mgeom = ogr.Geometry(ogr.wkbMultiPolygon)
            mgeom.AddGeometry(geom)
            p = mgeom.Intersection(IE_geom)
            if p:
            # print(f'Found feature and URI for scene {ProductID}: {url}')
                parts = ProductID.split('_')
                year, month, day = parts[2][:4], parts[2][4:6], parts[2][6:8]
                acqDate = feature.GetField('acquisitionDate')
                # year, month, day = acqDate[:4], acqDate[5:7], acqDate[8:10]
                if datetime.datetime.strptime(acqDate[:10], '%Y/%m/%d') <= lastday:
                    if not year in dldict.keys():
                        dldict[year] = {}
                    if not month in dldict[year].keys():
                        dldict[year][month] = {}
                    if not day in dldict[year][month].keys():
                        dldict[year][month][day] = {}
                    if (not ProductID in dldict[year][month][day].keys()) and (not os.path.isfile(os.path.join(ieo.Sen2ingestdir, f'{ProductID}.zip'))):
                        print(f'Adding scene to download list: {ProductID}')
                        dldict[year][month][day][ProductID] = url
                        maxdownloads += 1
            else:
                print(f'Scene {ProductID} does not intersect with Ireland, skipping.')
        else:
            print(f'Scene {ProductID} has already been downloaded or is not in the list of Irish MGRS tiles, skipping.')
        # if ProductID in queued.keys() or os.path.isfile(os.path.join(ieo.Sen2ingestdir, f'{ProductID}.zip')) or not p or ProductID in ingestedList:
        feature = layer.GetNextFeature()
    layer.CommitTransaction()
else:
    for ProductID in scenelist:
        parts = ProductID.split('_')
        year, month, day = parts[2][:4], parts[2][4:6], parts[2][6:8]
        datestr = f'{year}{month}{day}'
        url = getDownloadUrl(ProductID, user, password)
        if url:
            if not year in dldict.keys():
                dldict[year] = {}
            if not month in dldict[year].keys():
                dldict[year][month] = {}
            if not day in dldict[year][month].keys():
                dldict[year][month][day] = {}
            dldict[year][month][day][ProductID] = url
            maxdownloads += 1
        
print(f'Total files to download: {maxdownloads}')

waitdict = {}

if maxdownloads > 0:
    y = min(dldict.keys())
    m = min(dldict[y].keys())
    d = min(dldict[y][m].keys())
    datetuple = datetime.datetime.strptime(f'{y}{m}{d}', '%Y%m%d')
    yy = max(dldict.keys())
    mm = max(dldict[yy].keys())
    dd = max(dldict[yy][mm].keys())
    enddate = datetime.datetime.strptime(f'{yy}{mm}{dd}', '%Y%m%d')
    Prodlist = []
    urllist = []
    for year in sorted(dldict.keys()):
        for month in sorted(dldict[year].keys()):
            for day in sorted(dldict[year][month].keys()):
                for ProductID in sorted(dldict[year][month][day].keys()):
                    if not ProductID in Prodlist and not os.path.isfile(os.path.join(ieo.Sen2ingestdir, f'{ProductID}.zip')):
                        Prodlist.append(ProductID)
                        urllist.append(dldict[year][month][day][ProductID])
    print(f'Downloading data between {datetuple.strftime("%Y-%m-%d")} and {enddate.strftime("%Y-%m-%d")}.')
    i = 0
    while downloaded < maxdownloads:
        maxqueries = args.maxqueries
        print(f'{downloaded} downloaded and processed scenes out of {maxdownloads}, with {len(waitdict.keys())} in final wait queue.')
        queued = {}
        threads = []
        while len(queued.keys()) < maxqueries:
            if i < len(Prodlist):
                queued, maxqueries, waitdict = requestScene(Prodlist[i], urllist[i], user, password, queued, maxqueries, waitdict, maxnoresponses = args.maxnoresponses, maxwaittime = args.maxwaittime)
                if Prodlist[i] in queued.keys() or Prodlist[i] in waitdict.keys():
                    i += 1
            elif len(waitdict.keys()) > 0:
                
                print(f'Attempting request and download of {len(waitdict.keys())} problematic scenes.')
                for ProductID in sorted(waitdict.keys()):
                    j = Prodlist.index(ProductID)
                    # del waitdict[ProductID]
                    queued, maxqueries, waitdict = requestScene(Prodlist[j], urllist[j], user, password, queued, maxqueries, waitdict, maxnoresponses = args.maxnoresponses, maxwaittime = args.maxwaittime)
                        
            
        print(f'Total scenes in current download and processing queue: {len(queued.keys())}')    
        if len(queued.keys()) > 0:
            numoffline = len(queued.keys())
            waitdicted = []
            while (numoffline - len(waitdicted)) > 0:
                time.sleep(30)
                for ProductID in queued.keys():
                    if not queued[ProductID]['online'] and not ProductID in waitdict.keys():
                        queued, maxqueries, waitdict = requestScene(ProductID, queued[ProductID]['URI'], user, password, queued, maxqueries, waitdict, maxnoresponses = args.maxnoresponses, maxwaittime = args.maxwaittime)
                    if not ProductID in waitdict.keys():
                        if queued[ProductID]['online'] and not queued[ProductID]['threaded']:
                            print(f'Scene {ProductID} is now available for download.')
                            runDownload(threads, queued[ProductID]['URI'], user, password)
                            # queued[ProductID]['threaded'] = True
                            
                            # del queued[ProductID]
                            numoffline -= 1
                            # request next scene in lists
                            # queued, maxqueries, waitdict = requestScene(Prodlist[i], urllist[i], user, password, queued, maxqueries, waitdict, maxnoresponses = args.maxnoresponses, maxwaittime = args.maxwaittime)
                            # if Prodlist[i] in queued.keys():
                            #     i += 1
                    elif ProductID in waitdict.keys() and not ProductID in waitdicted:
                        waitdicted.append(ProductID)
            for thread in threads:
                thread.join()
            if args.importscenes:
                importnum = 0
                sceneliststr = ''
                
                dlflist = glob.glob(os.path.join(ieo.Sen2ingestdir, '*.zip'))
                if len(dlflist) > 0:
                    procdict = {}
                    for f in dlflist:
                        mtime = datetime.datetime.fromtimestamp(os.path.getmtime(f))
                        ProductID = os.path.basename(f)[:60]
                        parts = ProductID.split('_')
                        year, month, day = parts[2][:4], parts[2][4:6], parts[2][6:8]
                        datestr = f'{year}{month}{day}'
                        if not year in dldict.keys():
                            dldict[year] = {}
                        if not month in dldict[year].keys():
                            dldict[year][month] = {}
                        if not day in dldict[year][month].keys():
                            dldict[year][month][day] = {}
                        if not datestr in procdict.keys():
                            procdict[datestr] = {
                                'ready' : False,
                                'Products' : [],
                                'queued' : len(dldict[year][month][day].keys()),
                                'waitdict' : 0,
                                'maxmtime' : datetime.datetime.fromtimestamp(0),
                                }
                        procdict[datestr]['Products'].append(ProductID)
                        if procdict[datestr]['maxmtime'] < mtime:
                            procdict[datestr]['maxmtime'] = mtime
                                
                    for datestr in procdict.keys():
                        if len(waitdicted) > 0:
                            if any(datestr in x for x in waitdicted):
                                for x in waitdicted:
                                    parts = x.split('_')
                                    if parts[2].startswith(datestr):
                                        procdict[datestr]['waitdict'] += 1
                        if (procdict[datestr]['queued'] + procdict[datestr]['waitdict']) == len(dldict[year][month][day].keys()) and procdict[datestr]['maxmtime'] > procstarttime:
                            print(f'Ready to ingest {procdict[datestr]["queued"]} scenes from {datestr[:4]}-{datestr[4:6]}-{datestr[6:]} with {procdict[datestr]["waitdict"]} in final download queue.')
                            for scene in procdict[datestr]['Products']:
                                print(f'Adding scene: {scene}')
                                if sceneliststr == '':
                                    sceneliststr = scene
                                else:
                                    sceneliststr += f',{scene}'
                                importnum += 1
                        
                        
                print(f'Importing {importnum} downloaded scenes.')
                procstarttime = datetime.datetime.now()
                p = subprocess.Popen(['python', 'importSentinel2.py', '--S2TM', '--noNDVI', '--noEVI', '--noNDTI', '--noNBR', '--removelocal', '--verbose' ,'--localingest', '--maxCC', args.maxCC, '--scenelist', sceneliststr])
                # print(p.communicate())
                # print(f'{len(queued.keys())} scenes imported.')
            downloaded += len(queued.keys())
        else:
            print('No scenes left to process, finishing up.')
            break
        
    

# while startval < maxresults:
#     outfile = os.path.join(outdir, f'scihub_query_{producttype}_{startval}.xml')
#     if not os.path.isfile(outfile):
#         print(f'Querying records: {startval} to {startval + rows} out of {maxresults}.')
#         baseurl = f'https://scihub.copernicus.eu/dhus/search?start={startval}&rows={rows}&q={querytext}&orderby=beginposition asc'
        # print(baseurl)
        # outfile = os.path.join(outdir, f'scihub_query_{producttype}_{startval}.xml')
# baseurl = "https://scihub.copernicus.eu/dhus/odata/v1/Products('f147abbc-83f4-4203-a8b4-e01a23bb188f')/Online/$value"
# response = requests.get(baseurl, auth=HTTPBasicAuth(user, password))
# print(f'{response.status_code}: {response.text}')
#         # with open(outfile, 'w') as output:
#         #     output.write(response.text)
#     # startval += rowsprint("Downloading files... Please do not close the program\n")
# for thread in threads:
#     thread.join()

# print("Complete Downloading")
    
executionTime = round((datetime.datetime.now() - startTime).seconds, 2)
print(f'Total time: {executionTime} seconds')
print('Processing complete.')