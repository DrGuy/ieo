# -*- coding: utf-8 -*-
"""
Created on Tue Mar 22 09:50:10 2022

@author: guyse
"""

import os, sys, requests, argparse, threading, re, datetime, json, glob, time, subprocess
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
        import ieo #, S3ObjectStorage
    else:
        print('Error: that is not a valid path for the IEO module. Exiting.')
        sys.exit()

# These functions copied and modified from https://m2m.cr.usgs.gov/api/docs/example/download_landsat_c2-py

# Send http request

def requestScene(ProductID, url, user, password, queued, maxqueries):
    responded = False
    onlineurl = url.replace('/$value', '/Online/$value')
    while not responded:
        # time.sleep(5)
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
        else:
            print('Error: no response from server.')
    return queued, maxqueries
                
    

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
parser.add_argument('--enddate', type = str, default = '2021/12/31', help = 'Ending date, YYYY/MM/DD, default = 2021/12/31')
parser.add_argument('--intersectlayer', type = str, default = 'Ireland_Level_0', help = 'Layer to intersect with scene polygons.')
parser.add_argument('--intersectpoly', type = str, default = 'Ireland', help = 'Polygon to use in intersect layer.')
parser.add_argument('--intersectfieldname', type = str, default = 'NAME_TAG', help = 'Field name for selecting intersect layer polygon.')
parser.add_argument('--maxdownloads', type = int, default = None, help = 'Maximum number of allowed for session downloads. If not set, will download all available scenes.')
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
maxdownloads = args.maxdownloads

sema = threading.Semaphore(value = args.maxthreads)
label = datetime.datetime.now().strftime("%Y%m%d_%H%M%S") # Customized label using date time
threads = []

# if os.path.isfile(args.infile):
#     print(f'Opening product list: {args.infile}')
#     df = pd.read_csv(args.infile)
#     df.sort_values(by = ['CloudCover', 'ProductID'])
# else:
#     print(f'ERROR: File not found, exiting: {args.infile}')
#     sys.exit()

conn = ogr.Open(ieo.catgpkg)
ieo_conn = ogr.Open(ieo.ieogpkg)

layer = conn.GetLayer(ieo.Sen2shp)
IE_layer = ieo_conn.GetLayer(Ireland_layer)
IE_layer.SetAttributeFilter(f'"{IE_poly_fieldname}" = \'{IE_poly_ID}\'')
IE_feat = IE_layer.GetNextFeature()
IE_geom = IE_feat.GetGeometryRef()

notAllDownloaded = True

user = args.user
password = args.password

localfiles = glob.glob(os.path.join(ieo.Sen2ingestdir, 'S2*.zip'))
print(f'Found {len(localfiles)} in Sentinel-2 ingest directory.')

outdir = args.outdir

ds = ogr.Open(ieo.catgpkg)
layer = ds.GetLayer(ieo.Sen2shp)
layerDefn = layer.GetLayerDefn()
fieldlist = []

dldict = {}
layer.StartTransaction()
layer.SetAttributeFilter(f'("Surface_reflectance_tiles" IS NULL) AND ("Cloud_Coverage_Assessment" <= {maxCC}) AND ("PRODUCT_URI" LIKE \'https%\')')
layer.SetSpatialFilter(IE_geom)

total_scenes = layer.GetFeatureCount()
if total_scenes == 0:
    print('No scenes have been found to download. Exiting.')
    sys.exit()

print(f'{total_scenes} scenes have been found to download.')

if not maxdownloads:
    maxdownloads = total_scenes

feature = layer.GetNextFeature()
while downloaded < maxdownloads:
    print(f'{downloaded} downloaded and processed scenes out of {maxdownloads}.')
    queued = {}
    threads = []
    while len(queued.keys()) < maxqueries and feature:
        ProductID = feature.GetField('ProductID')
        url = feature.GetField('PRODUCT_URI')
        # print(f'Found feature and URI for scene {ProductID}: {url}')
        queued, maxqueries = requestScene(ProductID, url, user, password, queued, maxqueries)
        if ProductID in queued.keys():
            feature = layer.GetNextFeature()
        else:
            break
        
    if len(queued.keys()) > 0:
        numoffline = len(queued.keys())
        while numoffline > 0:
            time.sleep(30)
            for ProductID in queued.keys():
                if not queued[ProductID]['online']:
                     queued, maxqueries = requestScene(ProductID, queued[ProductID]['URI'], user, password, queued, maxqueries)
                if queued[ProductID]['online'] and not queued[ProductID]['threaded']:
                    print(f'Scene {ProductID} is now available for download.')
                    runDownload(threads, queued[ProductID]['URI'], user, password)
                    queued[ProductID]['threaded'] = True
                    numoffline -= 1
        for thread in threads:
            thread.join()
        print(f'Importing {len(queued.keys())} downloaded scenes.')
        p = subprocess.Popen(['python', 'importSentinel2.py', '--S2TM', '--noNDVI', '--noEVI', '--noNDTI', '--noNBR', '--removelocal', '--verbose' ,'--localingest'])
        print(p.communicate())
        print(f'{len(queued.keys())} scenes imported.')
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
    
executionTime = round((datetime.datetime.now() - startTime), 2)
print(f'Total time: {executionTime} seconds')
print('Processing complete.')