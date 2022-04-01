#!/usr/bin/env python
# By Guy Serbin, EOanalytics Ltd.
# Talent Garden Dublin, Claremont Ave. Glasnevin, Dublin 11, Ireland
# email: guyserbin <at> eoanalytics <dot> ie

# version 1.5

# This script does the following:
# 1. Downloads Sentinel2 L2A products from Amazon S3-compliant buckeys
# 2. Virtually stacks surface reflectance (SR) bands. 
# 3. Converts SR band data from UTM to the local projection.
# 4. Calculates NDVI and EVI values.
# 5. Saves tiles to S3 bucket

import os, sys, glob, datetime, argparse, shutil#, ieo, pickle
from osgeo import ogr

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

## main
parser = argparse.ArgumentParser('This script imports Sentinel-2 Level 2A-processed scenes into the local library. It stacks images and converts them to the locally defined projection in IEO, and adds ENVI metadata.')
parser.add_argument('-i', '--indir', default = ieo.Sen2ingestdir, type = str, help = 'Input directory to search for files. This will be overridden if --infile is set.')
parser.add_argument('-if', '--infile', type = str, help = 'Input file. This must be contain the full path and filename.')
# parser.add_argument('-f', '--fmaskdir', type = str, default = ieo.fmaskdir, help = 'Directory containing FMask cloud masks in local projection.')
# parser.add_argument('-q', '--pixelqadir', type = str, default = ieo.pixelqadir, help = 'Directory containing Landsat pixel QA layers in local projection.')
# parser.add_argument('--radsatqadir', type = str, default = ieo.radsatqadir, help = 'Directory containing Landsat radiometric saturation QA layers in local projection.')
# parser.add_argument('--aerosolqadir', type = str, default = ieo.aerosolqadir, help = 'Directory containing Landsat aerosol QA layers in local projection.')
parser.add_argument('-o', '--outdir', type = str, default = ieo.Sen2srdir, help = 'Surface reflectance output directory')
# parser.add_argument('-b', '--stoutdir', type = str, default = ieo.stdir, help = 'Surface temperature output directory')
parser.add_argument('-n', '--ndvidir', type = str, default = ieo.Sen2ndvidir, help = 'NDVI output directory')
parser.add_argument('-e', '--evidir', type = str, default = ieo.Sen2evidir, help = 'EVI output directory')
# parser.add_argument('-a', '--archdir', type = str, default = ieo.archdir, help = 'Original data archive directory')
parser.add_argument('--overwrite', type = bool, default = False, help = 'Overwrite existing files.')
parser.add_argument('-d', '--delay', type = int, default = 0, help = 'Delay execution of script in seconds.')
parser.add_argument('--maxCC', type = float, default = 99.0, help = 'Maximum allowable cloud cover in imported data. Default = 99.0.')
parser.add_argument('-r', '--remove', type = bool, default = True, help = 'Remove temporary files after ingest.')
parser.add_argument('--CalcVIs', type = bool, default = True, help = 'Calculate vegetation indices. Default = True.')
parser.add_argument('--removelocal', action = 'store_true', help = 'Remove local files after ingest.')
parser.add_argument('--S2TM', action = 'store_true', help = 'Process only equivalent Landsat 4-5/ Landsat 7 ETM+ bands.')
parser.add_argument('--S2OLI', action = 'store_true', help = 'Process only equivalent Landsat 8-9 OLI bands (overrides --S2TM).')
parser.add_argument('--noNDVI', action = 'store_true', help = 'Do not calculate NDVI.')
parser.add_argument('--noEVI', action = 'store_true', help = 'Do not calculate EVI.')
parser.add_argument('--noNDTI', action = 'store_true', help = 'Do not calculate NDTI.')
parser.add_argument('--noNBR', action = 'store_true', help = 'Do not calculate NBR.')
parser.add_argument('--reprocess', action = 'store_true', help = 'Reprocess all scenes for selected date period.')
parser.add_argument('--localingest', action = 'store_true', help = 'Ingest any zip files in default IEO Sentinel2 ingest directory.')
parser.add_argument('--copylater', action = 'store_true', help = 'Do not copy local files to sentinel2 bucket during script execution.')
parser.add_argument('--MGRS', type = str, default = None, help = 'Comma-delimited list of MGRS tiles to process, without any spaces. Default = 29UPU for now.')#'If missing, all default tiles will be processed for the date range.')
parser.add_argument('--startdate', type = str, default = '2015-06-23', help = 'Start date for processing in YYYY-mm-dd format. Default is 2015-06-23.')
parser.add_argument('--enddate', type = str, default = None, help = "End date for processing in YYYY-mm-dd format. If missing, today's date will be used.")
parser.add_argument('--bucket', type = str, default = None, help = 'Import data from a specific bucket.')#'If missing, all default tiles will be processed for the date range.')
parser.add_argument('--verbose', action = 'store_true', help = 'Display more messages during execution.')
args = parser.parse_args()

verbose = args.verbose

if args.S2OLI:
    outdatasettype = 'S2OLI'
elif args.S2TM:
    outdatasettype = 'S2TM'
else:
    outdatasettype = 'Sentinel-2'

if args.noNDVI:
    CalcNDVI = False
else:
    CalcNDVI = True

if args.noEVI:
    CalcEVI = False
else:
    CalcEVI = True

if args.noNDTI:
    CalcNDTI = False
else:
    CalcNDTI = True

if args.noNBR:
    CalcNBR = False
else:
    CalcNBR = True

if args.bucket and args.localingest:
    args.localingest = False

LocalProdList = []
if args.localingest:
    locallist = glob.glob(os.path.join(ieo.Sen2ingestdir, '*.zip'))
    
    if len(locallist) > 0:
        for p in locallist:
            if verbose: print(f'Found local scene: {p}')
            LocalProdList.append(os.path.basename(p)[:60])

def movefile(f, inbucket, outbucket, outf, *args, **kwargs):
    i = kwargs.get('i', None)
    n = kwargs.get('n', None)
    if i:
        print(f'Now transfering from bucket {inbucket} to bucket {outbucket}: {f} ({i}/{n})')
    else: 
        print(f'Now transfering from bucket {inbucket} to bucket {outbucket}: {f}')
    copy_source = {
                    'Bucket': inbucket,
                    'Key': f
                }
    S3ObjectStorage.s3res.meta.client.copy(copy_source, outbucket, outf)
    S3ObjectStorage.s3res.Object(inbucket, f).delete()

def joinfeatures(ProductIDs, layer):
    multi = ogr.Geometry(ogr.wkbMultiPolygon)
    for feature in layer: 
        if feature.GetField('ProductID') in ProductIDs:
            if feature.geometry():
                if verbose: print(f'Adding geometry for {ProductID}.')
                feature.geometry().CloseRings() # this copies the first point to the end
                wkt = feature.geometry().ExportToWkt()
                multi.AddGeometryDirectly(ogr.CreateGeometryFromWkt(wkt))
    union = multi.UnionCascaded()
    return union

def gettiles(feature, NTSlayer):
    acqdatestr = feature.GetField('acquisitionDate')
    datestr = acqdatestr[:10].replace('/', '')
    NTSlayer.StartTransaction()
    for tile in NTSlayer:
        tilegeometry = tile.GetGeometryRef()
        geom = feature.GetGeometryRef()
        if geom.Intersect(tilegeometry):
            tilename = tile.GetField('Tile')
            
            for d in transferdict.keys():
                prefix = f'{d}/{tilename}/{datestr[:4]}/{datestr[4:6]}/{datestr[6:8]}'
                objs = S3ObjectStorage.getbucketobjects('sentinel2', prefix)
                if isinstance(objs, dict):
                    if len(objs[prefix]) > 0:
                        if verbose: print(f'Found {len(objs[prefix])} on sentinel2 bucket to transfer for tile {tilename}.')
                        for f in objs[prefix]:
                            if not os.path.isfile(os.path.join(transferdict[d], f)):
                                s3_object = f'{prefix}/{f}'
                                S3ObjectStorage.downloadfile(transferdict[d], 'sentinel2', s3_object)
    NTSlayer.CommitTransaction()

    
# picklefile = os.path.join(ieo.catdir, 'sentinel2.pickle') # contains information on data saved in buckets.
# if os.path.isfile(picklefile):
#     print('Loading data from Sentinel 2 pickle file.')
#     with open(picklefile, 'rb') as handle:
#         scenedict = pickle.load(handle)
# else:
scenedict = {}

if args.delay > 0: # if we want to delay execution for whatever reason
    from time import sleep
    print('Delaying execution {} seconds.'.format(args.delay))
    sleep(args.delay)


# Setting a few variables
# archdir = args.archdir
# fmaskdir = args.fmaskdir
# fmasklist = glob.glob(os.path.join(args.fmaskdir, '*.dat'))

reflist = []
dcenedict = {}
filelist = []
ProductDict = {}
Prodlist = []
movedict = {}
# bucketdllist = []
# Create list of MGRS tiles to process

if not args.MGRS:
    MGRStilelist = ieo.Sen2tilelist
else:
    MGRStilelist = args.MGRS.split(',')

# Time variables: unlike other IEO code and scripts, these are objects will be datetime objects
now = datetime.datetime.now()
if not args.enddate: 
    enddate = now
else:
    enddate = datetime.datetime.strptime(args.enddate, '%Y-%m-%d')
enddatestr = enddate.strftime('%Y-%m-%d')
startdate = datetime.datetime.strptime(args.startdate, '%Y-%m-%d')

# In case there are any errors during script execution
errorfile = 'newSentinel2import_errors_{}.csv'.format(now.strftime('%Y%m%d_%H%M%S'))
badscenefile = os.path.join(ieo.logdir, 'Sentinel2badscenes.log')
#ieo.errorfile = errorfile

# def sceneidfromfilename(filename):
#     basename = os.path.basename(filename)
#     i = basename.find('-')
#     if i > 21:
#         nsceneid = basename[:i]
#         sceneid = nsceneid[:2] + nsceneid[3:10]
#         datetuple = datetime.datetime.strptime(nsceneid[10:18], '%Y%m%d')
#         sceneid += datetuple.strftime('%Y%j')
#     elif i == 21:
#         sceneid = basename[:16]
#     else:
#         sceneid = None
#     return sceneid
if args.bucket:
    # args.localingest = True
    print(f'Searching bucket {args.bucket} for files.')
    filelist = S3ObjectStorage.getbucketfoldercontents(args.bucket, '', '')
    if len(filelist) == 0:
        print('ERROR: No files found in bucket. Exiting.')
        sys.exit()
    else:
        print(f'Found {len(filelist)} files.')

# Open up ieo.Sen2shp and get the existing Product ID, Scene ID, and SR_path status
print('Opening local Sentinel 2 catalog file.\n')
if not ieo.usePostGIS:
    driver = ogr.GetDriverByName("GPKG")
    data_source = driver.Open(ieo.catgpkg, 1)
else:
    data_source = ogr.Open(ieo.catgpkg, 1)
layer = data_source.GetLayer(ieo.Sen2shp)

for feature in layer:
    # sceneID = feature.GetField('sceneID')
    ProductID = feature.GetField('ProductID')
    MGRS = feature.GetField('MGRS')
    if not MGRS:
        MGRS = feature.GetField('sceneID')[4:9]
        if verbose: print(f'Setting missing MGRS tile {MGRS} for ProductID {ProductID}.')
        feature.SetField('MGRS', MGRS)
        layer.SetFeature(feature)
    
    if ieo.usePostGIS:
        datetimestr = '%Y/%m/%d %H:%M:%S+00'
    else:
        datetimestr = '%Y/%m/%d %H:%M:%S'
    acqdatestr = feature.GetField('acquisitionDate')
    if not acqdatestr:
        productstarttimestr = feature.GetField('PRODUCT_START_TIME')
        if productstarttimestr:
            acqdatestr = productstarttimestr
            feature.SetField('acquisitionDate', productstarttimestr)
            layer.SetFeature(feature)
    if '.' in acqdatestr:
        datetimestr = '%Y/%m/%d %H:%M:%S.%f+00'
    acqDate = datetime.datetime.strptime(acqdatestr, datetimestr)
    if args.reprocess:
        ingestTime = None
    else:
        ingestTime = feature.GetField('Raster_Ingest_Time')
        SR_tiles = feature.GetField('Surface_reflectance_tiles')
        if not SR_tiles:
            ingestTime = None
    
    if verbose:
        print(f'\rAnalyzing feature: {ProductID}')
    # ProductDict[ProductID] = sceneID
    if not args.bucket:
        if (MGRS in MGRStilelist) and \
            (feature.GetField('Cloud_Coverage_Assessment') <= args.maxCC) and \
            (acqDate >= startdate) and (acqDate <= enddate) and (not ingestTime) \
            and ((ProductID in LocalProdList) or (not args.localingest)):
            ProductDict[ProductID] = {
                'ProductID' : ProductID, 
                'acqDate' : acqDate,
                'SR_path' : feature.GetField('Surface_Reflectance_tiles'), 
                'ingest_bucket' : feature.GetField('S3_ingest_bucket'), 
                'tile_basename' : feature.GetField('Tile_filename_base'), 
                'ingest_path' : feature.GetField('S3_endpoint_path'), 
                'processed_bucket' : feature.GetField('S3_tile_bucket'), 
                'Metadata_ingest_time' : feature.GetField('Metadata_Ingest_Time'), 
                'Raster_ingest_time' : feature.GetField('Raster_Ingest_Time'), 
                'cloudCover' : feature.GetField('Cloud_Coverage_Assessment'),
                                            }
            if not ProductDict[ProductID]['ingest_bucket']:
                ProductDict[ProductID]['ingest_bucket'] = 'Direct_download_from_SciHub'
    else:
        if any(ProductID in x for x in filelist) and (MGRS in MGRStilelist) and \
            (feature.GetField('Cloud_Coverage_Assessment') <= args.maxCC) and \
            (acqDate >= startdate) and (acqDate <= enddate) and (not ingestTime) \
            and ((ProductID in LocalProdList) or (not args.localingest)):
            ProductDict[ProductID] = {
                'ProductID' : ProductID, 
                'acqDate' : acqDate,
                'SR_path' : feature.GetField('Surface_Reflectance_tiles'), 
                'ingest_bucket' : args.bucket, 
                'tile_basename' : feature.GetField('Tile_filename_base'), 
                'ingest_path' : feature.GetField('S3_endpoint_path'), 
                'processed_bucket' : feature.GetField('S3_tile_bucket'), 
                'Metadata_ingest_time' : feature.GetField('Metadata_Ingest_Time'), 
                'Raster_ingest_time' : feature.GetField('Raster_Ingest_Time'), 
                'cloudCover' : feature.GetField('Cloud_Coverage_Assessment'),
                                            }
            # bucketdllist.append(ProductID)
            LocalProdList.append(ProductID)
        elif (any(ProductID in x for x in filelist) or os.path.isfile(os.path.join(ieo.Sen2ingestdir, f'{ProductID}.zip'))) and ingestTime:
            movedict[f'{ProductID}.zip'] = f'sentinel2/{acqDate.year}/{acqDate.month:02d}/{acqDate.day:02d}/{ProductID}.zip'

if args.bucket:
    if len(movedict.keys()) > 0:
        print(f'Found {len(movedict.keys())} scenes that have already been processed in bucket {args.bucket}, moving to bucket ingested.')
        i = 1
        n = len(movedict.keys())
        for x in list(movedict.keys()):
            movefile(x, args.bucket, 'ingested', movedict[x], i = i, n = n)
            i += 1
elif args.localingest:
    if len(movedict.keys()) > 0:
        print(f'Found {len(movedict.keys())} scenes that have already been processed in bucket {args.bucket}, moving to bucket ingested.')
        for x in list(movedict.keys()):
            f = os.path.join(ieo.Sen2ingestdir, x)
            try:
                S3ObjectStorage.copyfilestobucket(bucket = 'ingested', targetdir = movedict[x], filename = f)
                if args.removelocal:
                    os.remove(f)
                    dname = os.path.join(ieo.Sen2ingestdir, x[:-4])
                    for d in [dname, dname + '_ITM']:
                        if os.path.isdir(d):
                            print(f'Deleting path: {d}')
                            shutil.rmtree(d)
            except Exception as e:
                print(f'ERROR with file transfer for {ProductID}: ', e)
                ieo.logerror(ProductID, e)
            
if args.bucket:
    if len(LocalProdList) > 0:
        args.localingest = True
        print(f'{len(LocalProdList)} scenes identified for download from bucket {args.bucket}.')
        for ProductID in LocalProdList:
            try:
                S3ObjectStorage.downloadfile(ieo.Sen2ingestdir, args.bucket, f'{ProductID}.zip')
            except Exception as e:
                print(f'ERROR: {e}')
                LocalProdList.pop(LocalProdList.index(ProductID))
        print('Files downloaded.')
    else:
        print('No files found to process. Exiting.')
        sys.exit()
                
data_source = None

print(f'\nCreating processing lists for dates between {args.startdate} and {enddatestr}.\n')
for ProductID in sorted(ProductDict.keys()):
    print(f'\rAdding feature to daily processing list: {ProductID}')
    bucket = ProductDict[ProductID]['ingest_bucket']
    if args.localingest:
        if ProductID in LocalProdList:
            prefix = os.path.join(ieo.Sen2ingestdir, f'{ProductID}.zip')
    else: 
        prefix = ProductDict[ProductID]['ingest_path']
    acqDate = ProductDict[ProductID]['acqDate']
    year, month, day = acqDate.strftime('%Y-%m-%d').split('-')
    if not bucket in scenedict.keys():
        scenedict[bucket] = {}
    if not year in scenedict[bucket].keys():
        scenedict[bucket][year] = {}
    if not month in scenedict[bucket][year].keys():
        scenedict[bucket][year][month] = {}
    if not day in scenedict[bucket][year][month].keys():
        scenedict[bucket][year][month][day] = {}
        # scenedict[bucket][year][month][day]['granules'] = []
        scenedict[bucket][year][month][day]['ProductIDs'] = {}
        # scenedict[bucket][year][month][day]['tiles'] = []
        # scenedict[bucket][year][month][day]['ProductIDs']['MGRS'] = {}
        # scenedict[bucket][year][month][day]['ProductIDs']['NTS'] = []
    # scenedict[bucket][year][month][day]['granules'].append(prefix)
    scenedict[bucket][year][month][day]['ProductIDs'][ProductID] = prefix
    # scenedict[bucket][year][month][day]['ProductIDs']['MGRS'][ProductID]
        
            
    
transferdict = {'SR' : ieo.Sen2srdir}  
if not args.noNDVI: transferdict['NDVI'] = ieo.Sen2ndvidir
if not args.noEVI: transferdict['EVI'] = ieo.Sen2evidir
if not args.noNDTI: transferdict['NDTI'] = ieo.Sen2ndtidir
if not args.noNBR: transferdict['NBR'] = ieo.Sen2nbrdir
          

# This look finds any existing processed data 
# for dir in [args.outdir, os.path.join(args.outdir, 'L1G')]:
#     rlist = glob.glob(os.path.join(args.outdir, '*_ref_{}.dat'.format(ieo.projacronym)))
#     for f in rlist:
#         if not 'ESA' == os.path.basename(f)[16:19]:
#             reflist.append(f)

for d in [ieo.Sen2srdir, ieo.Sen2ndvidir, ieo.Sen2evidir, ieo.Sen2ndtidir, ieo.Sen2nbrdir, ieo.Sen2ingestdir]:
    if not os.path.isdir(d):
        print(f'Now creating missing directory: {d}')
        os.makedirs(d)

# Now create the processing list

# scenedict = S3ObjectStorage.getSentinel2scenedict(MGRStilelist, startdate = startdate, enddate = enddate, verbose = args.verbose, scenedict = scenedict)

# if args.infile: # This is in case a specific file has been selected for processing
#     if os.access(args.infile, os.F_OK) and (args.infile.endswith('.tar.gz') or args.infile.endswith('.tar')):
#         print('File has been found, processing.')
#         filelist.append(args.infile)
#     else:
#         print('Error, file not found: {}'.format(args.infile))
#         ieo.logerror(args.infile, 'File not found.')
# else: # find and process what's in the ingest directory
#     for root, dirs, files in os.walk(args.indir, onerror = None): 
#         for name in files:
#             # if name.endswith('.tar'):
#             #     print(name)
#             i = name.find('.tar')
#             if name[:i] in ProductDict.keys():
                
#             # if name.endswith('.tar.gz') or name.endswith('.tar') or name.endswith('_sr_band7.img'):
#                 fname = os.path.join(root, name)
#                 ssceneID = ProductDict[name[:i]]
#                 # ssceneID = sceneidfromfilename(name)
#                 if ssceneID:
#                     sslist = [x for x in scenedict.keys() if ssceneID in x]
#                     if len(sslist) > 0:
#                         for sceneID in sslist:
#                             if (args.overwrite or not any(ssceneID in x for x in reflist)) and (not fname in filelist): # any(scenedict[ProductID]['sceneID'][:16] == os.path.basename(x)[:16] for x in reflist)
#                                 print('Found unprocessed SceneID {}, adding to processing list.'.format(sceneID))
#                                 filelist.append(fname)

        
# Now process files that are in the list
if not ieo.usePostGIS:
    data_source = driver.Open(ieo.catgpkg, 1)
    ds2 = driver.Open(ieo.ieogpkg, 0)
else:
    data_source = ogr.Open(ieo.catgpkg, 1)
    ds2 = ogr.Open(ieo.ieogpkg, 0)
layer = data_source.GetLayer(ieo.Sen2shp)
NTSlayer = ds2.GetLayer(ieo.NTS)
for bucket in sorted(scenedict.keys()):
    if bucket != 'lastupdate':
        print(f'Now processing files in bucket {bucket}.')
        for year in sorted(scenedict[bucket].keys()):
            for month in sorted(scenedict[bucket][year].keys()):
                for day in sorted(scenedict[bucket][year][month].keys()):
                    numfiles = len(scenedict[bucket][year][month][day]['ProductIDs'].keys())
                    if numfiles > 0:
                        
                        print(f'There are {numfiles} scenes to be processed for date {year}/{month}/{day}.')
                        filenum = 1
                        for ProductID in sorted(scenedict[bucket][year][month][day]['ProductIDs'].keys()):
                            # i = scenedict[bucket][year][month][day]['granules'].index(f)
                            f = scenedict[bucket][year][month][day]['ProductIDs'][ProductID]
                            # if ProductID in f:
                            layer.StartTransaction()                            
                            if f.endswith('/'):
                                f = f[:-1]
                            # ProductID = os.path.basename(f)
                            # if len(ProductID) > 60:
                            #     ProductID = ProductID[:60]
                            satellite = ProductID[:3]
                            if args.verbose:
                                print(f)                            
                        #        try:
                            proddir = os.path.join(ieo.Sen2ingestdir, ProductID)
                            # Prodlist.append(proddir)
                            try:
                                if args.overwrite or not os.path.isdir(proddir):
                                    if args.localingest or f.endswith('.zip'):
                                        if not os.path.isfile(f):
                                            print(f'\nDownloading {ProductID} metadata from bucket {bucket} ({filenum}/ {numfiles}).\n')
                                            S3ObjectStorage.downloadfile(ieo.Sen2ingestdir, bucket, f)
                                        zfile = os.path.join(ieo.Sen2ingestdir, f)
                                        ieo.unzip(zfile, proddir)
                                        
                                    else:
                                        print(f'\nDownloading {ProductID} from bucket {bucket}, file number {filenum} of {numfiles}.\n')
                                        S3ObjectStorage.download_s3_folder(bucket, f, proddir)
                                        filenum += 1
                                if args.localingest:
                                    if not os.path.isfile(os.path.join(proddir, 'MTD_MSIL2A.xml')):
                                        if os.path.isdir(os.path.join(proddir, f'{os.path.basename(proddir)}.SAFE')):
                                            if os.path.isfile(os.path.join(proddir, f'{os.path.basename(proddir)}.SAFE', 'MTD_MSIL2A.xml')):
                                                proddir = (os.path.join(proddir, f'{os.path.basename(proddir)}.SAFE'))
                                # This will be modified soon to process multiple Sentinel-2 tiles from the same day.
                                print(f'Now importing scene {ProductID} for date {year}/{month}/{day}.')
                            # geom = joinfeatures(scenedict[bucket][year][month][day]['ProductIDs'], layer)
                                layer.SetAttributeFilter(f'"ProductID" = \'{ProductID}\'')
                                if layer.GetFeatureCount() > 0:
                                    feature = layer.GetNextFeature()
                                    # print(f'\r{feature.GetField("ProductID")}')
                                    # PiD = feature.GetField('ProductID')
                                    # if verbose: print(f'PiD {PiD}: {len(PiD)}, ProductID {ProductID}: {len(ProductID)}.')
                                    # if PiD == ProductID:
                                    if verbose: print(f'Found feature for {ProductID}.')
                                    gettiles(feature, NTSlayer)
                                    feature = ieo.importSentinel2totiles( \
                                                 proddir, feature, \
                                                 remove = args.remove, \
                                                 overwrite = args.overwrite, 
                                                 CalcVIs = args.CalcVIs, \
                                                  CalcNDVI = CalcNDVI, \
                                                  CalcEVI = CalcEVI, CalcNDTI = CalcNDTI, \
                                                  CalcNBR = CalcNBR, \
                                                  outdatasettype = outdatasettype)
                        # tilelist = []
                                    
                                            # for feature in layer:
                                            #     if feature.GetField('ProductID') in scenedict[bucket][year][month][day]['ProductIDs']:
                                            #         outtilelist = []
                                            #         geometry = feature.geometry
                                            #         for feat in layer2:
                                            #             tilegeom = feat.geometry()
                                            #             tilename = feat.GetField('Tile')
                                            #             if tilegeom.Intersect(geometry) and tilename in tilelist:
                                            #                 outtilelist.append(tilename)
                                            #         if len(outtilelist) > 0:
                                            #             for tilename in outtilelist:
                                            #                 if outtilelist.index(tilename) == 0:
                                            #                     tilestr = tilename
                                            #                 else:
                                            #                     tilestr +=  f',{tilename}'
                                            #             for fieldname in ['Surface_reflectance_tiles', 'NDVI_tiles', 'EVI_tiles', 'NDTI_tiles', 'NBR_tiles']:
                                            #                 feature.SetField(fieldname, tilestr)
                                    if verbose: print(f'Tile processing for {ProductID} complete, updating feature in geodatabase layer.')
                                    feature.SetField('Tile_filename_base', f'{satellite}_{year}{month}{day}')
                                    if not args.copylater: feature.SetField('S3_tile_bucket', 'sentinel2')
                                    now = datetime.datetime.now()
                                    feature.SetField('Raster_Ingest_Time', now.strftime('%Y-%m-%d %H:%M:%S'))
                                    if not args.copylater: 
                                        feature.SetField('S3_tile_bucket', 'sentinel2')
                                    layer.SetFeature(feature)
                                    if args.removelocal:
                                        for d in [proddir, proddir + '_ITM']:
                                            if os.path.isdir(d):
                                                print(f'Deleting path: {d}')
                                                shutil.rmtree(d)
                                    if not args.copylater: 
                                        if args.bucket:
                                            movefile(f'{ProductID}.zip', args.bucket, 'ingested', f'sentinel2/{year}/{month}/{day}/{ProductID}.zip')
                                        elif f.endswith('.zip'):
                                            try:
                                                S3ObjectStorage.copyfilestobucket(bucket = 'ingested', targetdir = f'sentinel2/{year}/{month}/{day}', filename = f)
                                                if args.removelocal:
                                                    os.remove(f)
                                            except Exception as e:
                                                print(f'ERROR with file transfer for {ProductID}: ', e)
                                                ieo.logerror(ProductID, e)
                                        if verbose: print(f'Feature {ProductID} set. Archiving tiles to bucket: sentinel2')
                                        tilelist = feature.GetField('Surface_reflectance_tiles')
                                        if isinstance(tilelist, str):
                                            tilelist = tilelist.split(',')
                                        else:
                                            tilelist = []
                                            tilefilelist = glob.glob(os.path.join(ieo.Sen2srdir, f'{satellite}_{year}{month}{day}*.dat'))
                                            if len(tilefilelist) > 0:
                                                for tfl in tilefilelist:
                                                    if verbose: print(f'Adding tile to list: {tfl[-7:-4]}.')
                                                    tilelist.append(tfl[-7:-4])
                                        if len(tilelist) > 0:
                                            for tile in tilelist:
                                                # scenedict[bucket][year][month][day]['tiles'].append(tile)
                                                
                                                for d in transferdict.keys():
                                                    dname = ieo.Sen2srdir.replace('SR', d)
                                                    copylist = glob.glob(os.path.join(dname, f'{satellite}_{year}{month}{day}_{tile}.*'))
                                                    z = transferdict[d]
                                                    if len(copylist) > 0:
                                                        for item in copylist:
                                                            if item.endswith('.bak'):
                                                                if args.removelocal:
                                                                    os.remove(item)
                                                                copylist.remove(item)
                                                    if len(copylist) > 0:
                                                        remotedir = f'{d}/{tile}/{year}/{month}/{day}'
                                                        if verbose: print(f'Copying {len(copylist)} files to bucket/path: sentinel2/{remotedir}.')
                                                        try:
                                                            S3ObjectStorage.copyfilestobucket(bucket = 'sentinel2', targetdir = remotedir, filelist = copylist)
                                                        except Exception as e:
                                                            print(f'ERROR with file transfer for {ProductID}: ', e)
                                                            ieo.logerror(ProductID, e)
                                                        if args.removelocal:
                                                            for c in copylist:
                                                                print(f'Deleting from disk: {c}')
                                                                os.remove(c)
                            except Exception as e:
                                print(f'ERROR: {e}')
                                ieo.logerror(f, e)
                                with open(badscenefile, 'a') as output:
                                    output.write(f'{bucket}: {f}\n')
                            layer.CommitTransaction()
data_source = None
ds2 = None
layer = None
# layer2 = None                        
                                   
                    #        except Exception as e:
                    #            print('There was a problem processing the scene. Adding to error list.')
                    #            exc_type, exc_obj, exc_tb = sys.exc_info()
                    #            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    #            print(exc_type, fname, exc_tb.tb_lineno)
                    #            print(e)
                    #            ieo.logerror(f, '{} {} {}'.format(exc_type, fname, exc_tb.tb_lineno))
                        # else:
                        #     print('Scene {} has already been processed, skipping file number {} of {}.'.format(scene, filenum, numfiles))



print('Processing complete.')