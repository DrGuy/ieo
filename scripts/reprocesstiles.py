# -*- coding: utf-8 -*-

"""
Created on Wed May 18 13:53:15 2022

@author: guyse
"""

import argparse, os, sys, shutil, datetime, json, glob
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
        import ieo
        import S3ObjectStorage as s3
        s3cli = s3.s3cli
        s3res = s3.s3res
        
    else:
        print('Error: that is not a valid path for the IEO module. Exiting.')
        sys.exit()

from osgeo import gdal, ogr, osr
import numpy as np

parser = argparse.ArgumentParser('This script imports Sentinel-2 Scihub metadata into PostGIS.')
parser.add_argument('-t', '--tiles', default = None, type = str, help = 'Comma-delimited list of tiles to check and reprocess. If not set, will reprocess all tiles.')
parser.add_argument('-d', '--dirname', default = '~/ingest', type = str, help = 'Directory containing SciHub XML files.')
parser.add_argument('--noscan', action = 'store_true', help = 'Do not scan for files, used saved results on disk.')
parser.add_argument('--ignoremissing', default = True, type = bool, help = 'Ignore any missing scenes and just process tiles.')
parser.add_argument('--startdate', type = str, default = '2017/04/08', help = 'Starting date, YYYY/MM/DD, default = 2017/04/08')
parser.add_argument('--enddate', type = str, default = '2021/12/31', help = 'Ending date, YYYY/MM/DD, default = 2021/12/31')
args = parser.parse_args()

if not ieo.usePostGIS:
    driver = ogr.GetDriverByName("GPKG")
    data_source = driver.Open(ieo.catgpkg, 1)
    ds2 = driver.Open(ieo.ieogpkg, 0)
else:
    data_source = ogr.Open(ieo.catgpkg, 1)
    ds2 = ogr.Open(ieo.ieogpkg, 0)
    
layer = data_source.GetLayer(ieo.Sen2shp)
if args.tiles:
    tiles = args.tiles.split(',')
else:
    tiles = ieo.gettilelist()

tilelayer = ds2.GetLayer(ieo.NTS)

if len(tiles) > 0:
    print(f'Total tiles to process: {len(tiles)}')
else:
    print('ERROR: No tiles were found to process. Exiting.')
    sys.exit()

if args.startdate:
    startdate = datetime.datetime.strptime(args.startdate, '%Y/%m/%d')
else:
    startdate = datetime.datetime.strptime('2015/06/23', '%Y/%m/%d')
if args.enddate:
    enddate = datetime.datetime.strptime(args.enddate + ' 23:59:59', '%Y/%m/%d %H:%M:%S')
else:
    enddate = datetime.datetime.now()
# def checkLayer(layer, corruptedDict):
#     print('Checking layer for temporal errors.')
#     layer.StartTransaction()
#     for feature in layer:
#         ProductID = feature.GetField('ProductID')
#         acqdatestr = feature.GetField('acquisitionDate')
#         if '.' in acqdatestr:
#             datetimestr = '%Y/%m/%d %H:%M:%S.%f+00'
#         else:
#             datetimestr = '%Y/%m/%d %H:%M:%S+00'
#         acqDate = datetime.datetime.strptime(acqdatestr, datetimestr)
#         # ingestTime = feature.GetField('Raster_Ingest_Time')
#         SR_tiles = feature.GetField('Surface_reflectance_tiles')
#         if SR_tiles:
#             SR_tiles.split(',')
#         else:
#             SR_tiles = []
#         tilebasename = feature.GetField('Tile_filename_base')
#         if tilebasename:
#             ymd = acqDate.strftime('%Y%m%d')
#             # year, month, day = ymd[:4], ymd[4:6], ymd[6:]
            
#             if tilebasename[4:12] != ymd:
#                 print(f'Corruption issues have been found for tiles from scene {ProductID} and with files starting with {tilebasename}.')
#                 basescenename = f'{tilebasename[:4]}MSIL2A_{tilebasename[4:]}'
#                 i = len(basescenename)
#                 for d in [ProductID[:i], basescenename]:
#                     if not d in corruptedDict.keys():
#                         corruptedDict[d] = []
#                     if len(SR_tiles) > 0:
#                         for tile in SR_tiles:
#                             if not tile in corruptedDict[d]:
#                                 corruptedDict[d].append(tile)
#                         print(f'A total of {len(corruptedDict[d])} tiles for: {d}')
#     layer.CommitTransaction()
#     return corruptedDict

# def fixLayer(layer, corruptedDict):
#     if len(corruptedDict.keys()) > 0:
#         dlist = sorted(corruptedDict.keys())
#         for d in dlist:
#             print(f'Processing features with ProductIDs starting with: {d}')
#             layer.StartTransaction()
#             layer.SetAttributeFilter(f'"ProductID" LIKE \'{d}%\'')
#             numfeats = layer.GetFeatureCount()
#             dellist = []
#             if numfeats > 0:
#                 feature = layer.GetNextFeature()
#                 while feature:
#                     ProductID = feature.GetField('ProductID')
#                     SR_tiles = feature.GetField('Surface_reflectance_tiles')
#                     tilebasename = feature.GetField('Tile_filename_base')
#                     # print(SR_tiles)
#                     if SR_tiles:
#                         SR_tiles = SR_tiles.split(',')
#                         lsr = len(SR_tiles)
#                         if lsr > 0:
#                             for tile in SR_tiles:
#                                 if tile in corruptedDict[d]:
#                                     if not tile in dellist:
#                                         dellist.append(d)
#                                     SR_tiles.remove(tile)
#                             if len(SR_tiles) < lsr and len(SR_tiles) > 0 and tilebasename[4:] == d[-8:]:
#                                 print(f'Updating tile metadata for {ProductID}.')
#                                 for tile in sorted(SR_tiles):
#                                     if SR_tiles.index(tile) == 0:
#                                         outstr = tile
#                                     else:
#                                         outstr += f',{tile}'
#                                 for fieldName in ['Surface_reflectance_tiles', 'NDVI_tiles', 'NBR_tiles', 'EVI_tiles', 'NDTI_tiles']:
#                                     if feature.GetField(fieldName):
#                                         feature.SetField(fieldName, outstr)
#                                 layer.SetFeature(feature)
#                             elif len(SR_tiles) == 0 or tilebasename[4:] != d[-8:]:
#                                 print(f'Deleting corrupt entries for {ProductID}.')
#                                 for fieldName in ['Raster_Ingest_Time', 'Surface_reflectance_tiles', 'Tile_filename_base', 'NDVI_tiles', 'NBR_tiles', 'EVI_tiles', 'NDTI_tiles']:
#                                     feature.SetField(fieldName, None)
#                                 layer.SetFeature(feature)
#                     feature = layer.GetNextFeature()
#             if len(dellist) > 0:
#                 print(f'Now deleting files for {len(dellist)} tiles.')
#                 year, month, day = d[-8:-4], d[-4:-2], d[-2:]
#                 purgeTiles(SR_tiles, year, month, day)
#             layer.CommitTransaction()
#     return layer
    
reprocdict = {}
def writeMissingScene(ProductID):
    if os.path.isfile(missingscenefile):
        writestatus = 'a'
    else:
        writestatus = 'w'
    with open(missingscenefile, writestatus) as output:
        output.write(f'{ProductID}\n')
        

def updateJson(jsonfile, reprocdict):
    print(f'Writing interim problematic data to: {jsonfile}')
    with open(jsonfile, 'w') as outfile:
        outfile.write(json.dumps(reprocdict))

def getFile(ProductID, layer, year, month, day):
    bucket = None
    localfile = os.path.join(ieo.Sen2ingestdir, f'{ProductID}.zip')
    proddir = os.path.join(ieo.Sen2ingestdir, ProductID)
    if os.path.isfile(localfile):
        print(f'Scene {ProductID} is already present on disk.')
    else:
        filelist = s3.getbucketfoldercontents('ingested', f'sentinel2/{year}/{month}/{day}/', '')
        if f'sentinel2/{year}/{month}/{day}/{ProductID}.zip' in filelist:
            s3.downloadfile(ieo.Sen2ingestdir, 'ingested', f'sentinel2/{year}/{month}/{day}/{ProductID}.zip')
        else:
            filelist = s3.getbucketfoldercontents('scihub', '', '')
            if f'{ProductID}.zip' in filelist:
                s3.downloadfile(ieo.Sen2ingestdir, 'scihub', f'{ProductID}.zip')
            else:
                # layer.SetAttributeFilter(f'"ProductID" = \'{prodstr}%\'')
                # if layer.GetFeatureCount() > 0:
                #     feature = layer.GetNextFeature()
                #     bucket = feature.GetField('S3_ingest_bucket')
                if not bucket and not year in ['2022', '2015', '2016', '2017']:
                    if month in ['01', '02', '03']:
                        q = 1
                    elif month in ['04', '05', '06']:
                        q = 2
                    elif month in ['07', '08', '09']:
                        q = 3
                    else:
                        q = 4
                    bucket = f's2-l2a-{year}-q{q}'
                elif not bucket:
                    bucket = f's2-l2a-{year}'
                parts = ProductID.split('_')
                prefix = f'{parts[5][1:3]}/{parts[5][3:4]}/{parts[5][4:6]}/{year}/{month}/{day}/'
                print(f'Trying: {bucket} {prefix}')
                filelist = s3.getbucketfoldercontents(bucket, prefix, '/')
                if len(filelist) > 0:
                    for f in filelist:
                        if f.startswith(ProductID):
                            proddir = os.path.join(ieo.Sen2ingestdir, f)
                            if not os.path.isdir(proddir):
                                s3.download_s3_folder(bucket, f'{parts[5][1:3]}/{parts[5][3:4]}/{parts[5][4:6]}/{year}/{month}/{day}/{f}/', proddir)
                else:
                    print(f'ERROR: No data found for scene {ProductID}.')
                    writeMissingScene(ProductID)
                    return None
    if os.path.isfile(localfile) and not os.path.isdir(proddir):
        ieo.unzip(localfile, proddir)
    if localfile.endswith('.zip'):
        if not os.path.isfile(os.path.join(proddir, 'MTD_MSIL2A.xml')):
            if os.path.isdir(os.path.join(proddir, f'{os.path.basename(proddir)}.SAFE')):
                if os.path.isfile(os.path.join(proddir, f'{os.path.basename(proddir)}.SAFE', 'MTD_MSIL2A.xml')):
                    proddir = (os.path.join(proddir, f'{os.path.basename(proddir)}.SAFE'))
    if os.path.isfile(os.path.join(proddir, 'MTD_MSIL2A.xml')):
        return proddir
    else:
        print(f'ERROR: No MTD_MSIL2A.xml file found for scene {ProductID}.')
        writeMissingScene(ProductID)
        return None

def checkMissingData(dat, layer, tilelayer, tiles, prodstr, *args, **kwargs):
    outlist = []
    minzerofrac = kwargs.get('minzerofrac', 0.05)
    excesszeroes = False
    print(f'Opening file: {f}')
    src_ds = gdal.Open(dat)
    nb = src_ds.RasterCount
    # if nb < 6:
    #     continue
    ns = src_ds.RasterXSize
    nl = src_ds.RasterYSize
    numpixels = ns * nl
    minpixels = numpixels - int(np.ceil(numpixels * minzerofrac))
    srcband = src_ds.GetRasterBand(4)
    stats = srcband.GetStatistics( True, True )
    if stats[0] == 0:
        srcarr = srcband.ReadAsArray(0, 0 , ns, nl).astype(np.uint)
        nonzeropixels = np.count_nonzero(srcarr)
        if nonzeropixels <= minpixels:
            zeropct = (1 - (nonzeropixels / numpixels)) * 100
            print(f'Tile has {zeropct:.02f}% zero-value tiles. Identifying scenes.')
            excesszeroes = True
    if excesszeroes or not dat:    
        tilelayer.SetAttributeFilter(f'"Tile" = \'{tile}\'')
        feat = tilelayer.GetNextFeature()
        tilegeom = feat.GetGeometryRef()
        layer.SetAttributeFilter(f'"ProductID" LIKE \'{prodstr}%\'')
        layer.SetSpatialFilter(tilegeom)
        if layer.GetFeatureCount() > 0:
            for feature in layer:
                fgeom = feature.GetGeometryRef()
                if fgeom.Intersect(tilegeom):
                    ProductID = feature.GetField('ProductID')
                    print(f'Scene {ProductID} is potentially missing from tile. Adding to the list.')
                    outlist.append(ProductID)
            
            # driver = ogr.GetDriverByName('MEMORY')
            # dst_layername = "missing_data"
            # dst_ds = drv.CreateDataSource('memData')
            # dst_layer = dst_ds.CreateLayer(dst_layername, srs = None )

            # gdal.Polygonize( srcband, None, dst_layer, 0, [], callback = None )
            
            # if dst_layer.GetFeatureCount() > 0:
            #     feat = dst_layer.GetNextFeature()
            #     while feat:
            #         geom = feat.GetGeometryRef()
            #         
                                
            #         feat = dst_layer.GetNextFeature()
            # dst_layer = None
            # dst_ds = None
    srcarr = None
    srcband = None
    src_ds = None
    return outlist

missingscenefile = '/data/temp/sentinel2/missing-S2_scenes.txt'

jsonfile = '/data/temp/sentinel2/reproc.json'
updatejson = False
scihubfiles = s3.getbucketfoldercontents('scihub', '', '')
if os.path.isfile(jsonfile):
    print(f'Reading in previously processed data from: {jsonfile}')
    with open(jsonfile, 'r') as data:
        reprocdict = json.load(data)
else:
    reprocdict = {}
if not args.noscan:
    for tile in tiles:
        print(f'Searching for rasters for tile: {tile}')
        years = s3.getbucketfoldercontents('sentinel2', f'SR/{tile}/', '/')
        for year in years:
            months = s3.getbucketfoldercontents('sentinel2', f'SR/{tile}/{year}/', '/')
            for month in months:
                days = s3.getbucketfoldercontents('sentinel2', f'SR/{tile}/{year}/{month}/', '/')
                for day in days:
                    dat = None
                    hdr = None
                    flist = s3.getbucketfoldercontents('sentinel2', f'SR/{tile}/{year}/{month}/{day}/', '/')
                    if flist[0] == '':
                        flist.pop(0)
                    basename = os.path.basename(flist[0])[:12]
                    try:
                        sat, datestr = basename.split('_')
                    except Exception as e:
                        print(f'Error:\n\t{flist[0]}\n\t{e}')
                        sys.exit()
                    updatejson = False
                    prodstr = f'{sat}_MSIL2A_{datestr}'
                    if not prodstr in reprocdict.keys():
                        updatejson = True
                    elif not tile in reprocdict[prodstr]['tiles']:
                        updatejson = True
                    if updatejson:
                        for f in flist:
                            if f.endswith('.dat'):
                                dat = os.path.join(ieo.Sen2srdir, os.path.basename(f))
                            elif f.endswith('.hdr'):
                                hdr = os.path.join(ieo.Sen2srdir, os.path.basename(f))
                            if not os.path.isfile(os.path.join(ieo.Sen2srdir, os.path.basename(f))):
                                s3.downloadfile(ieo.Sen2srdir, 'sentinel2', f)
                        if not hdr:
                            dat = None
                        outlist = checkMissingData(dat, layer, tilelayer, tiles, prodstr)
                        if len(outlist) > 0:
                            if not prodstr in reprocdict.keys():
                                reprocdict[prodstr] = {}
                                reprocdict[prodstr]['scenes'] = []
                                reprocdict[prodstr]['tiles'] = []
                                reprocdict[prodstr]['corrected'] = False
                            if not tile in reprocdict[prodstr]['tiles']:
                                reprocdict[prodstr]['tiles'].append(tile)
                                # updatejson = True
                            for item in outlist:
                                if not item in reprocdict[prodstr]['scenes']:
                                    reprocdict[prodstr]['scenes'].append(item)
                                    # updatejson = True
                        updateJson(jsonfile, reprocdict)
                            
# if updatejson:
#     updateJson(jsonfile, reprocdict)
errorlist = []
numdates = len(reprocdict.keys())
print(f'A total of {numdates} dates require tile processing.')
for prodstr in sorted(reprocdict.keys()):
    if (not reprocdict[prodstr]['corrected']) and (len(reprocdict[prodstr]["scenes"]) > 1):
        procday = True
        proddirs = []
        ITMfiles = []
        parts = prodstr.split('_')
        datestr = parts[2]
        datetuple = datetime.datetime.strptime(reprocdict[prodstr]['scenes'][0][11:26], '%Y%m%dT%H%M%S')
        year, month, day = datestr[:4], datestr[4:6], datestr[6:]
        if datetuple >= startdate and datetuple <= enddate:
            print(f'Downloading any existing tiles for {year}/{month}/{day}.')
            for tile in tiles:
                flist = s3.getbucketfoldercontents('sentinel2', 'SR/{tile}/{year}/{month}/{day}/', '')
                if len(flist) >= 2:
                    for f in flist:
                        print(f'Downloading file: {f}')
                        s3.downloadfile(ieo.Sen2srdir, 'sentinel2', f)
                        if f.endswith.dat:
                            ITMfiles.append(os.path.join(ieo.Sen2shp, os.path.basename(f)))
            print(f'Now processing {len(reprocdict[prodstr]["tiles"])} tiles and {len(reprocdict[prodstr]["scenes"])} scenes for {year}/{month}/{day}.')
            for ProductID in reprocdict[prodstr]["scenes"]:
                proddir = getFile(ProductID, layer, year, month, day)
                if proddir:
                    proddirs.append(proddir)
                else:
                    errorlist.append(ProductID)
                    if not args.ignoremissing:
                        procday = False
                        print(f'Cleaning up files for {year}/{month}/{day}.')
                        flist = glob.glob(os.path.join(ieo.Sen2ingestdir, f'{prodstr}*'))
                        if len(flist) > 0:
                            for f in flist:
                                print(f'Deleting: {f}')
                                if os.path.isdir(f):
                                    shutil.rmtree(f)
                                else:
                                    os.remove(f)
                        break
            if procday and len(proddirs) > 0:
                vrtdir = os.path.join(ieo.Sen2ingestdir, datestr)
                if not os.path.isdir(vrtdir):
                    os.mkdir(vrtdir)
                for proddir in proddirs:
                    outputfile, datestr, sat = ieo.WarpMGRS(proddir, 'S2TM')
                    ITMfiles.append(outputfile)
                vrtfile = os.path.join(vrtdir, f'{sat}_{datestr}.vrt')
                # prodstr = f'{sat}_MSIL2A_{datestr}'
                print(f'Creating VRT from {len(ITMfiles)} files.')
                gdal.BuildVRT(vrtfile, ITMfiles, options = gdal.BuildVRTOptions(srcNodata = 0))
                
                print(f'Converting data in scene {ProductID} to tiles.')   
                tilelist = ieo.converttotiles(vrtfile, ieo.Sen2srdir, 'S2TM', pixelqa = False, \
                              overwrite = True, ProductID = f'{sat}_{datestr}', \
                              datestr = datestr, satellite = sat, \
                              CalcVIs = False, CalcNDVI = False, \
                              CalcEVI = False, CalcNDTI = False, \
                              CalcNBR = False, tilelist = reprocdict[prodstr]['tiles'])
                if len(tilelist) > 0:
                    for tile in tilelist:
                        copylist = glob.glob(os.path.join(ieo.Sen2srdir, f'{sat}_{year}{month}{day}_{tile}.*'))
                        # z = transferdict[d]
                        if len(copylist) > 0:
                            for item in copylist:
                                if item.endswith('.bak'):
                                    print(f'Deleting: {item}')
                                    os.remove(item)
                                    copylist.remove(item)
                        if len(copylist) > 0:
                            remotedir = f'SR/{tile}/{year}/{month}/{day}'
                            print(f'Copying {len(copylist)} files to bucket/path: sentinel2/{remotedir}.')
                            try:
                                s3.copyfilestobucket(bucket = 'sentinel2', targetdir = remotedir, filelist = copylist)
                                for item in copylist:
                                    print(f'Deleting: {item}')
                                    os.remove(item)
                            except Exception as e:
                                print(f'ERROR with file transfer for {vrtfile}: ', e)
                                ieo.logerror(vrtfile, e)
                    for proddir in proddirs:
                        rmlist = [proddir, proddir + '_ITM']
                        if os.path.dirname(proddir).endswith(ProductID):
                            rmlist.append(os.path.dirname(proddir))
                        for d in rmlist:
                            if os.path.isdir(d):
                                print(f'Deleting path: {d}')
                                shutil.rmtree(d)
                    print(f'Deleting path: {vrtdir}')
                    shutil.rmtree(vrtdir)
                    for ProductID in reprocdict[prodstr]["scenes"]:
                        zfile = os.path.join(ieo.Sen2ingestdir, f'{ProductID}.zip')
                        if os.path.isfile(zfile):
                            if f'{ProductID}.zip' in scihubfiles:
                                s3.movefile(f'{ProductID}.zip', 'scihub', 'ingested', f'sentinel2/{year}/{month}/{day}/{ProductID}.zip')
                            elif not f'sentinel2/{year}/{month}/{day}/{ProductID}.zip' in s3.getbucketfoldercontents('ingested', f'sentinel2/{year}/{month}/{day}/', ''):
                                s3.copyfilestobucket(bucket = 'ingested', targetdir = f'sentinel2/{year}/{month}/{day}', filename = zfile)
                                        
                            print(f'Deleting input file: {zfile}')
                            os.remove(zfile)
                    reprocdict[prodstr]['corrected'] = True   
                    updateJson(jsonfile, reprocdict)
                elif len(proddirs) == 0: 
                    print('ERROR: No data exist to process. Skipping.')
                    if len(ITMfiles) > 0:
                        for f in ITMfiles:
                            if f.endswith('.dat'):
                                os.remove(f)
                                os.remove(f.replace('.dat', '.hdr'))

if len(errorlist) > 0:
    errorfile = '/data/temp/sentinel2/reproc-errors.txt'
    print(f'{len(errorlist)} errors occurred. Saving list to: {errorfile}')
    with open(errorfile, 'w') as output:
        for e in errorlist:
            output.write(f'{e}\n')

print('Processing complete.')