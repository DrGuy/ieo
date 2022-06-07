# -*- coding: utf-8 -*-
"""
Created on Fri May 13 16:57:58 2022

@author: guyse
"""

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
        import ieo
        import S3ObjectStorage as s3
    else:
        print('Error: that is not a valid path for the IEO module. Exiting.')
        sys.exit()

def checkHDR(hdr, layer):
    basename = os.path.basename(hdr)
    sen, datestr, tile = basename[:-4].split('_')
    fixit = False
    linelist = []
    with open(hdr, 'r') as lines:
        for line in lines:
            linelist.append(line)
            line0 = line.strip()
            
            if line.startswith('parent rasters') or line.startswith('S2'):
                if '{' in line0:
                    i = line0.find('{') + 2
                    j = line0.find('}') - 1
                else:
                    i = 0
                    j = len(line0)
                scenes = line[i:j].split(',')
                for scene in scenes:
                    parts = scene.split('_')
                    if parts[0] != basename[:3] or parts[2][:8] != datestr:
                        fixit = True
                        linelist.remove(line)
                        break
    if fixit:
        print(f'Fixing HDR file: {hdr}')
        with open(hdr, 'w') as output:
            for line in linelist:
                output.write(line)
        targetdir = f'SR/{tile}/{datestr[:4]}/{datestr[4:6]}/{datestr[6:]}/'
        s3.copyfilestobucket(bucket = bucket, targetdir = targetdir, filename = hdr)
        d = f'{sen}_MSIL2A_{datestr}'
        print(f'Processing features with ProductIDs starting with: {d}')
        layer.StartTransaction()
        layer.SetAttributeFilter(f'"ProductID" LIKE \'{d}%\'')
        numfeats = layer.GetFeatureCount()
        dellist = []
        if numfeats > 0:
            feature = layer.GetNextFeature()
            while feature:
                ProductID = feature.GetField('ProductID')
                SR_tiles = feature.GetField('Surface_reflectance_tiles')
                tilebasename = feature.GetField('Tile_filename_base')
                # print(SR_tiles)
                if SR_tiles:
                    SR_tiles = SR_tiles.split(',')
                    lsr = len(SR_tiles)
                    if lsr > 0:
                        if tile in SR_tiles:
                            SR_tiles.remove(tile)
                            if len(SR_tiles) > 0:
                                print(f'Updating tile metadata for {ProductID}.')
                                outstr = ''
                                for t in sorted(SR_tiles):
                                    if SR_tiles.index(t) == 0:
                                        outstr = t
                                    else:
                                        outstr += f',{t}'
                                if outstr.startswith(','):
                                    outstr = outstr[1:]
                                for fieldName in ['Surface_reflectance_tiles', 'NDVI_tiles', 'NBR_tiles', 'EVI_tiles', 'NDTI_tiles']:
                                    if feature.GetField(fieldName):
                                        feature.SetField(fieldName, outstr)
                                
                            else:
                                print(f'Deleting corrupt entries for {ProductID}.')
                                for fieldName in ['Raster_Ingest_Time', 'Surface_reflectance_tiles', 'Tile_filename_base', 'NDVI_tiles', 'NBR_tiles', 'EVI_tiles', 'NDTI_tiles']:
                                    feature.SetField(fieldName, None)
                            layer.SetFeature(feature)
                feature = layer.GetNextFeature()
        
        layer.CommitTransaction()
    return layer
        

print('Opening local Sentinel 2 catalog file.\n')
if not ieo.usePostGIS:
    driver = ogr.GetDriverByName('GPKG')
    data_source = driver.Open(ieo.catgpkg, 1)
    
else:
    data_source = ogr.Open(ieo.catgpkg, 1)
    
layer = data_source.GetLayer(ieo.Sen2shp)

    
bucket = 'sentinel2'
prefix = 'SR/'
tiles = s3.getbucketfoldercontents(bucket, prefix, '/')
for tile in tiles:
    print(f'Searching for anomalous HDRs for tile: {tile}')
    years = s3.getbucketfoldercontents(bucket, f'{prefix}{tile}/', '/')
    for year in years:
        months = s3.getbucketfoldercontents(bucket, f'{prefix}{tile}/{year}/', '/')
        for month in months:
            days = s3.getbucketfoldercontents(bucket, f'{prefix}{tile}/{year}/{month}/', '/')
            for day in days:
                flist = s3.getbucketfoldercontents(bucket, f'{prefix}{tile}/{year}/{month}/{day}/', '/')
                for f in flist:
                    if f.endswith('.hdr'):
                        if s3.s3res.Bucket(bucket).Object(f).content_length > 2000:
                            s3.downloadfile(ieo.Sen2srdir, bucket, f)
                            hdr = os.path.join(ieo.Sen2srdir, os.path.basename(f))
                            layer = checkHDR(hdr, layer)
                            os.remove(hdr)
layer = None
data_source = None
print('Processing complete.')
                        