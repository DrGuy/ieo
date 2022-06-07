# -*- coding: utf-8 -*-
"""
Created on Wed Apr 13 11:18:54 2022

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
        import ieo, S3ObjectStorage
    else:
        print('Error: that is not a valid path for the IEO module. Exiting.')
        sys.exit()

bucket1 = 'scihub'
bucket2 = 'ingested'

prefix = 'sentinel2/'

conn = ogr.Open(ieo.catgpkg)
ieo_conn = ogr.Open(ieo.ieogpkg)

layer = conn.GetLayer(ieo.Sen2shp)
# IE_layer = ieo_conn.GetLayer(Ireland_layer)
# IE_layer.SetAttributeFilter(f'"{IE_poly_fieldname}" = \'{IE_poly_ID}\'')
# IE_feat = IE_layer.GetNextFeature()
# IE_geom = IE_feat.GetGeometryRef()

MGRSlayer = ieo_conn.GetLayer(ieo.Sen2tiles)
MGRSlist = []
for feat in MGRSlayer:
    tilename = feat.GetField('TILE_ID')
    if not tilename in MGRSlist:
        MGRSlist.append(tilename)
MGRSlayer = None

ingestdict = {}

years = S3ObjectStorage.getbucketfoldercontents(bucket2, prefix, '/')
if len(years) > 0:
    for year in years:
        months = S3ObjectStorage.getbucketfoldercontents(bucket2, f'{prefix}{year}/', '/')
        for month in months:
            days = S3ObjectStorage.getbucketfoldercontents(bucket2, f'{prefix}{year}/{month}/', '/')
            for day in days:
                ingestdict[f'{prefix}{year}/{month}/{day}'] = S3ObjectStorage.getbucketfoldercontents(bucket2, f'{prefix}{year}/{month}/{day}/', '/')
                print(f'Found {len(ingestdict[f"{prefix}{year}/{month}/{day}"])} scenes on ingested bucket for date {year}/{month}/{day}.')
                
flist = S3ObjectStorage.getbucketfoldercontents(bucket1, '', '/')
if len(flist) > 0:      
    for f in flist:
        if f.endswith('.zip'):
            parts = f.split('_')
            year, month, day = parts[2][:4], parts[2][4:6], parts[2][6:8]
            ingestedf = f'{prefix}{year}/{month}/{day}/{f}'
            if ingestedf in ingestdict[f'{prefix}{year}/{month}/{day}']:
                print(f'Found ingested scene, deleting from scihub bucket: {f}')
                S3ObjectStorage.s3res.Object(bucket1, f).delete()
            
years = S3ObjectStorage.getbucketfoldercontents(bucket1, prefix, '/')
if len(years) > 0:
    for year in years:
        months = S3ObjectStorage.getbucketfoldercontents(bucket1, f'{prefix}{year}/', '/')
        for month in months:
            downloaded = False
            days = S3ObjectStorage.getbucketfoldercontents(bucket1, f'{prefix}{year}/{month}/', '/')
            for day in days:
                flist = S3ObjectStorage.getbucketfoldercontents(bucket1, f'{prefix}{year}/{month}/{day}/', '/')
                key = f'{prefix}{year}/{month}/{day}'
                print(f'{len(flist)} scenes have been found in scihub bucket folder: {key}')
                for f in flist:
                    if key in ingestdict.keys():
                        if f in ingestdict[key]:
                            print(f'Found ingested scene, deleting from scihub bucket: {f}')
                            S3ObjectStorage.s3res.Object(bucket1, f).delete()
                        else:
                            parts = os.path.basename(f).split('_')
                            if not parts[5][1:] in MGRSlist:
                                print(f'Found scene which is not in the Irish MGRS tiles, deleting from scihub bucket: {f}')
                                S3ObjectStorage.s3res.Object(bucket1, f).delete()
                            else:
                                ProductID = os.path.basename(f)[:-4]
                                layer.ResetReading()
                                layer.StartTransaction()                            
                                layer.SetAttributeFilter(f'"ProductID" = \'{ProductID}\'')
                                if layer.GetFeatureCount() > 0:
                                    feature = layer.GetNextFeature()
                                    SRtiles = feature.GetField('Surface_reflectance_tiles')
                                    if SRtiles:
                                        print(f'Scene has already been ingested, deleting from scihub bucket: {f}')
                                        S3ObjectStorage.s3res.Object(bucket1, f).delete()
                                    else:
                                        S3ObjectStorage.downloadfile(ieo.Sen2ingestdir, bucket1, f)
                                        downloaded = True
                                        print(f'Deleting downloaded scene from scihub bucket: {f}')
                                        S3ObjectStorage.s3res.Object(bucket1, f).delete()
                                layer.CommitTransaction()
            if downloaded:
                print(f'Ingesting scenes for {year}/{month}.')
                p = subprocess.Popen(['python', 'importSentinel2.py', '--S2TM', '--noNDVI', '--noEVI', '--noNDTI', '--noNBR', '--removelocal', '--verbose' ,'--localingest', '--maxCC', '100'])
                print(p.communicate())
            
                        