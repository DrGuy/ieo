# -*- coding: utf-8 -*-
"""
Created on Sun Apr 24 15:24:23 2022

@author: guyse
"""
import argparse, os, sys, shutil, datetime
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

from osgeo import ogr

parser = argparse.ArgumentParser('This script imports Sentinel-2 Scihub metadata into PostGIS.')
parser.add_argument('-p', '--password', default = None, type = str, help = 'Password to log into PostGIS server.')
parser.add_argument('-l', '--layer', default = 'Ireland_Sentinel2', type = str, help = 'Directory containing SciHub XML files.')
parser.add_argument('-b', '--bucket', default = 'sentinel2', type = str, help = 'S3 bucket to scan. Default = "sentinel2".')
parser.add_argument('-o', '--outbucket', default = 'ingested', type = str, help = 'S3 bucket to move any ZIP files found in scanned bucket. Default = "ingested".')
parser.add_argument('--prefix', default = 'SR/', type = str, help = 'S3 bucket prefix to search. Default = "SR/".')
args = parser.parse_args()

bucket = args.bucket.lower()
outbucket = args.outbucket.lower() 
prefix = args.prefix

if args.layer:
    layers = [args.layer]
else:
    layers = ['Ireland_Sentinel2', 'WRS2_Ireland_scenes']

if args.password:
    catgpkg = f'{ieo.catgpkg} password={args.password}'
    ds = ogr.Open(catgpkg, 1)
else:
    ds = ogr.Open(ieo.catgpkg, 1)
layer = ds.GetLayer(ieo.Sen2shp)

def fixGeoDB(tile, tile_basestr):
    querystr = querystr = f'("Tile_filename_base" = \'{tile_basestr}\')'
    layer.StartTransaction()
    layer.SetAttributeFilter(querystr)
    if layer.GetFeatureCount() > 0:
        for feature in layer:
            tilelist = feature.GetField('Surface_reflectance_tiles')
            if tilelist:
                if tile in tilelist:
                    tilelist = tilelist.split(',')
                    tilelist.remove(tile)
                    ProductID = feature.GetField('ProductID')
                    print(f'Updating feature {ProductID} with removal of tile {tile}.')
                    if len(tilelist) > 0: 
                        for t in tilelist:
                            if tilelist.index(t) == 0: 
                                tilestr = t
                            else:
                                tilestr += f',{t}'
                    else:
                        tilestr = None
                    feature.SetField('Surface_reflectance_tiles', tilestr)
                    layer.SetFeature(feature)
    layer.CommitTransaction()
            
fixlist = []
movelist = []
tiles = s3.getbucketfoldercontents(bucket, f'{prefix}', '/')
for tile in tiles:
    years = s3.getbucketfoldercontents(bucket, f'{prefix}{tile}/', '/')
    for year in years:
        months = s3.getbucketfoldercontents(bucket, f'{prefix}{tile}/{year}/', '/')
        for month in months:
            days = s3.getbucketfoldercontents(bucket, f'{prefix}{tile}/{year}/{month}/', '/')
            for day in days:
                dellist = []
                flist = s3.getbucketfoldercontents(bucket, f'{prefix}{tile}/{year}/{month}/{day}/', '/')
                if len(flist) > 0:
                    for f in flist:
                        if f.endswith('.dat') or f.endswith('.hdr'):
                            if f.endswith('.dat'):
                                dat = f
                                hdr = f.replace('.dat', '.hdr')
                            if f.endswith('.hdr'):
                                hdr = f
                                dat = f.replace('.hdr', '.dat')
                            if f == dat:
                                obj = s3res.Object(bucket, f)
                                if obj.content_length < 100000000 or not hdr in flist:
                                    if not f in dellist:
                                        print(f'File found with insuffient length or missing HDR file. Adding to delete list: {f}')
                                        dellist.append(f)
                                        if hdr in flist and not hdr in dellist:
                                            dellist.append(hdr)
                            elif f == hdr and not dat in flist and not hdr in dellist:
                                print(f'DAT file is missing. Adding to delete list: {f}')
                                dellist.append(f)
                            elif f == hdr:
                                obj = s3res.Object(bucket, f)
                                if obj.content_length > 2000:
                                    print(f'Found anomalously large HDR file, adding to fix list: {f}')
                                    fixlist.append(f)
                        elif f.endswith('.bak') and not f in dellist:
                            print(f'Found BAK file to delete: {f}')
                            dellist.append(f)
                        elif f.endswith('.zip'): 
                            if not f in movelist:
                                print(f'Adding ZIP file to move list: {f}')
                                movelist.append(f)
                            else:
                                print(f'Adding ZIP file to delete list: {f}')
                                dellist.append(f)
                if len(dellist) > 0:
                    for f in dellist:
                        print(f'Deleting object from {bucket} bucket: {f}')
                        s3res.Object(bucket, f).delete()
                    if any(x[-4:] in ['.hdr', '.dat'] for x in dellist):
                        tile_basestr = os.path.basename(dellist[0])[:12]
                        fixGeoDB(tile, tile_basestr)


if len(fixlist) > 0:
    print(f'Found {len(fixlist)} anomalously large HDR files. Attempting to fix.')
    for f in fixlist:
        updatedhdr = False
        if bucket == 'sentinel2': outdir = ieo.Sen2srdir
        outbasename = os.path.basename(f)
        hdr = os.path.join(outdir, outbasename)
        print(f'Now processing: {f} ({fixlist.index(f) + 1}/{len(fixlist)})')
        if not os.path.isfile(hdr):
            s3.downloadfile(outdir, bucket, f)
        now = datetime.datetime.now()
        bak = f'{hdr}.{now.strftime("%Y%m%d-%H%M%S")}.bak'
        shutil.move(hdr, bak)
        outlines = []
        with open(bak, 'r') as lines:
            for line in lines:
                if line.startswith('ENVI') or ('=' in line and not line.startswith('parent rasters')):
                    outlines.append(line)
                elif len(line) > 3:
                    if line.startswith('parent rasters'):
                        i = line.find('{') + 1
                        j = line.find('}')
                        parentrasters = line[i:j].strip().split(',')
                    else:
                        parentrasters = line.strip().split(',')
                        updatedhdr = True
        if len(parentrasters) > 0:
            for r in parentrasters:
                if len(r) < 3 or not outbasename[4:12] in r:
                    parentrasters.remove(r)
                    updatedhdr = True
        if len(parentrasters) > 0 and updatedhdr:
            parentrasterstr = 'parent rasters = { '
            for r in parentrasters:
                if parentrasters.index(r) == 0:
                    parentrasterstr += r
                else:
                    parentrasterstr += f',{r}'
            parentrasterstr += ' }\n'
            outlines.append(parentrasterstr)
            print(f'Writing updated HDR file to disk: {outbasename}')
            with open(hdr, 'w') as output:
                for line in outlines:
                    output.write(line)
            s3.copyfilestobucket(bucket = bucket, filename = hdr, targetdir = os.path.dirname(f))
        print('Deleting temporary files.')
        os.remove(bak)
        if not os.path.isfile(hdr.replace('.hdr', '.dat')):
            os.remove(hdr)
                
                    

n = len(movelist)
if n > 0:
    i = 1
    print(f'Now moving {n} misplaced files from sentinel2 bucket to ingested bucket.')
    for f in movelist:
        s3.movefile(f, bucket, outbucket, f.replace(prefix, f'{outbucket}/'), i = i, n = n)
        i += 1
                        
