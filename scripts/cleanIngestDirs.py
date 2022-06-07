# -*- coding: utf-8 -*-
"""
Created on Thu Mar 31 15:26:31 2022

@author: guyse
"""

import os, shutil, glob, sys, datetime, argparse
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

parser = argparse.ArgumentParser('This script imports Sentinel-2 Level 2A-processed scenes into the local library. It stacks images and converts them to the locally defined projection in IEO, and adds ENVI metadata.')
parser.add_argument('-s', '--sensor', default = None, type = str, help = 'Only remove files relating to a specific sensor. If not set, local ingest files for all sensors will be deleted.')
args = parser.parse_args()

sensordict = {
    'sentinel2' : {
        'ingestdir' : ieo.Sen2ingestdir,
        'SRdir' : ieo.Sen2srdir},
    'landsat' : {
        'ingestdir' : ieo.ingestdir,
        'SRdir' : ieo.srdir},
    }

if args.sensor:
    sensor = args.sensor.lower()
    if not sensor in sensordict.keys():
        print(f'ERROR: {sensor} is not supported by this script. Exiting.')
        sys.exit()
    else:
        print(f'Removing files belonging to sensor: {sensor}')
        sensors = [sensor]
else:
    sensors = ['landsat', 'sentinel2']

for sensor in sensors:
    d = sensordict[sensor]['ingestdir']
    flist = glob.glob(os.path.join(d, '*'))
    if len(flist) > 0:
        for f in flist:
            if os.path.isdir(f):
                print(f'Deleting path: {f}')
                shutil.rmtree(f)
        dirname = sensordict[sensor]['SRdir']
        for d in ['SR', 'EVI', 'ST', 'NDVI', 'NBR', 'NDTI', 'pixel_qa', 'aerosol_qa', 'radsat_qa']:
            if not d == 'SR':
                dr = dirname.replace('SR', d)
            else:
                dr = dirname
            if os.path.isdir(dr):
                filelist = glob.glob(os.path.join(dr, '*'))
                if len(filelist) > 0:
                    print(f'Found {len(filelist)} files to possibly transfer in path: {dr}')
                    for f in filelist:
                        if f.endswith('.bak'):
                            print(f'Deleting from disk: {f}')
                            os.remove(f)
                        else:
                            parts = os.path.basename(f)[:-4].split('_')
                            tile, year, month, day = parts[2], parts[1][:4], parts[1][4:6], parts[1][6:]
                            prefix = f'{d}/{tile}/{year}/{month}/{day}'
                            S3ObjectStorage.copyfilestobucket(bucket = sensor, filename = f, targetdir = prefix)
                            print(f'Deleting from disk: {f}')
                            os.remove(f)

# for sensor in ['landsat', 'sentinel2']:
#     if sensor == 'landsat':
#         dirname = ieo.ingestdir
#         ext = '.gz'
#     else:
#         dirname = ieo.Sen2ingestdir
#         ext ='.zip'
#     filelist = glob.glob(os.path.join(dirname, f'*{ext}'))
#     if len(filelist) > 0:
#         print(f'Found {len(filelist)} files in path: {dirname}')
#         print('Searching for files which are over a day old.')
#         for f in filelist:
#             ctime = os.path.getmtime(f) 
#             if (datetime.datetime.now() - datetime.datetime.fromtimestamp(ctime)).days >= 1:
#                 print(f'Found file that is at least one day old. Moving to scihub bucket: {f} ({filelist.index(f) + 1}/{len(filelist)})')
#                 parts = os.path.basename(f)[:-4].split('_')
#                 year, month, day = parts[2][:4], parts[2][4:6], parts[2][6:8]
#                 prefix = f'{sensor}/{year}/{month}/{day}'
#                 S3ObjectStorage.copyfilestobucket(bucket = 'scihub', filename = f, targetdir = prefix)
#                 print(f'Deleting from disk: {f}')
#                 os.remove(f)
                
        

print('Processing complete.')