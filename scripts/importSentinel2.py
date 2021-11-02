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

import os, sys, glob, datetime, argparse, pickle#, ieo, shutil
from osgeo import ogr

try: # This is included as the module may not properly install in Anaconda.
    import ieo
except:
    ieodir = os.getenv('IEO_INSTALLDIR')
    if not ieodir:
        print('Error: IEO failed to load. Please input the location of the directory containing the IEO installation files.')
        ieodir = input('IEO installation path: ')
    if os.path.isfile(os.path.join(ieodir, 'ieo.py')):
        sys.path.append(ieodir)
        import ieo, S3ObjectStorage
    else:
        print('Error: that is not a valid path for the IEO module. Exiting.')
        sys.exit()

## main
parser = argparse.ArgumentParser('This script imports ESPA-processed scenes into the local library. It stacks images and converts them to the locally defined projection in IEO, and adds ENVI metadata.')
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
parser.add_argument('-r', '--remove', type = bool, default = True, help = 'Remove temporary files after ingest.')
parser.add_argument('--removelocal', action = 'store_true', help = 'Remove local files after ingest.')
parser.add_argument('--MGRS', type = str, default = None, help = 'Comma-delimited list of MGRS tiles to process, without any spaces. Default = 29UPU for now.')#'If missing, all default tiles will be processed for the date range.')
parser.add_argument('--startdate', type = str, default = '2015-01-01', help = 'Start date for processing in YYYY-mm-dd format. Default is 2015-01-01.')
parser.add_argument('--enddate', type = str, default = None, help = "End date for processing in YYYY-mm-dd format. If missing, today's date will be used.")
parser.add_argument('--verbose', action = 'store_true', help = 'Display more messages during execution.')
args = parser.parse_args()

picklefile = os.path.join(ieo.catdir, 'sentinel2.pickle') # contains information on data saved in buckets.
if os.path.isfile(picklefile):
    print('Loading data from Sentinel 2 pickle file.')
    with open(picklefile, 'rb') as handle:
        scenedict = pickle.load(handle)
else:
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

filelist = []
ProductDict = {}
Prodlist = []
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
startdate = datetime.datetime.strptime(args.startdate, '%Y-%m-%d')

# In case there are any errors during script execution
errorfile = 'newlandsatimport_errors_{}.csv'.format(now.strftime('%Y%m%d_%H%M%S'))
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


# Open up ieo.landsatshp and get the existing Product ID, Scene ID, and SR_path status
# driver = ogr.GetDriverByName("GPKG")
# data_source = driver.Open(ieo.catgpkg, 0)
# layer = data_source.GetLayer(ieo.landsatshp)
# for feature in layer:
#     sceneID = feature.GetField('sceneID')
#     productID = feature.GetField('LANDSAT_PRODUCT_ID_L2')
#     ProductDict[productID] = sceneID
#     scenedict[sceneID] = {'ProductID' : productID, 'sceneID' : sceneID, 'SR_path' : feature.GetField('Surface_Reflectance_tiles')}
# data_source = None

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

scenedict = S3ObjectStorage.getSentinel2scenedict(MGRStilelist, startdate = startdate, enddate = enddate, verbose = args.verbose, scenedict = scenedict)

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
for bucket in sorted(scenedict.keys()):
    if bucket != 'lastupdate':
        print(f'Now processing files in bucket {bucket}.')
        for year in sorted(scenedict[bucket].keys()):
            for month in sorted(scenedict[bucket][year].keys()):
                for day in sorted(scenedict[bucket][year][month].keys()):
                    numfiles = len(scenedict[bucket][year][month][day]['granules'])
                    if numfiles > 0:
                        
                        print(f'There are {numfiles} scenes to be processed for date {year}/{month}/{day}.')
                        filenum = 1
                        for f in scenedict[bucket][year][month][day]['granules']:
                            if f.endswith('/'):
                                f = f[:-1]
                            ProductID = os.path.basename(f)
                            satellite = ProductID[:3]
                            if args.verbose:
                                print(f)                            
                        #        try:
                            proddir = os.path.join(ieo.Sen2ingestdir, ProductID)
                            Prodlist.append(proddir)
                            if args.overwrite or not os.path.isdir(proddir):
                                print(f'\nDownloading {ProductID} from bucket {bucket}, file number {filenum} of {numfiles}.\n')
                                S3ObjectStorage.download_s3_folder(bucket, f, proddir)
                                filenum += 1
                            # This will be modified soon to process multiple Sentinel-2 tiles from the same day.
                        print(f'Now importing scenes for date {year}/{month}/{day}.')
                        tilelist = ieo.importSentinel2totiles(Prodlist, remove = args.remove, overwrite = args.overwrite)
                        if len(tilelist) > 0:
                            for tile in tilelist:
                                scenedict[bucket][year][month][day]['tiles'].append(tile)
                                
                                for d, z in zip([ieo.Sen2srdir, ieo.Sen2ndvidir, ieo.Sen2evidir, ieo.Sen2nbrdir, ieo.Sen2ndtidir], ['SR', 'NDVI', 'EVI', 'NBR', 'NDTI']):
                                    copylist = glob.glob(os.path.join(d, f'{satellite}_{year}{month}{day}_{tile}.*'))
                                    if len(copylist) > 0:
                                        remotedir = f'{z}/{tile}/{year}/{month}/{day}'
                                        S3ObjectStorage.copyfilestobucket(bucket = 'sentinel2', tagetdir = remotedir, filelist = copylist)
                                        if args.removelocal:
                                            for c in copylist:
                                                print(f'Deleting from disk: {c}')
                                                os.remove(c)
                        print('Saving metadata to pickle file.')
                        with open('filename.pickle', 'wb') as handle:
                            pickle.dump(scenedict, handle, protocol = pickle.HIGHEST_PROTOCOL)
                                    
                                
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