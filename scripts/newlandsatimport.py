#!/usr/bin/env python
# By Guy Serbin, EOanalytics Ltd.
# Talent Garden Dublin, Claremont Ave. Glasnevin, Dublin 11, Ireland
# email: guyserbin <at> eoanalytics <dot> ie

# version 1.5

# This script does the following:
# 1. Extracts ESPA-processed Landsat imagery data from tar.gz files
# 2. Virtually stacks surface reflectance (SR) and brightness temperature (BT) bands. 
# 3. Converts SR, ST, and QA band data from UTM to the local projection.
# 4. Calculates NDVI and EVI for clear land pixels
# 5. Archives tar.gz files after use

import os, sys, glob, datetime, argparse, shutil#, ieo, shutil
from osgeo import ogr

try: # This is included as the module may not properly install in Anaconda.
    import ieo
except:
    # ieodir = os.getenv('IEO_INSTALLDIR')
    # if not ieodir:
    ieodir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    # print('Error: IEO failed to load. Please input the location of the directory containing the IEO installation files.')
    # ieodir = input('IEO installation path: ')
    if os.path.isfile(os.path.join(ieodir, 'ieo.py')):
        sys.path.append(ieodir)
        import ieo
    else:
        print('Error: that is not a valid path for the IEO module. Exiting.')
        sys.exit()

## main
parser = argparse.ArgumentParser('This script imports ESPA-processed scenes into the local library. It stacks images and converts them to the locally defined projection in IEO, and adds ENVI metadata.')
parser.add_argument('-i', '--indir', default = ieo.ingestdir, type = str, help = 'Input directory to search for files. This will be overridden if --infile is set.')
parser.add_argument('-if', '--infile', type = str, help = 'Input file. This must be contain the full path and filename.')
# parser.add_argument('-f', '--fmaskdir', type = str, default = ieo.fmaskdir, help = 'Directory containing FMask cloud masks in local projection.')
parser.add_argument('-q', '--pixelqadir', type = str, default = ieo.pixelqadir, help = 'Directory containing Landsat pixel QA layers in local projection.')
parser.add_argument('--radsatqadir', type = str, default = ieo.radsatqadir, help = 'Directory containing Landsat radiometric saturation QA layers in local projection.')
parser.add_argument('--aerosolqadir', type = str, default = ieo.aerosolqadir, help = 'Directory containing Landsat aerosol QA layers in local projection.')
parser.add_argument('-o', '--outdir', type = str, default = ieo.srdir, help = 'Surface reflectance output directory')
parser.add_argument('-b', '--stoutdir', type = str, default = ieo.stdir, help = 'Surface temperature output directory')
parser.add_argument('-n', '--ndvidir', type = str, default = ieo.ndvidir, help = 'NDVI output directory')
parser.add_argument('-e', '--evidir', type = str, default = ieo.evidir, help = 'EVI output directory')
parser.add_argument('-a', '--archdir', type = str, default = ieo.archdir, help = 'Original data archive directory')
parser.add_argument('--overwrite', type = bool, default = False, help = 'Overwrite existing files.')
parser.add_argument('-d', '--delay', type = int, default = 0, help = 'Delay execution of script in seconds.')
parser.add_argument('--CalcVIs', type = bool, default = True, help = 'Calculate vegetation indices. Default = True.')
parser.add_argument('--removelocal', action = 'store_true', help = 'Remove local files after ingest.')
parser.add_argument('--noNDVI', action = 'store_true', help = 'Do not calculate NDVI.')
parser.add_argument('--noEVI', action = 'store_true', help = 'Do not calculate EVI.')
parser.add_argument('--noNDTI', action = 'store_true', help = 'Do not calculate NDTI.')
parser.add_argument('--noNBR', action = 'store_true', help = 'Do not calculate NBR.')
parser.add_argument('-r', '--remove', type = bool, default = True, help = 'Remove temporary files after ingest.')
parser.add_argument('--useS3', action = 'store_true', help = 'If set, copy outputs to S3 storage. Otherwise defaults to ieo.useS3')
args = parser.parse_args()

if args.delay > 0: # if we want to delay execution for whatever reason
    from time import sleep
    print('Delaying execution {} seconds.'.format(args.delay))
    sleep(args.delay)

if not args.useS3:
    useS3 = ieo.useS3
else:
    useS3 = args.useS3
    
# Setting a few variables
archdir = args.archdir
# fmaskdir = args.fmaskdir
# fmasklist = glob.glob(os.path.join(args.fmaskdir, '*.dat'))

reflist = []
scenedict = {}
filelist = []
ProductDict = {}
today = datetime.datetime.today()

# In case there are any errors during script execution
errorfile = 'newlandsatimport_errors_{}.csv'.format(today.strftime('%Y%m%d_%H%M%S'))
#ieo.errorfile = errorfile

def sceneidfromfilename(filename):
    basename = os.path.basename(filename)
    i = basename.find('-')
    if i > 21:
        nsceneid = basename[:i]
        sceneid = nsceneid[:2] + nsceneid[3:10]
        datetuple = datetime.datetime.strptime(nsceneid[10:18], '%Y%m%d')
        sceneid += datetuple.strftime('%Y%j')
    elif i == 21:
        sceneid = basename[:16]
    else:
        sceneid = None
    return sceneid


# Open up ieo.landsatshp and get the existing Product ID, Scene ID, and SR_path status
if not ieo.usePostGIS:
    driver = ogr.GetDriverByName("GPKG")
    data_source = driver.Open(ieo.catgpkg, 0)
else:
    data_source = ogr.Open(ieo.catgpkg, 0)
if data_source:
    layer = data_source.GetLayer(ieo.landsatshp)
    for feature in layer:
        sceneID = feature.GetField('sceneID')
        productID = feature.GetField('LANDSAT_PRODUCT_ID_L2')
        ProductDict[productID] = sceneID
        scenedict[sceneID] = {'ProductID' : productID, 'sceneID' : sceneID, 'SR_path' : feature.GetField('Surface_Reflectance_tiles')}
    data_source = None

# This look finds any existing processed data 
for dir in [args.outdir, os.path.join(args.outdir, 'L1G')]:
    rlist = glob.glob(os.path.join(args.outdir, '*_ref_{}.dat'.format(ieo.projacronym)))
    for f in rlist:
        if not 'ESA' == os.path.basename(f)[16:19]:
            reflist.append(f)

# Now create the processing list
if args.infile: # This is in case a specific file has been selected for processing
    if os.access(args.infile, os.F_OK) and (args.infile.endswith('.tar.gz') or args.infile.endswith('.tar')):
        print('File has been found, processing.')
        filelist.append(args.infile)
    else:
        print('Error, file not found: {}'.format(args.infile))
        ieo.logerror(args.infile, 'File not found.')
else: # find and process what's in the ingest directory
    for root, dirs, files in os.walk(args.indir, onerror = None): 
        for name in files:
            # if name.endswith('.tar'):
            #     print(name)
            i = name.find('.tar')
            if name[:i] in ProductDict.keys():
                
            # if name.endswith('.tar.gz') or name.endswith('.tar') or name.endswith('_sr_band7.img'):
                fname = os.path.join(root, name)
                ssceneID = ProductDict[name[:i]]
                # ssceneID = sceneidfromfilename(name)
                if ssceneID:
                    sslist = [x for x in scenedict.keys() if ssceneID in x]
                    if len(sslist) > 0:
                        for sceneID in sslist:
                            if (args.overwrite or not any(ssceneID in x for x in reflist)) and (not fname in filelist): # any(scenedict[ProductID]['sceneID'][:16] == os.path.basename(x)[:16] for x in reflist)
                                print('Found unprocessed SceneID {}, adding to processing list.'.format(sceneID))
                                filelist.append(fname)

# Now process files that are in the list
numfiles = len(filelist)
print('There are {} reflectance files and {} scenes to be processed.'.format(len(reflist), numfiles))
filenum = 1
for f in filelist:
    basename = os.path.basename(f)
    scene = basename[:16]
    if args.overwrite or not any(scene in x for x in reflist):
#        try:
        print('\nProcessing archive {}, file number {} of {}.\n'.format(f, filenum, numfiles))
        ieo.importespatotiles(f, remove = args.remove, useS3 = useS3, overwrite = args.overwrite)
        if args.removelocal:
            localdirs = glob.glob(f'{f[:-4]}*')
            if len(localdirs) > 0:
                for d in localdirs:
                    if os.path.isdir(d):
                        print(f'Deleting temporary directory: {d}')
                        shutil.rmtree(d)
#        except Exception as e:
#            print('There was a problem processing the scene. Adding to error list.')
#            exc_type, exc_obj, exc_tb = sys.exc_info()
#            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
#            print(exc_type, fname, exc_tb.tb_lineno)
#            print(e)
#            ieo.logerror(f, '{} {} {}'.format(exc_type, fname, exc_tb.tb_lineno))
    else:
        print('Scene {} has already been processed, skipping file number {} of {}.'.format(scene, filenum, numfiles))
    filenum += 1

print('Processing complete.')