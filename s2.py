#/usr/bin/python
# By Guy Serbin, EOanalytics Ltd.
# Talent Garden Dublin, Claremont Ave. Glasnevin, Dublin 11, Ireland
# email: guyserbin <at> eoanalytics <dot> ie

# Irish Earth Observation (IEO) Python Module
# version 2.0

# This contains code borrowed from the Python GDAL/OGR Cookbook: https://pcjericks.github.io/py-gdalogr-cookbook/

import os, datetime, time, shutil, sys, glob, csv, ENVIfile, numpy, numexpr
from xml.dom import minidom
from subprocess import Popen
from pkg_resources import resource_stream, resource_string, resource_filename, Requirement
from ENVIfile import *

# Import GDAL
# if not 'linux' in sys.platform: # this way I can use the same library for processing on multiple systems
    # if sys.version_info[0] !=3: # Attempt to load ArcPy and EnviPy libraries, if not, use GDAL.
    #     try:
    #         from arcenvipy import *
    #     except:
    #         print('There was an error loading either ArcPy or EnviPy. Functions requiring this library will not be available.')
from osgeo import gdal, ogr, osr

# else: # Note- this hasn't been used or tested with Linux in a long time. It probably doesn't work.
#     import gdal, ogr, osr
#     sys.path.append('/usr/bin')
#     sys.path.append('/usr/local/bin')

# Set some global variables
global pixelqadir, aerosolqadir, radsatqadir, srdir, stdir, ingestdir, ndvidir, evidir, archdir, catdir, logdir, NTS, Sen2tiles, prjstr, WRS1, WRS2, defaulterrorfile, gdb_path, landsatshp, prj, projacronym, useProductID

# configuration data
if sys.version_info[0] == 2:
    import ConfigParser as configparser
else:
    import configparser

pythondir = os.path.dirname(sys.executable)

# Access configuration data inside Python egg
config = configparser.ConfigParser()
# ieoconfigdir = os.getenv('IEO_CONFIGDIR')
# if ieoconfigdir:
#     configfile = os.path.join(ieoconfigdir, 'ieo.ini')
# else:
configfile = 'config/ieo.ini'
config_location = resource_filename(Requirement.parse('ieo'), configfile)
config.read(config_location) # config_path
# fmaskdir = config['DEFAULT']['fmaskdir'] # Deprecated in version 1.5
# pixelqadir = config['DEFAULT']['pixelqadir']
# radsatqadir = config['DEFAULT']['radsatqadir']
# aerosolqadir = config['DEFAULT']['aerosolqadir']
# stdir = config['DEFAULT']['stdir'] # Surface Temperature 
srdir = config['DEFAULT']['srdir'] # Surface Reflectance 

# btdir = config['DEFAULT']['btdir'] #Brightness temperature, deprecated in version 1.5
ingestdir = config['DEFAULT']['ingestdir']
# ndvidir = config['DEFAULT']['ndvidir']
# evidir = config['DEFAULT']['evidir']
catdir = config['DEFAULT']['catdir']
archdir = config['DEFAULT']['archdir']
logdir = config['DEFAULT']['logdir']
# useProductID = config['DEFAULT']['useProductID']
prjstr = config['Projection']['proj']
projacronym = config['Projection']['projacronym']
ieogpkg = os.path.join(catdir, config['VECTOR']['ieogpkg'])
# WRS1 = config['VECTOR']['WRS1'] # WRS-1, Landsats 1-3
# WRS2 = config['VECTOR']['WRS2'] # WRS-2, Landsats 4-8
NTS = config['VECTOR']['nationaltilesystem'] # For Ireland, the All-Ireland Raster Tile (AIRT) tile polygon layer
Sen2tiles = config['VECTOR']['Sen2tiles'] # Sentinel-2 tiles for Ireland
catgpkg = os.path.join(catdir, config['catalog']['catgpkg'])
# landsatshp = config['catalog']['landsat']

Sen2srdir = config['Sentinel2']['srdir'] # Surface Reflectance 
Sen2ndvidir = config['Sentinel2']['ndvidir']
Sen2evidir = config['Sentinel2']['evidir']
Sen2ingestdir = config['Sentinel2']['ingestdir']

queryURL = 'https://catalog-browse.default.mundiwebservices.com/acdc/catalog/proxy/search/Sentinel2/opensearch'

useS3 = config['S3']['useS3'] 
useS3 = False
if useS3 == 'Yes':
    tempprocdir = config['DEFAULT']['tempprocdir']
    useS3 = True
    archivebucket = config['S3']['archivebucket']
    Sentinel2bucket = config['S3']['landsatdata']
    from S3ObjectStorage import *
else:
    tempprocdir = None
    useS3 = False

S2dict = {'driver' : 'SENTINEL2',
          '10m' : ['2', '3', '4', '8'],
          '20m' : ['5', '6', '7', '8a', '11', '12'],
          '60m' : ['1', '9'],
          'qbands' :['AOT', 'CLD', 'SCL', 'SNW', 'WVP'],
          'alt' : ['08a'],
          }    


def CreateOpenSearchQueryURL(*args, **kwargs):
    startdate = kwargs.get('startdate', '2017-04-01')
    enddate = kwargs.get('enddate', 'NOW')
    url = kwargs.get('url', queryURL)
    cloudcover = kwargs.get('cloudcover', 0.7)
    onlinestatus = kwargs.get('onlinestatus', 'ONLINE')
    xcoord = kwargs.get('xcoord', None)
    ycoord = kwargs.get('ycoord', None)
    point = kwargs.get('point', None) # [X, Y]
    footprint = kwargs.get('footprint', None) # this is a list of two coordinates- UL and LR [[ULX, ULY], [LRX, LRY]]
    polygon = kwargs.get('polygon', None) # this is a list, end point is the same value as starting point
    if xcoord and ycoord:
        if not point:
            point = [xcoord, ycoord]
    
    if enddate != 'NOW':
        sensingEndDate = f'{enddate}T23:59:59Z'
    else:
        sensingEndDate = 'NOW'
    if startdate:
        sensingStartDate = f'{startdate}T00:00:00Z'
    # qstr = f'q=(sensingStartDate[{sensingStartDate} TO {sensingEndDate}])'
    
    params = {'platform' : 'Sentinel2',
              'instrument' : 'MSI',
              'processingLevel' : 'L2A',
              'onlineStatus' : onlinestatus,
              'cloudCover' : cloudcover}
    if footprint:
        params['bbox'] = '{},{},{},{}'.format(footprint[0][0], footprint[0][1], footprint[1][0], footprint[1][1])
    elif polygon:
        params['geometry'] = 'POLYGON(('
        for coord in polygon:
            if polygon.index(coord) != len(polygon) -1: 
                params['geometry'] += '{} {}, '.format(coord[0], coord[1])
            else:
                params['geometry'] += '{} {}))'.format(coord[0], coord[1])
    elif point:
        params['geometry'] = 'POINT({} {})'.format(coord[0], coord[1])
    
    urlstr = f'{url}?' #'{qstr}'
    for key in params.keys():
        if urlstr.endswith('?'):
            urlstr += '{}={}'.format(key, params[key])
        else:
            urlstr += '&{}={}'.format(key, params[key])
    return urlstr
    

def reproject(in_raster, out_raster, band, *args, **kwargs): # Converts raster to local projection
    rastertype = kwargs.get('rastertype', None)
    # landsat = kwargs.get('landsat', None) # Not currently used
    sceneid = kwargs.get('sceneid', None)
    outdir = kwargs.get('outdir', None)
    rewriteheader = kwargs.get('rewriteheader', True)
    parentrasters = kwargs.get('parentrasters', None)
    if os.access(in_raster, os.F_OK):
        src_ds = gdal.Open(in_raster)
        gt = src_ds.GetGeoTransform()
        # Close datasets
        src_ds = None
        p = Popen(['gdalwarp', '-t_srs', prjstr, '-tr', str(abs(gt[1])), str(abs(gt[5])), '-of', 'ENVI', in_raster, out_raster])
        print(p.communicate())
        if rewriteheader:
            if isinstance(parentrasters, list):
                parentrasters = makeparentrastersstring(parentrasters)
            ENVIfile(out_raster, rastertype, SceneID = sceneid, outdir = outdir, parentrasters = parentrasters).WriteHeader()

def WarpMGRS(dirname, *args, **kwargs):
    # This function imports new ESPA-process LEDAPS data
    # Version 1.5: Landsat Collection 2 Level 2 data now supported, AWS S3 
    #              object storage
    
    basename = os.path.basename(dirname)
    ProductID = basename
    parts = basename.split('_')
    satellite = parts[0]
    datestr = parts[2][:8]
    EPSGstr = 'EPSG:326{}'.format(parts[5][1:3])
    f = os.path.join(dirname, 'MTD_MSIL2A.xml')
    
    outputdir = f'{dirname}_{projacronym}'
    projdir = os.path.join(outputdir, projacronym)
    if not os.path.isdir(projdir):
        os.makedirs(projdir)
    for sds in ['10m', '20m', '60m']:
        sdsname = f'SENTINEL2_L2A:MTD_MSIL2A.xml:{sds}:{EPSGstr}'
        print(f'Opening: {sdsname}')
        ds = gdal.Open(os.path.join(dirname, sds))
        for bandname in S2dict[sds]:
            
            if bandname == '2' and sds == '10m':
                gt = ds.GetGeoTransform()
                xRes = gt[0]
                yRes = -gt[4]
            bandnum = S2dict[sds].index(bandname) + 1
            if sds == '10m':
                print(f'Now extracting band {bandname} .')
            else:
                print(f'Now extracting band {bandname} at 10m spatial resolution.')
            outputfile = os.path.join(outputdir, f'{ProductID}_B{bandname}.dat')
            gdal.Translate(outputfile, ds, xRes = xRes, yRes = yRes, resampleAlg = "bilinear", bandList = [bandnum])
    bandlist = ['1', '2', '3', '4', '5', '6', '7', '8', '8a', '9', '11', '12']    
    srlist = []
    out_vrt = os.path.join(outputdir, '{}.vrt'.format(ProductID))  
    if not os.path.isfile(out_vrt):
        
        for band in bandlist:
            fb = os.path.join(outputdir, f'{ProductID}_B{band}.dat')
            
            srlist.append(fb)
    print('Stacking bands in a VRT.')
    gdal.BuildVRT(out_vrt, srlist, separate = True)
        
        
    options = gdal.WarpOptions(format = 'ENVI', dstSRS = prjstr,
                                  resampleAlg = 'bilinear')
    outputfile = os.path.join(projdir, f'{ProductID}.dat')
    gdal.Warp(outputfile, out_vrt, options = options)
    return outputfile, datestr, satellite
   
def importSentinel2totiles(dirname, *args, **kwargs): 
    overwrite = kwargs.get('overwrite', False)
    noupdate = kwargs.get('noupdate', False)
    tempdir = kwargs.get('tempdir', None)
    remove = kwargs.get('remove', False)
    S3tarfilepath = kwargs.get('S3tarfilepath', 'Sentinel2') # This is for archiving input files after ingestion only.
    S3tarfilepath = kwargs.get('S3tarfilebucket', 'ingested') # This is for archiving input files after ingestion only.         
    ProductID = os.path.basename(dirname)
    # projection = prj.GetAttrValue('projcs')
    
    tfile, datestr, satellite = WarpMGRS(dirname)
    tdir = os.path.dirname(tfile)
        
    # if any(x.endswith('.tif') for x in filelist):
    #     ext = 'tif'
    # else:
    #     ext = 'img'
    # xml = glob.glob(os.path.join(outputdir, '*.xml'))
    # if len(xml) > 0:
    #     ProductID = os.path.basename(xml[0]).replace('.xml', '') # Modified from sceneID in 1.1.1: sceneID will now be read from landsatshp
    # elif basename[:1] == 'L' and len(basename) > 40:
    #     ProductID = basename[:40]
    # else:
    #     print('No XML file found, returning.')
    #     logerror(f, 'No XML file found.')
    #     return

    # open landsat shapefile (starting version 1.1.1)
    
    # Surface reflectance data
    
    feat = converttotiles(tfile, Sen2srdir, 'Sentinel-2', pixelqa = True, overwrite = overwrite, feature = None, noupdate = noupdate)
    feat = None
#        if feat.GetField('BT_path') != BT_ITM:
#            feat.SetField('BT_path', BT_ITM)

    # Calculate EVI and NDVI
    print('Processing vegetation indices.')
    evibasefile = '{}_EVI.dat'.format(ProductID)
    
    Sen2evifile = os.path.join(tdir, evibasefile)
    Sen2ndvifile = os.path.join(tdir, evibasefile.replace('_EVI', '_NDVI'))
    if not os.path.isfile(evifile):
        try:
            calcvis(tfile, qafile = qafile)
            feat = converttotiles(ndvifile, Sen2ndvidir, 'NDVI', pixelqa = False, feature = feat, overwrite = overwrite, noupdate = noupdate)
            # layer.SetFeature(feat)
            feat = converttotiles(evifile, Sen2evidir, 'EVI', pixelqa = False, feature = feat, overwrite = overwrite, noupdate = noupdate)
            # layer.SetFeature(feat)
        except Exception as e:
            print('An error has occurred calculating VIs for scene {}:'.format(sceneid))
            print(e)
            logerror(tfile, e)
#    if os.path.isfile(evifile) and feat.GetField('EVI_path') != evifile:
#        feat.SetField('EVI_path', evifile)
#    if os.path.isfile(ndvifile) and feat.GetField('NDVI_path') != ndvifile:
#        feat.SetField('NDVI_path', ndvifile)

    # Set feature in shapefile to preserve processed file metadata
#     print('Updating information in shapefile.')
# #    layer.SetFeature(feat)
#     layer.CommitTransaction()
#     data_source = None # Close the shapefile

    # Clean up files.

    # if basename.endswith('.tar.gz') or basename.endswith('.tar'): # Move input tarfile to archive location 
    #     if useS3: # Archive to S3 object storage
    #         year = sceneid[9:13]
    #         targetdir = '{}/{}/{}'.format(S3tarfilebucket, S3tarfilebucket, year)
    #         print('Moving {} to S3 object storage bucket: {}'.format(basename, targetdir))
    #         copyfilestobucket(filename = f, bucket= S3tarfilebucket, targetdir = targetdir)
    #         os.remove(f)
    #     else: # archive to archdir
    #         print('Moving {} to archive: {}'.format(basename, archdir))
    #         if not os.access(os.path.join(archdir, os.path.basename(f)), os.F_OK):
    #             shutil.move(f, archdir)
    if remove:
        print('Cleaning up files in directory.')
        shutil.rmtree(outputdir)
        shutil.rmtree(dirname)
        # for d in [tdir, outputdir]:
        #     filelist = glob.glob(os.path.join(d, '*.*'))
        #     try:
        #         for fname in filelist:
        #             if os.access(fname, os.F_OK):
        #                 os.remove(fname)
        #         os.rmdir(d)
        #     except Exception as e:
        #         print('An error has occurred cleaning up files for scene {}:'.format(sceneid))
        #         print(e)
        #         logerror(f, e)

    print('Processing complete for scene {}.'.format(ProductID))
    
