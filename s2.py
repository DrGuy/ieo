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
    
def reproject(in_raster, out_raster, *args, **kwargs): # Converts raster to local projection
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

