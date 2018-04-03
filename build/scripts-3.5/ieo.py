#/usr/bin/python
# By Guy Serbin, Environment, Soils, and Land Use Dept., CELUP, Teagasc,
# Johnstown Castle, Co. Wexford Y35 TC97, Ireland
# email: guy <dot> serbin <at> teagasc <dot> ie

# Irish Earth Observation (IEO) Python Module
# version 1.1.1 

# This contain code borrowed from the Python GDAL/OGR Cookbook: https://pcjericks.github.io/py-gdalogr-cookbook/

import os, datetime, time, shutil, sys, glob, csv, ENVIfile, numpy, numexpr
from xml.dom import minidom
from subprocess import Popen
from pkg_resources import resource_stream, resource_string, resource_filename, Requirement
from ENVIfile import *

# Import GDAL
if not 'linux' in sys.platform: # this way I can use the same library for processing on multiple systems
    # if sys.version_info[0] !=3: # Attempt to load ArcPy and EnviPy libraries, if not, use GDAL.
    #     try:
    #         from arcenvipy import *
    #     except:
    #         print('There was an error loading either ArcPy or EnviPy. Functions requiring this library will not be available.')
    from osgeo import gdal, ogr, osr

else: # Note- this hasn't been used or tested with Linux in a long time. It probably doesn't work.
    import gdal, ogr, osr
    sys.path.append('/usr/bin')
    sys.path.append('/usr/local/bin')

# Set some global variables
global fmaskdir, srdir, btdir, ingestdir, ndvidir, evidir, archdir, catdir, logdir, NTS, Sen2tiles, prjstr, WRS1, WRS2, defaulterrorfile, gdb_path, landsatshp, prj, projacronym, useProductID

# configuration data
if sys.version_info[0] == 2:
    import ConfigParser as configparser
else:
    import configparser


# Access configuration data inside Python egg
config = configparser.ConfigParser()
config_location = resource_filename(Requirement.parse('ieo'), 'config/ieo.ini')
config.read(config_location) # config_path
fmaskdir = config['DEFAULT']['fmaskdir'] # support for fmask may be removed in future versions
pixelqadir = config['DEFAULT']['pixelqadir']
srdir = config['DEFAULT']['srdir']
btdir = config['DEFAULT']['btdir']
ingestdir = config['DEFAULT']['ingestdir']
ndvidir = config['DEFAULT']['ndvidir']
evidir = config['DEFAULT']['evidir']
catdir = config['DEFAULT']['catdir']
archdir = config['DEFAULT']['archdir']
logdir = config['DEFAULT']['logdir']
useProductID = config['DEFAULT']['useProductID']
prjstr = config['Projection']['proj']
projacronym = config['Projection']['projacronym']
landsatshp = os.path.join(catdir, 'Landsat', config['VECTOR']['landsatshp'])
# gdb_path = os.path.join(catdir, config['DEFAULT']['GDBname'])
WRS1 = os.path.join(catdir, 'shapefiles', config['VECTOR']['WRS1']) # WRS-1, Landsats 1-3
WRS2 = os.path.join(catdir, 'shapefiles', config['VECTOR']['WRS2']) # WRS-2, Landsats 4-8
NTS = os.path.join(catdir, 'shapefiles', config['VECTOR']['nationaltilesystem']) # For Ireland, the All-Ireland Raster Tile (AIRT) tile polygon layer
Sen2tiles = os.path.join(catdir, 'shapefiles', config['VECTOR']['Sen2tiles']) # Sentinel-2 tiles for Ireland
defaulterrorfile = os.path.join(logdir, 'errors.csv')
badlandsat = os.path.join(catdir, 'Landsat', 'badlist.txt')

if useProductID.lower() == 'yes' or useProductID.lower() =='y': # change useProductID to Boolean
    useProductID = True
else:
    useProductID = False

# Configuration data for maskfromqa()
global qaland, qawater, qasnow, qashadow, qausemedcloud, qausemedcirrus, qausehighcirrus, qauseterrainocclusion
qaland = True # Include land pixels
qawater = False # Include water pixels
qasnow = False # Include snow/ice pixels
qashadow = False # Include cloud shadowed pixels
qausemedcloud = False # Allow medium confidence cloud pixels to be treated as clear
qausemedcirrus = True # Allow medium confidence cirrus pixels to be treated as clear
qausehighcirrus = True # Allow high confidence cirrus pixels to be treated as clear
qauseterrainocclusion = False # Allow terrain-occluded pixels to be treated as clear

if ':' in prjstr:
    i = prjstr.find(':') + 1
    prjstrval = prjstr[i:]
prj = osr.SpatialReference()
prj.ImportFromEPSG(int(prjstrval))

## Some functions

def logerror(f, message, *args, **kwargs):
    # This function logs errors to an error file.
    errorfile = kwargs.get('errorfile', defaulterrorfile) 
    dirname, basename = os.path.split(errorfile)
    if not os.path.isdir(dirname):
        errorfile = os.path.join(logdir, basename)
    if not os.path.exists(errorfile):
        with open(errorfile,'w') as output:
            output.write('Time, File, Error\n')
    now = datetime.datetime.now()
    with open(errorfile, 'a') as output:
        output.write('%s, %s, %s\n'%(now.strftime('%Y-%m-%d %H:%M:%S'), f, message))
    

def extract_xml(s):
    # This function extracts metadata from XML files
    '''Take a string like
    "<tag>         something          </tag>"
    drop the opening and closing tags and leading or trailing whitespace.'''
    start = s.index(">") + 1
    end  = s.rindex("<")
    s = str (s[start:end])
    return (s.strip())

def get_landsat_fileparams(filename):
    # This function gets Landsat scene parameters from scene IDs
    filename = os.path.basename(filename)
    if '_' in filename: # Extract data from MTL filename
        fileparts = filename.split('_')
        if len(fileparts) != 2:  # This is for old format tar.gz MTL files from the USGS that included the acquisition date in YYYYMMDD format.
            b1, b2 = fileparts[0], fileparts[1]
            landsat, path, row = b1[1:2], b1[-6:-3], b1[-3:]
            year, yearmonthday = b2[-8:-4], b2[-8:]
            datetuple = time.strptime(yearmonthday, "%Y%m%d")
            julian = time.strftime("%j", datetuple)
    else:    # This will work with a simple scene ID
        landsat, path, row, year, julian = filename[2:3], filename[3:6], filename[6:9], filename[9:13], filename[13:16]
        datetuple = time.strptime(year+julian,'%Y%j')
        yearmonthday = time.strftime('%Y%m%d', datetuple)
    return landsat, path, row, year, julian, yearmonthday

def makegrid(*args, **kwargs):
    import string
    minX = kwargs.get('minX', float(config['makegrid']['minX']))
    minY = kwargs.get('minY', float(config['makegrid']['minY']))
    maxX = kwargs.get('maxX', float(config['makegrid']['maxX']))
    maxY = kwargs.get('maxY', float(config['makegrid']['maxY']))
    xtiles = kwargs.get('xtiles', float(config['makegrid']['xtiles']))
    ytiles = kwargs.get('ytiles', float(config['makegrid']['ytiles']))
    outfile = kwargs.get('outfile', os.path.join(catdir, '{}.shp'.format(float(config['VECTOR']['nationaltilesystem']))))
    inshp = kwargs.get('inshape', None)
    projection = kwargs.get('prj', prjstr)
    overwrite = kwargs.get('overwrite', False)
#    if ytiles > 99: # max number of ytiles supported is 999, likely easy fix for more
#        ytilestr = '{:03d}'
#    elif ytiles > 9:
#        ytilestr = '{:02d}'
#    else:
#        ytilestr = '{}'
    ytilestr = '{}{}{}'.format('{:0', len(str(ytiles)), 'd}') # limit for ytiles removed
    
    if overwrite:
        flist = glob.glob(outfile.replace('.shp', '.*'))
        for f in flist:
            os.remove(f)
    
   # determine tile sizes
    dx = (maxX - minX) / xtiles
    dy = (maxY - minY) / ytiles
    
    # set up the shapefile driver
    driver = ogr.GetDriverByName("ESRI Shapefile")
    
    # Get input shapefile
    inDataSource = driver.Open(inshp, 0)
    inLayer = inDataSource.GetLayer()
    feat = inLayer.GetNextFeature()
    infeat = feat.GetGeometryRef()
    
    # create the data source
    if os.path.exists(outfile):
        os.remove(outfile)
    data_source = driver.CreateDataSource(outfile)
    
    # create the layer
    layer = data_source.CreateLayer("Tiles", projection, ogr.wkbPolygon)
    
    # Add fields
    field_name = ogr.FieldDefn("Tile", ogr.OFTString)
    field_name.SetWidth(2)
    layer.CreateField(field_name)
    
    # create the tiles
    h = 0
    i1 = 0
    for i in range(xtiles):
        for j in range(ytiles):
            if xtiles > 26:
                tilename = ('{}{}' + ytilestr).format(string.ascii_uppercase[h], string.ascii_uppercase[i1], j + 1)
                
#            elif ytiles > 9:
#                tilename = '{}{:02d}'.format(string.ascii_uppercase[i], j + 1)
            else:
                tilename = ('{}' + ytilestr).format(string.ascii_uppercase[i], j + 1)
            # if xtiles == 4 and ytiles ==5 and tilename != 'A5':
            mx = minX + i * dx
            X = mx + dx
            my = minY + j * dy
            Y = my + dy
            ring = ogr.Geometry(ogr.wkbLinearRing)
            ring.AddPoint(mx, my)
            ring.AddPoint(mx, Y)
            ring.AddPoint(X, Y)
            ring.AddPoint(X, my)
            ring.AddPoint(mx, my)
            # Create polygon
            poly = ogr.Geometry(ogr.wkbPolygon)
            poly.AddGeometry(ring)
            # add new geom to layer if it intersects Ireland shapefile
            p = infeat.Intersect(poly)
    #            print(p)
            if p:
                outFeature = ogr.Feature(layer.GetLayerDefn())
                outFeature.SetGeometry(poly)
                outFeature.SetField('Tile', tilename)
                layer.CreateFeature(outFeature)
                outFeature.Destroy
        if i1 >= 25:
            h += 1
            i1 = 0
        else:    
            i1 += 1
    
    # Create ESRI.prj file
    spatialRef = osr.SpatialReference()
    i = projection.find(':') + 1
    spatialRef.ImportFromEPSG(int(projection[i:]))
    spatialRef.MorphToESRI()
    with open(outfile.replace('.shp', '.prj'), 'w') as output:
        output.write(spatialRef.ExportToWkt())

    data_source = None
    inDataSource = None


## Ireland specific functions    

def reproject(in_raster, out_raster, *args, **kwargs): # Converts raster to local projection
    rastertype = kwargs.get('rastertype', None)
    landsat = kwargs.get('landsat', None) # Not currently used
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
            ENVIfile(out_raster, rastertype, SceneID = sceneid, outdir = outdir, parentrasters = parentrasters).WriteHeader()

def checkscenelocation(scene, dst = 50.0): # This function assesses geolocation accuracy of scenes warped to local grid
    misplaced = False # Boolean value for whether scene centre fits in acceptable tolerances  
    basename = os.path.basename(scene)
    if 'lndsr.' in basename:
        basename.replace('lndsr.', '')
    if int(basename[2:3]) < 4: # Determine WRS type, Path, and Row
        WRS = 1
        polygon = WRS1
    else:
        WRS = 2
        polygon = WRS2
    path = int(basename[3:6])
    row = int(basename[7:9])
    # Get scene centre coordinates
    print('Checking scene centre location accuracy for {} centre to within {:0.1f} km of WRS-{} Path {} Row {} standard footprint centre.'.format(basename, dst, WRS, path, row))
    
    src_ds = gdal.Open(scene)
    geoTrans = src_ds.GetGeoTransform()
    latmax = geoTrans[3]
    latmin = latmax + geoTrans[5] * float(src_ds.RasterYSize)
    longmin = geoTrans[0]
    longmax = longmin + geoTrans[1] * float(src_ds.RasterXSize)
    src_ds = None
    X = (float(longmax.getOutput(0)) + float(longmin.getOutput(0))) / 2.
    Y = (float(latmax.getOutput(0)) + float(latmin.getOutput(0))) / 2.
    print('{} scene centre coordinates are {:0.1f} E, {:0.1f} N.'.format(projacronym, X, Y))
    
    driver = ogr.GetDriverByName("ESRI Shapefile")
#    gdb, wrs = os.path.split(polygon)
    ds = driver.Open(polygon, 0)
    layer = ds.Getlayer()
    found = False
    while not found:
        feature = layer.GetNextFeature()
        items = feature.items()
        if path == items['PATH'] and row == items['ROW']:
            geometry = feature.geometry()
            envelope = geometry.GetEnvelope()
            wX = (envelope[0] + envelope[1]) / 2.
            wY = (envelope[2] + envelope[3]) / 2.
            found = True
    ds = None
    
    print('{} standard WRS-{} footprint centre coordinates are {:0.1f} E, {:0.1f} N.'.format(projacronym, WRS, wX, wY))
    offset = (((X - wX) ** 2 + ( Y - wY) ** 2) ** 0.5) / 1000 # determine distance in km between scene and standard footprint centres
    print('Offset = {:0.1f} km out of maximum distance of {:0.1f} km.'.format(offset, dst))
    if dst >= offset:
        print('Scene {} is appropriately placed, and is {:0.1f} km from the standard WRS-{} scene footprint centre.'.format(basename, offset, WRS))
    else:
        print('Scene {} is improperly located, and is {:0.1f} km from the standard WRS-{} scene footprint centre.'.format(basename, offset, WRS))
        misplaced = True
    return misplaced, offset
    

## Landsat import and VI calculation functions

def envihdracqtime(hdr):
    # This function extracts the acquisition time from an ENVI header file
    acqtime = None
    with open(hdr, 'r') as lines:
        for line in lines:
            if line.startswith('acquisition time'):
                acqtime = line
    return acqtime
    

def maskfromqa(qafile, landsat, sceneid, *args, **kwargs):
    # Added in version 1.1.1. This recreates a processing mask layer to memory using the pixel_qa layer. It does not save to disk.
    land = kwargs.get('land', qaland) # Include land pixels
    water = kwargs.get('water', qawater) # Include water pixels
    snow = kwargs.get('snowice', qasnow) # Include snow/ice pixels
    shadow = kwargs.get('shadow', qashadow) # Include cloud shadowed pixels
    usemedcloud = kwargs.get('usemedcloud', qausemedcloud) # Allow medium confidence cloud pixels to be treated as clear
    usemedcirrus = kwargs.get('usemedcirrus', qausemedcirrus) # Allow medium confidence cirrus pixels to be treated as clear
    usehighcirrus = kwargs.get('usehighcirrus', qausehighcirrus) # Allow high confidence cirrus pixels to be treated as clear
    useterrainocclusion = kwargs.get('useterrainocclusion', qauseterrainocclusion) # Allow terrain-occluded pixels to be treated as clear
    
    if usehighcirrus:
        usemedcirrus = True
    
    # Create list of pixel value that will be used for the good data mask (bit data baased upon USGS/EROS LEAPS/ LaSRC Product Guides)
    bitinfo = ['Fill', 'Clear', 'Water', 'Shadow', 'Snow', 'Cloud', 'No/ low cloud', 'med/ high cloud', 'No/ low cirrus', 'med/ high cirrus', 'Terrain occlusion']
    includevals = []
    baseL47 = 64 # Bit 6 always set to 1 unless bit 7 set to 1
    baseL8 = 64 + 256 # Bits 6, 8 always set to 1 unless bits 7, 9 set to 1, respectively
    for x, y in zip([land, water, shadow, snow], bitinfo[1:4]):
        if x:
            if landsat >= 8:
                includevals.append(2 ** bitinfo.index(y) + baseL8)
                if usemedcloud:
                    includevals.append(2 ** bitinfo.index(y) + baseL8 + 64)
                if usehighcirrus:
                    includevals.append(2 ** bitinfo.index(y) + baseL8 + 512)
                if usemedcirrus:
                    includevals.append(2 ** bitinfo.index(y) + baseL8 + 256)
                if useterrainocclusion:
                    includevals.append(2 ** bitinfo.index(y) + baseL8 + 1024)
            else:
                includevals.append(2 ** bitinfo.index(y) + baseL47)
                if usemedcloud:
                    includevals.append(2 ** bitinfo.index(y) + baseL47 + 64)
    
    # Open Pixel QA file
    print('Opening Pixel QA layer for scene {}.'.format(sceneid))
    qaobj = gdal.Open(qafile)
    qalayer = qaobj.GetRasterBand(1).ReadAsArray()
    # Get file geometry
    ns = qaobj.RasterXSize
    nl = qaobj.RasterYSize    
    
    # Create mask of zero values
    mask = numpy.zeros((nl, ns), dtype = numpy.uint8)
    if len(includevals) > 0:
        for val in includevals:
            maskvals = numexpr.evaluate('(qalayer == val)')
            mask[maskvals] = 1
    
    maskvals = None
    qalayer = None
    qaobj = None
    return mask
    

def calcvis(refitm, *args, **kwargs): # This should calculate a masked NDVI.
    # This function creates NDVI and EVI files.
    useqamask = kwargs.get('useqamask', True)
    usefmask = kwargs.get('usefmask', False)
    usecfmask = kwargs.get('usecfmask', False)
    dirname, basename = os.path.split(refitm)
    i = basename.find('_ref_')
    sceneid = basename[:i] # This will now use either the SceneID or ProductID
    acqtime = envihdracqtime(refitm.replace('.dat', '.hdr'))
    qafile = os.path.join(pixelqadir,'{}_pixel_qa.dat'.format(sceneid))
    fmaskfile = os.path.join(fmaskdir,'{}_cfmask.dat'.format(sceneid))
    parentrasters = [os.path.basename(refitm)]
    if useqamask:
        if not os.path.isfile(qafile):
            usefmask = False
            usecfmask = False
    if usefmask or usecfmask:
        usefmask = True
        if not os.path.isfile(fmaskfile):
            fmaskfile = fmaskfile.replace('_cfmask.dat', '_fmask.dat')
            if not os.path.exists(fmaskfile):
                print('ERROR: Fmask file does not exist, returning.')
                logerror(fmaskfile, 'File not found.')
                usefmask = False
            else:
                parentrasters.append(os.path.basename(fmaskfile))
    
    print('Calculating NDVI for scene {}.'.format(sceneid))
    refobj = gdal.Open(refitm)
    
    # Get file geometry
    geoTrans = refobj.GetGeoTransform()
    ns = refobj.RasterXSize
    nl = refobj.RasterYSize    
    if useqamask:
        if sceneid[2:3] == '0':
            landsat = int(sceneid[3:4])
        else:
            landsat = int(sceneid[2:3])
        fmask = maskfromqa(qafile, landsat, sceneid)
    elif usefmask:
        fmaskobj = gdal.Open(fmaskfile)
        fmaskdata = fmaskobj.GetRasterBand(1).ReadAsArray()
        fmask = numpy.zeros((nl, ns), dtype = numpy.uint8)
        maskvals = numexpr.evaluate('(fmaskdata == 0)')
        fmask[maskvals] = 1
        fmaskdata = None
        maskvals = None
    else: 
        print('Warning: No Fmask file found for scene {}.'.format(sceneid))
        fmask = None
    if basename[2:3] == '8':
        NIR = refobj.GetRasterBand(5).ReadAsArray()
        red = refobj.GetRasterBand(4).ReadAsArray()
        blue = refobj.GetRasterBand(2).ReadAsArray()
    else:
        NIR = refobj.GetRasterBand(4).ReadAsArray()
        red = refobj.GetRasterBand(3).ReadAsArray()
        blue = refobj.GetRasterBand(1).ReadAsArray()
    
    # NDVI calculation 
    NDVI = NDindex(NIR, red, fmask = fmask)
    ENVIfile(NDVI, 'NDVI', outdir = ndvidir, geoTrans = geoTrans, SceneID = sceneid, acqtime = acqtime, parentrasters =  parentrasters).Save()
    NDVI = None
    
    # EVI calculation
    evi = EVI(blue, red, NIR, fmask = fmask)
    ENVIfile(evi, 'EVI', outdir = evidir, geoTrans = geoTrans, SceneID = sceneid, acqtime = acqtime, parentrasters =  parentrasters).Save()
    evi = None
    
    NIR = None
    red = None
    refobj = None
    fmask = None
    fmaskobj = None

def EVI(blue, red, NIR, *args, **kwargs):
    # This calculates a 2 dimensional array consisting of Enhanced Vegetation Index values
    fmask = kwargs.get('fmask', None)
    if not isinstance(fmask, numpy.ndarray):
        mask = numexpr.evaluate('(NIR < 0) | (NIR > 10000) | (red < 0) | (red > 10000) | (blue < 0) | (blue > 10000)') # masks exclude invalid pixels
    else:
        mask = numexpr.evaluate('(fmask == 0) | ((NIR < 0) | (NIR > 10000) | (red < 0) | (red > 10000) | (blue < 0) | (blue > 10000))') # reevaluate mask for EVI
    C1 = 6
    C2 = 7.5
    G = 2.5
    L = 1
    EVI = 10000 * (G * ((NIR - red)/(NIR+ C1 * red - C2 * blue + L)))
    EVI[mask] = 0.0 # replace invalid pixels with zero
    mask = None
    return EVI.astype(numpy.int16)

def NDindex(A, B, *args, **kwargs):
    # This function calculates a 2 dimensional normalized difference array
    fmask = kwargs.get('fmask', None)
    if not isinstance(fmask, numpy.ndarray):   
        mask = numexpr.evaluate('(A < 0) | (A > 10000) | (B < 0) | (B > 10000)')  # masks exclude invalid pixels
    else:
        mask = numexpr.evaluate('(fmask == 0) | ((A < 0) | (A > 10000) | (B < 0) | (B > 10000))')
    data =  10000 * ((A - B)/(A + B))#numpy.divide(numpy.subtract(A, B),numpy.add(A, B))
    data[mask] = 0.0 # replace invalid pixels with zero
    mask = None
    return data.astype(numpy.int16)

def importespa(f, *args, **kwargs):
    # This function imports new ESPA-process LEDAPS data
    # Version 1.1.1: Landsat Collection 1 Level 2 data now supported
    overwrite = kwargs.get('overwrite', False)
    tempdir = kwargs.get('tempdir', None)
    remove = kwargs.get('remove', False)
    useProdID = kwargs.get('useProductID', useProductID) # Name files using new Landsat Collection 1 Product ID rather than old Scene ID
    btimg = None
    basename = os.path.basename(f)
    dirname = os.path.dirname(f)
    if basename[2:3] == '0': # This will have to be updated once Landsat 10 launches
        landsat = basename[3:4]
    else:
        landsat = basename[2:3]
    outputdir = None 
    projection = prj.GetAttrValue('projcs')
    
    if landsat == '8': 
        bands = ['1', '2', '3', '4', '5', '6', '7']
    elif basename[1:2] == 'M':
        print('Landsat MSS is not supported yet, returning.')
        return
    else:
        bands = ['1', '2', '3', '4', '5', '7']
    if f.endswith('.tar.gz'):
        if tempdir:
            if not os.path.isdir(tempdir):
                try: 
                    os.mkdir(tempdir)
                    outputdir = tempdir
                except:
                    outputdir = None
        if not outputdir:
            if '-' in basename:
                i = f.rfind('-')
            else:
                i = f.find('.tar.gz')
            outputdir = f[:i]
        filelist = untarfile(f, outputdir)
    else:
        filelist = glob.glob(os.path.join(dirname, '*'))
        outputdir = dirname
    if filelist == 0 or len(filelist) == 0:
        print('ERROR: there is a problem with the files, skipping.')
        if len(filelist) == 0:
            logerror(f, 'No files found.')
        return
    
    if any(x.endswith('.tif') for x in filelist):
        ext = 'tif'
    else:
        ext = 'img'
    xml = glob.glob(os.path.join(outputdir, '*.xml'))
    if len(xml) > 0:
        ProductID = os.path.basename(xml[0]).replace('.xml', '') # Modified from sceneID in 1.1.1: sceneID will now be read from landsatshp
    elif basename[:1] == 'L' and len(basename) > 40:
        ProductID = basename[:40]
    else: 
        print('No XML file found, returning.')
        logerror(f, 'No XML file found.')
        return
    
    # open landsat shapefile (starting version 1.1.1)
    sceneid = None
    driver = ogr.GetDriverByName("ESRI Shapefile")
    data_source = driver.Open(landsatshp, 1) # opened with write access as LEDAPS data will be updated
    layer = data_source.GetLayer()
    while not sceneid:
        feat = layer.GetNextFeature()
        if ProductID == feat.GetField('LandsatPID'):
            sceneid = feat.GetField('sceneID')
    
    # delete any processed files if overwrite is set
    if overwrite:
        for d in [fmaskdir, pixelqadir, srdir, btdir, ndvidir, evidir]:
            dellist = glob.glob(os.path.join(d, '{}*.*'.format(sceneid[:16]))) # This will delete everything from the same date, path, and row, and ignore station/ processing info in sceneid[16:21]
            if len(dellist) > 0:
                print('Deleting existing output files.')
                for entry in dellist:
                    os.remove(entry)
            dellist = glob.glob(os.path.join(d, '{}*.*'.format(ProductID)))
            if len(dellist) > 0:
                print('Deleting existing output files.')
                for entry in dellist:
                    os.remove(entry)
    
    # Fmask file, if exists
    in_raster = os.path.join(outputdir, '{}_cfmask.{}'.format(sceneid, ext))
    if os.access(in_raster, os.F_OK): 
        if useProdID:
            out_raster = os.path.join(fmaskdir, '{}_cfmask.dat'.format(ProductID))
        else:
            out_raster = os.path.join(fmaskdir, '{}_cfmask.dat'.format(sceneid))
        if not os.path.exists(out_raster):  
            print('Reprojecting {} Fmask to {}.'.format(sceneid, projection))
            reproject(in_raster, out_raster, sceneid = sceneid, rastertype = 'Fmask')
    
    # Pixel QA layer
    in_raster = os.path.join(outputdir, '{}_pixel_qa.{}'.format(ProductID, ext))
    if not os.access(in_raster, os.F_OK):
        print('Error, Pixel QA file is missing. Returning.')
        logerror(in_raster, 'Pixel QA file missing.')
        return
        
    if useProdID:
        out_raster = os.path.join(fmaskdir, '{}_pixel_qa.dat'.format(ProductID))
    else:
        out_raster = os.path.join(fmaskdir, '{}_pixel_qa.dat'.format(sceneid))
    if not os.path.exists(out_raster):  
        print('Reprojecting {} Pixel QA layer to {}.'.format(sceneid, projection))
        reproject(in_raster, out_raster, sceneid = sceneid, rastertype = 'pixel_qa')    
    
    # Surface reflectance data
    if useProdID:
        out_itm = os.path.join(srdir,'{}_ref_{}.dat'.format(ProductID, projacronym))
    else:
        out_itm = os.path.join(srdir,'{}_ref_{}.dat'.format(sceneid, projacronym))
    if not os.path.exists(out_itm): 
        print('Compositing surface reflectance bands to single file.')
        srlist = []
        out_raster = os.path.join(outputdir, '{}.vrt'.format(sceneid))  # no need to update to ProductID for now- it is a temporary file
        if not os.path.exists(out_raster): 
            mergelist = ['gdalbuildvrt', '-separate', out_raster]
            for band in bands:
                fname = os.path.join(outputdir, '{}_sr_band{}.{}'.format(ProductID, band, ext))
                srlist.append(os.path.basename(fname))
                if not os.path.isfile(fname):
                    print('Error, {} is missing. Returning.'.format(os.path.basename(fname)))
                    logerror(f, '{} band {} file missing.'.format(ProductID, band))
                    return
                mergelist.append(fname)
            p = Popen(mergelist)
            print(p.communicate())
        print('Reprojecting {} reflectance data to {}.'.format(sceneid, projection))
        reproject(out_raster, out_itm, rastertype = 'ref', sceneid = sceneid, parentrasters = srlist)
        
        # Update LEDAPS info in shapefile 
        feat.SetField('LEDAPS', out_itm)
        layer.SetFeature(feat)
    
    # Thermal data
    if basename[2:3] != '8':
#        outbtdir = btdir
        rastertype = 'Landsat Band6'
        btimg = os.path.join(outputdir,'{}_bt_band6.{}'.format(ProductID, ext))
        parentrasters = [os.path.basename(btimg)]
    else:
#        outbtdir = os.path.join(btdir, 'Landsat8')
        rastertype = 'Landsat TIR'
        btimg = os.path.join(outputdir,'{}_BT.vrt'.format(sceneid))
        print('Stacking Landsat 8 TIR bands for scene {}.'.format(sceneid))
        mergelist = ['gdalbuildvrt', '-separate', btimg]
        parentrasters = []
        for band in [10, 11]:
            fname = os.path.join(outputdir,'{}_bt_band{}.{}'.format(ProductID, band, ext))
            mergelist.append(fname)
            parentrasters.append(os.path.basename(fname))
        p = Popen(mergelist)
        print(p.communicate())
    if btimg:
        if useProdID:
            BT_ITM = os.path.join(btdir, '{}_BT_{}.dat'.format(ProductID, projacronym))
        else:
            BT_ITM = os.path.join(btdir, '{}_BT_{}.dat'.format(sceneid, projacronym))
        if not os.path.exists(BT_ITM): 
            print('Reprojecting {} brightness temperature data to {}.'.format(sceneid, projection))
            reproject(btimg, BT_ITM, rastertype = rastertype, sceneid = sceneid, parentrasters = parentrasters)
        
    
    # Calculate EVI and NDVI
    if useProdID:
        evibasefile = '{}_EVI.dat'.format(ProductID)
    else:
        evibasefile = '{}_EVI.dat'.format(sceneid)
    if not os.path.exists(os.path.join(evidir, evibasefile)): 
        try:
            calcvis(out_itm)
        except Exception as e:
            print('An error has occurred calculating VIs for scene {}:'.format(sceneid))
            print(e)
            logerror(out_itm, e)
    
    # Clean up files.
    
    if basename.endswith('.tar.gz'):
        print('Moving {} to archive: {}'.format(basename, archdir))
        if not os.access(os.path.join(archdir, os.path.basename(f)), os.F_OK):
            shutil.move(f, archdir)
    if remove:
        print('Cleaning up files in directory.')
        filelist = glob.glob(os.path.join(outputdir, '{}*.*'.format(sceneid)))
        try:
            for fname in filelist:
                if os.access(fname, os.F_OK):
                    os.remove(fname)
            os.rmdir(outputdir)
        except Exception as e:
            print('An error has occurred cleaning up files for scene {}:'.format(sceneid))
            print(e)
            logerror(f, e)
    
    data_source = None # Close the shapefile
    print('Processing complete for scene {}.'.format(sceneid))
        

def ESPAreprocess(SceneID, listfile):
    print('Adding scene {} for ESPA reprocessing to: {}'.format(SceneID, listfile))
    with open(listfile, 'a') as output:
        output.write('{}\n'.format(SceneID))


## File compression/ decompression

def unzip(f, outdir):
    import zipfile
    basename = os.path.basename(f)
    print('Unzipping {} to: {}'.format(basename, outdir)) 
    zip_ref = zipfile.ZipFile(f, 'r')
    zip_ref.extractall(outdir)
    zip_ref.close()
    print('Files extracted to: {}'.format(outdir))

def maketarfile(f, archdir):
    import tarfile
    basename, dirname = os.path.split(f)
    j = basename.rfind('_')
    dataset = basename[:j]
    outname = os.path.join(archdir, '{}.tar.gz'.format(dataset))
    datalist=glob.glob(os.path.join(dirname, '{}*.*'.format(dataset)))
    tarlist = [d for d in datalist if (('.dat' not in d) and ('.hdr' not in d))]
    tarfiles = len(tarlist)
    tarnum = 1
    print('Writing {} files to: {}'.format(tarfiles, outname))
    tar = tarfile.open(outname, 'w:gz')
    boname = os.path.basename(outname)
    for d in tarlist:
        bname = os.path.basename(d)
        print('Writing {} to {}, file number {} of {}.'.format(bname, boname, tarnum, tarfiles))
        tar.add(d, recursive = False)
        tarnum += 1
    tar.close()
    print('Now deleting files from disk.')
    for d in tarlist:
        os.remove(d)
    print('Processing for {} complete.'.format(boname))  

def untarfile(file, outdir):
    import tarfile
    basename = os.path.basename(file)
    outbasepath = os.path.basename(outdir)
    if outbasepath in basename:
        outpath = outdir
    else:
        outpath = os.path.join(outdir, basename.rstrip('.tar.gz'))
    if not os.path.isdir(outpath):
        os.mkdir(outpath)
    os.chdir(outpath)
    print('Extracting {} to: {}'.format(basename, outpath))
    try:
        with tarfile.open(file) as tar:
            tar.extractall()
        filelist = glob.glob(os.path.join(outpath, '*.*'))
        # tar.close()
        if os.path.isdir(os.path.join(outpath, 'gap_mask')):
            shutil.rmtree(os.path.join(outpath, 'gap_mask'))
        print('Completed extracting: {}'.format(file))
        return filelist
    except Exception as e:
        logerror(file, e)
        print(e)
        tar.close()
        os.remove(file) # delete bad tar.gz
        return 0
