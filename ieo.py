#/usr/bin/python
# By Guy Serbin, EOanalytics Ltd.
# Talent Garden Dublin, Claremont Ave. Glasnevin, Dublin 11, Ireland
# email: guyserbin <at> eoanalytics <dot> ie

# Irish Earth Observation (IEO) Python Module
# version 1.2


# This contains code borrowed from the Python GDAL/OGR Cookbook: https://pcjericks.github.io/py-gdalogr-cookbook/

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

def getbadlist(*args, **kwargs):
    badlistfile = kwargs.get('badlist', badlandsat)
    badlist = []
    if os.path.isfile(badlistfile):
        with open(badlistfile, 'r') as lines:
            for line in lines:
                if len(line) >= 7:
                    badlist.append(line.rstrip())
    else:
        print('ERROR: file not found: {}'.format(badlistfile))
        logerror(badlistfile, 'File not found.')
    return badlist


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

    # Create spatial reference if missing or set to something other
    if not prj or projection != prjstr:
        spatialRef = osr.SpatialReference()
        i = projection.find(':') + 1
        spatialRef.ImportFromEPSG(int(projection[i:]))
    else:
        spatialRef = prj

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
    layer = data_source.CreateLayer("Tiles", spatialRef, ogr.wkbPolygon)

    # Add fields
    field_name = ogr.FieldDefn("Tile", ogr.OFTString)
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
                outFeature.Destroy()
        if i1 >= 25:
            h += 1
            i1 = 0
        else:
            i1 += 1

    # Create ESRI.prj file
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
            if isinstance(parentrasters, list):
                parentrasters = makeparentrastersstring(parentrasters)
            ENVIfile(out_raster, rastertype, SceneID = sceneid, outdir = outdir, parentrasters = parentrasters).WriteHeader()

def makeparentrastersstring(parentrasters):
    outline = 'parent rasters = { '
    for x in parentrasters:
        if parentrasters.index(x) == 0:
            outline += x
        else:
            outline += ', {}'.format(x)
    outline += ' }\n'
    return outline

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
    layer = ds.GetLayer()
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

def world2Pixel(geoMatrix, x, y):
  """
  Uses a gdal geomatrix (gdal.GetGeoTransform()) to calculate
  the pixel location of a geospatial coordinate
  """
  ulX = geoMatrix[0]
  ulY = geoMatrix[3]
  xDist = geoMatrix[1]
  yDist = geoMatrix[5]
  rtnX = geoMatrix[2]
  rtnY = geoMatrix[4]
  pixel = int((x - ulX) / xDist)
  line = int((ulY - y) / xDist)
  return (pixel, line)

def pixel2world(geoMatrix, pixel, line):
  """
  Uses a gdal geomatrix (gdal.GetGeoTransform()) to calculate
  the pixel location of a geospatial coordinate
  """
  ulX = geoMatrix[0]
  ulY = geoMatrix[3]
  xDist = geoMatrix[1]
  yDist = geoMatrix[5]
  rtnX = geoMatrix[2]
  rtnY = geoMatrix[4]
  x = xDist * float(pixel) + ulX
  y = yDist * float(line) + ulY
  return (x, y)


def converttotiles(infile, outdir, rastertype, *args, **kwargs):
    # This function converts existing data to NTS tiles
    # Code addition started on 11 July 2019
    # includes code from https://gis.stackexchange.com/questions/220844/get-field-names-of-shapefiles-using-gdal
    inshp = kwargs.get('inshape', landsatshp) #input shapefile containing data inventory
    tileshp = kwargs.get('tileshp', NTS) 
    pixelqa = kwargs.get('pixelqa', True) # determines whether to search for Pixel QA of CFmask files
    rewriteheader = kwargs.get('rewriteheader', True)
    overwrite = kwargs.get('overwrite', True) # overwrite existing files without updating, deleting any tiles first.
    noupdate = kwargs.get('noupdate', False) # if set to True, will not update existing tiles with new data.
    ext = kwargs.get('ext', 'dat') # file extension of raster files. Assumes ENVI format
    acqtime = None
    sceneids = []
    if infile.endswith('.vrt'):
        with open(infile, 'r') as lines:
            for line in lines:
                if 'SourceFilename' in line:
                    i = line.find('>') + 1
                    j = line.rfind('<')
                    fname = line[i:j]
                    sceneids.append(os.path.basename(fname)[:21])
                    if not acqtime:
                        acqtime = envihdracqtime(line[i:j].replace('.dat', '.hdr'))
                    
    else:
        try:
            acqtime = envihdracqtime(infile.replace('.{}'.format(ext), '.hdr'))
        except:
            acqtime = None
    driver = ogr.GetDriverByName("ESRI Shapefile")
    data_source = driver.Open(inshp, 1) # opened with write access as LEDAPS data will be updated
    layer = data_source.GetLayer()
    ldefn = layer.GetLayerDefn()
    schema = [ldefn.GetFieldDefn(n).name for n in range(ldefn.GetFieldCount())]
    if not 'tiles' in schema: # this will add two fields to the s
        tilebasefield = ogr.FieldDefn('tilebase', ogr.OFTString)
        layer.CreateField(tilebasefield)
        tilesfield = ogr.FieldDefn('tiles', ogr.OFTString)
        layer.CreateField(tilesfield)
    indir, inbasename = os.path.split(infile)
    sceneid = inbasename[:21] # optimised now for Landsat. Must change for IEO 2.0
    outbasename = '{}_{}'.format(inbasename[:3], inbasename[9:16])
    
    tile_ds = driver.Open(tileshp, 0)
    tilelayer = tile_ds.GetLayer()
    
#    hdr = isenvifile(infile)
#    if hdr:
#        headerdict = readenvihdr(hdr)
#    else:
#        headerdict = None
#    
#    headerdict['ready'] = True
    
    src_ds = gdal.Open(infile)
    gt = src_ds.GetGeoTransform()
    
    # create scene geometry polygon
    minX = gt[0]
    maxY = gt[3]
    maxX = gt[0] + gt[1] * src_ds.RasterXSize
    minY = gt[3] + gt[5] * src_ds.RasterYSize
    ring = ogr.Geometry(ogr.wkbLinearRing)
    ring.AddPoint(minX, maxY)
    ring.AddPoint(maxX, maxY)
    ring.AddPoint(maxX, minY)
    ring.AddPoint(minX, minY)
    ring.AddPoint(minX, maxY)
    rasterGeometry = ogr.Geometry(ogr.wkbPolygon)
    rasterGeometry.AddGeometry(ring)
    
    fieldnamedict = {'Fmask_path' : ['Fmask'],
        'PixQA_path' : ['pixel_qa'],
        'BT_path' : ['BT'], #['Landsat TIR', 'Landsat Band6'],
        'SR_path' : ['ref'], #['Landsat TM', 'Landsat ETM+', 'Landsat OLI', 'Sentinel-2'],
        'NDVI_path' : ['NDVI'],
        'EVI_path' : ['EVI']}
    fieldname = None
    for key in fieldnamedict.keys():
        if rastertype in fieldnamedict[key]:
            fieldname = key
            break
    found = False
    while not found:
        feat = layer.GetNextFeature()
        if len(sceneids) > 0:
            sid = sceneids[0]
        else:
            sid = sceneid
        if sid == feat.GetField('sceneID'):
            found = True
            #featgeom = feat.GetGeometryRef()
#            print('Found record for SceneID: {}.'.format(sceneid))
#            print(feat.GetField('sceneID'))
            for tile in tilelayer:
                tilegeom = tile.GetGeometryRef()
                tilename = tile.GetField('Tile')
#                print(tilename)
#                print(tilegeom.Intersect(featgeom))
                if tilegeom.Intersect(rasterGeometry) and not sceneid[9:16] in getbadlist():
                    if pixelqa:
                        basedir = os.path.dirname(outdir)
                        tileqafile = os.path.join(os.path.join(basedir, 'pixel_qa'), '{}_{}.dat'.format(outbasename, tilename))
                        pixelqadata = gettileqamask(tileqafile, sid, land = True, water = True, snowice = True, usemedcloud = True, usehighcirrus = True, useterrainocclusion = True)
                    else: 
                        pixelqadata = None
                    print('Now creating tile {} of type {} for SceneID {}.'.format(tilename, rastertype, sid))
#                    print(headerdict['description'])
                    try:
                        result = makerastertile(tile, src_ds, gt, outdir, outbasename, infile, rastertype, pixelqadata = pixelqadata, SceneID = sid, rewriteheader = rewriteheader, acqtime = acqtime, noupdate = noupdate, overwrite = overwrite)
                    except Exception as e:
                        logerror(outbasename, e)
                        print('ERROR: {}: {}'.format(outbasename, e))
                        result = False
                    if result:  
                        tilebasestr = feat.GetField('tilebase')
                        tilestr = feat.GetField('tiles')
                        
                        if not tilestr:
                            tilestr = tilename
                        else:
                            tilestr += ',{}'.format(tilename)
                        feat.SetField('tiles', tilestr)
                        if not tilebasestr == outbasename:
                            feat.SetField('tilebase', outbasename)
                        if fieldname:
                            fieldnamestr = feat.GetField(fieldname)
                            if not fieldnamestr:
                                fieldnamestr = ''
                            if not tilename in fieldnamestr:
                                if len(fieldnamestr) == 0:
                                    fieldnamestr = fieldname
                                else:
                                    fieldnamestr += ',{}'.format(fieldname)
                                feat.SetField(fieldname, fieldnamestr)
                        layer.SetFeature(feat)
    del src_ds
    del tile_ds
    del pixelqadata
    del data_source


def makerastertile(tile, src_ds, gt, outdir, outbasename, inrastername, rastertype, *args, **kwargs):
    # Adapted from IForDEO code starting on 9 July 2019
    # This function only processes individual tiles, and should be called from another function
    # raster tile and pixelQA files should be VRTs
    # optional pixelQA data should be an array of zeroes and ones, where pixels that equal one are included for processing. Valid pixels will be identified as clear land, water, or snow/ice.
    overwrite = kwargs.get('overwrite', False) # setting this option will delete an existing raster tile
    update = kwargs.get('update', True) # setting this option will replace pixels with no data with any new data.
#    hdict = kwargs.get('headerdict', None)
    pixelqatile = kwargs.get('pixelqadata', None)
    SceneID = kwargs.get('SceneID', None)
    rewriteheader = kwargs.get('rewriteheader', True)
    acqtime = kwargs.get('acqtime', None)
    noupdate = kwargs.get('noupdate', False) # This will prevent the function from updating the tile with new data
    overwrite = kwargs.get('overwrite', False) # This will delete any existing tile data
    tilename = tile.GetField('Tile')
    tilegeom = tile.GetGeometryRef()
    outfile = os.path.join(outdir, '{}_{}.dat'.format(outbasename, tilename))
    parentrasters = makeparentrastersstring([os.path.basename(inrastername)])
    if rastertype in ['ref', 'BT']:
        print('SceneID = {}'.format(SceneID))
        hdtype = headerdict['Landsat'][SceneID[:3]][rastertype]
    else:
        hdtype = rastertype
    ndval = headerdict[hdtype]['data ignore value']
    print('hdtype = {}, data ignore value = {}'.format(hdtype, ndval))
    if os.path.isfile(outfile) and not overwrite and not update: # skips this tile if the tile is not to be overwritten or updated.
        print('The tile {} exists already on disk, and both overwrite and update flags are set to False. Skipping this tile,'.format(os.path.basename(tilename)))
        return False
    minX, maxX, minY, maxY = tilegeom.GetEnvelope()
    geoTrans = (minX, 30, 0.0, maxY, 0.0, -30)
    cols = int((maxX - minX) / 30) # number of samples or columns
    rows = int((maxY - minY) / 30) # number of lines or rows
    bands = src_ds.RasterCount
    print('Processing tile: {}'.format(tilename))
    dims = [minX, maxY, maxX, minY]
    
#    print('Output columns: {}'.format(cols))
#    print('Output rows: {}'.format(rows))
    # determine extent of tile, etc.   
    extent = [gt[0], gt[3], gt[0] + gt[1] * src_ds.RasterXSize, gt[3] + gt[5] * src_ds.RasterYSize]
    #                if checkintersect(tilegeom, extent):
    ul = [max(dims[0], extent[0]), min(dims[1], extent[1])]
    lr = [min(dims[2], extent[2]), max(dims[3], extent[3])]
#    print('raster ul:')
#    print(ul)
#    print('raster lr:')
#    print(lr)
#    print('Tile coordinates (minX, maxY, maxX, minY): {}, {}, {}, {}'.format(minX, maxY, maxX, minY))
    px, py = world2Pixel(geoTrans, ul[0], ul[1])
    if px < 0:
        px = 0
    if py < 0:
        py = 0
    plx, ply = world2Pixel(geoTrans, lr[0], lr[1])
    if plx >= extent[0]:
        plx = extent[0]-1
    if ply >= extent[1]:
        ply = extent[1]-1
    pX, pY = pixel2world(geoTrans, px, py)
    plX, plY = pixel2world(geoTrans, plx, ply)
    ulx,uly = world2Pixel(gt, pX, pY)
    if ulx < 0:
        ulx = 0
    lrx,lry = world2Pixel(gt, plX, plY)
    if lrx >= src_ds.RasterXSize:
        lrx = src_ds.RasterXSize - 1 
    if uly >= src_ds.RasterYSize:
        uly = src_ds.RasterYSize - 1
    if lry < 0:
        lry = 0 
    
    dx = plx-px + 1
    dy = ply-py + 1
#    print('ulx = {}, uly = {}, dx = {}, dy = {}.'.format(ulx, uly, dx - 1, dy - 1))
#    print('plx = {}, ply = {}, px = {}, py = {}.'.format(plx, ply, px, py))
#    if isinstance(pixelqadata, numpy.ndarray):
#        pixelqatile = pixelqadata[uly:dy - 1, ulx:dx - 1]
#        print(pixelqatile.shape)
#    else:
    if not isinstance(pixelqatile, numpy.ndarray):
        pixelqatile = numpy.ones((rows, cols), dtype = numpy.int8)
    
    if 1 in pixelqatile: # determine if there are usable pixels in tile area
#        if bands > 1:
#            p = Popen(['gdal_translate', '-of', 'ENVI', '-projwin', str(minX), str(maxY), str(maxX), str(minY), inrastername, outfile])
#            print(p.communicate())
#            if rewriteheader:
#                if isinstance(parentrasters, list):
#                    parentrasters = makeparentrastersstring(parentrasters)
#                ENVIfile(outfile, rastertype, SceneID = SceneID, outdir = outdir, parentrasters = parentrasters).WriteHeader()
#            
#        else:
#        print(pixelqatile.shape)
        print('Subsetting raster data for tile: {}'.format(tilename))
        # Get the type of the data
        gdal_dt = src_ds.GetRasterBand(1).DataType
        if gdal_dt == gdal.GDT_Byte:
            dt = 'uint8'
        elif gdal_dt == gdal.GDT_Int16:
            dt = 'int16'
        elif gdal_dt == gdal.GDT_UInt16:
            dt = 'uint16'
        elif gdal_dt == gdal.GDT_Int32:
            dt = 'int32'
        elif gdal_dt == gdal.GDT_UInt32:
            dt = 'uint32'
        elif gdal_dt == gdal.GDT_Float32:
            dt = 'float32'
        elif gdal_dt == gdal.GDT_Float64:
            dt = 'float64'
        elif gdal_dt == gdal.GDT_CInt16 or gdal_dt == gdal.GDT_CInt32 or gdal_dt == gdal.GDT_CFloat32 or gdal_dt == gdal.GDT_CFloat64 :
            dt = 'complex64'
        else:
            print('Error: Data type unknown')
            logerror(outbasename, 'Error: Data type unknown')
            return False
    #        print('data type = {}'.format(dt))
        if bands > 1:
            bandarr = []
    #            shape = (bands, rows, cols)
    #        else:
        shape = (rows, cols)
        
        outtile = numpy.full(shape, ndval, dtype = dt)
        
        if os.path.isfile(outfile):
            if noupdate:
                print('noupdate has been set to True, skipping file.')
                return False
            elif overwrite:
                print('Deleting existing tile.')
                os.remove(outfile)
            else:
                out_ds = gdal.Open(outfile)
    #            outheaderdict = readheader(outfile.replace('.dat', '.hdr'))
    #        else:
    #            outheaderdict = headerdict['default'].copy()
        for i in range(bands):
            if os.path.isfile(outfile):
                band = out_ds.GetRasterBand(i + 1).ReadAsArray()
            else:
                band = numpy.full((rows, cols), ndval, dtype = dt)
            tiledata = numpy.full((rows, cols), ndval, dtype = dt)
            tiledata[py:ply, px:plx] = src_ds.GetRasterBand(i + 1).ReadAsArray(ulx, uly, dx - 1, dy - 1)
    #            print('pixelqatile shape:')
    #            print(pixelqatile.shape)
    #            print('tiledata shape:')
    #            print(tiledata.shape)
            band[numexpr.evaluate("((pixelqatile == 1) & (tiledata != ndval))")] = tiledata[numexpr.evaluate("((pixelqatile == 1) & (tiledata != ndval))")]
            if bands > 1:
                bandarr.append(band)
            else:
                outtile = band
            
            band = None   
    #                indata = None
            tiledata = None             
        
        if bands > 1:
            outtile = numpy.stack(bandarr)
            bandarr = None
        out_ds = None # close tile before it gets overwritten, if open
    #    if not inrastername in headerdict['parent rasters']:
    #        headerdict['parent rasters'].append(inrastername)
        print('Writing to disk: {}'.format(outfile))
#        print(outtile.shape)
        ENVIfile(outtile, rastertype, geoTrans = geoTrans, outfilename = outfile, parentrasters = parentrasters, SceneID = SceneID, acqtime = acqtime).Save()
    #    p = Popen(['gdal_translate', '-projwin', extent[0], extent[1], extent[2], extent[3], '-of', 'ENVI', in_raster, out_raster])
    #    print(p.communicate())
    #    if rewriteheader:
    #        if isinstance(parentrasters, list):
    #            parentrasters = makeparentrastersstring(parentrasters)
    #        ENVIfile(outfilename, rastertype, SceneID = sceneid, outdir = outdir, parentrasters = parentrasters).WriteHeader()
        print('Tile has been written to disk')
    else:
        print('No useful pixels in tile, skipping.')
    outtile = None
    band = None
    pixelqatile = None
    return True

    
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


def gettileqamask(f, sceneid, *args, **kwargs):
    # started on 24 July 2019
    # This function identifies will get Pixel QA data for a specific NRT tile 
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
        
#    pixelqadata = None
    
    if not os.path.isfile(f):
        dirname, basename = os.path.split(f)
        f = os.path.join(dirname.replace('pixel_qa', 'Fmask'), basename)
        if not os.path.isfile(f):
            print('Error: Pixel QA/ cloud mask file missing. Skipping scene.')
            logerror(f, 'Error: Pixel QA/ cloud mask file missing. Skipping scene.')
            return None
    print('Now creating good pixel mask using cloud mask file: {}'.format(f))
    return maskfromqa(f, int(os.path.basename(f)[2:3]), sceneid, land = land, water = water, snowice = snow, usemedcloud = usemedcloud, usemedcirrus = usemedcirrus, usehighcirrus = usehighcirrus, useterrainocclusion = useterrainocclusion, shadow = shadow) #


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
    parentrasters = makeparentrastersstring(parentrasters)
    ENVIfile(NDVI, 'NDVI', outdir = ndvidir, geoTrans = geoTrans, SceneID = sceneid, acqtime = acqtime, parentrasters = parentrasters).Save()
    NDVI = None

    # EVI calculation
    evi = EVI(blue, red, NIR, fmask = fmask)
    ENVIfile(evi, 'EVI', outdir = evidir, geoTrans = geoTrans, SceneID = sceneid, acqtime = acqtime, parentrasters = parentrasters).Save()
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
    masktype = None
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
        masktype = 'Fmask'
        if feat.GetField('Fmask_path') != out_raster:
            feat.SetField('Fmask_path', out_raster)
        if feat.GetField('MaskType') != masktype:
            feat.SetField('MaskType', masktype)

    # Pixel QA layer
    in_raster = os.path.join(outputdir, '{}_pixel_qa.{}'.format(ProductID, ext))
    if os.access(in_raster, os.F_OK):
        if useProdID:
            out_raster = os.path.join(pixelqadir, '{}_pixel_qa.dat'.format(ProductID))
        else:
            out_raster = os.path.join(pixelqadir, '{}_pixel_qa.dat'.format(sceneid))
        if not os.path.isfile(out_raster):
            print('Reprojecting {} Pixel QA layer to {}.'.format(sceneid, projection))
            reproject(in_raster, out_raster, sceneid = sceneid, rastertype = 'pixel_qa')
        masktype = 'Pixel_QA'
        if feat.GetField('PixQA_path') != out_raster:
            feat.SetField('PixQA_path', out_raster)
        if feat.GetField('MaskType') != masktype:
            feat.SetField('MaskType', masktype)

    # Surface reflectance data
    if useProdID:
        out_itm = os.path.join(srdir,'{}_ref_{}.dat'.format(ProductID, projacronym))
    else:
        out_itm = os.path.join(srdir,'{}_ref_{}.dat'.format(sceneid, projacronym))
    if not os.path.isfile(out_itm):
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
        feat.SetField('SR_path', out_itm) # Update LEDAPS info in shapefile

    # Thermal data
    print('Processing thermal data.')
    if landsat != '8':
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
        if not os.path.isfile(BT_ITM):
            print('Reprojecting {} brightness temperature data to {}.'.format(sceneid, projection))
            reproject(btimg, BT_ITM, rastertype = rastertype, sceneid = sceneid, parentrasters = parentrasters)
        if feat.GetField('BT_path') != BT_ITM:
            feat.SetField('BT_path', BT_ITM)

    # Calculate EVI and NDVI
    print('Processing vegetation indices.')
    if useProdID:
        evibasefile = '{}_EVI.dat'.format(ProductID)
    else:
        evibasefile = '{}_EVI.dat'.format(sceneid)
    evifile = os.path.join(evidir, evibasefile)
    ndvifile = os.path.join(evidir, evibasefile.replace('_EVI', '_NDVI'))
    if not os.path.isfile(evifile):
        try:
            calcvis(out_itm)
        except Exception as e:
            print('An error has occurred calculating VIs for scene {}:'.format(sceneid))
            print(e)
            logerror(out_itm, e)
    if os.path.isfile(evifile) and feat.GetField('EVI_path') != evifile:
        feat.SetField('EVI_path', evifile)
    if os.path.isfile(ndvifile) and feat.GetField('NDVI_path') != ndvifile:
        feat.SetField('NDVI_path', ndvifile)

    # Set feature in shapefile to preserve processed file metadata
    print('Updating information in shapefile.')
    layer.SetFeature(feat)
    data_source = None # Close the shapefile

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
