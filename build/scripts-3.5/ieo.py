#/usr/bin/python
# By Guy Serbin, Environment, Soils, and Land Use Dept., CELUP, Teagasc,
# Johnstown Castle, Co. Wexford Y35 TC97, Ireland
# email: guy <dot> serbin <at> teagasc <dot> ie

# Irish Earth Observation (IEO) Python Module
# version 1.1.0 

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
global fmaskdir, srdir, btdir, ingestdir, ndvidir, evidir, archdir, catdir, logdir, NTS, Sen2tiles, prjstr, WRS1, WRS2, defaulterrorfile, gdb_path, landsatshp, prj, projacronym

# configuration data
if sys.version_info[0] == 2:
    import ConfigParser as configparser
else:
    import configparser


# Access configuration data inside Python egg
config = configparser.ConfigParser()
config_location = resource_filename(Requirement.parse('ieo'), 'config/ieo.ini')
config.read(config_location) # config_path
fmaskdir = config['DEFAULT']['fmaskdir']
srdir = config['DEFAULT']['srdir']
btdir = config['DEFAULT']['btdir']
ingestdir = config['DEFAULT']['ingestdir']
ndvidir = config['DEFAULT']['ndvidir']
evidir = config['DEFAULT']['evidir']
catdir = config['DEFAULT']['catdir']
archdir = config['DEFAULT']['archdir']
logdir = config['DEFAULT']['logdir']
prjstr = config['Projection']['proj']
projacronym = config['Projection']['projacronym']
landsatshp = os.path.join(catdir, 'Landsat', config['VECTOR']['landsatshp'])
gdb_path = os.path.join(catdir, config['DEFAULT']['GDBname'])
WRS1 = os.path.join(gdb_path, config['VECTOR']['WRS1']) # WRS-1, Landsats 1-3
WRS2 = os.path.join(gdb_path, config['VECTOR']['WRS2']) # WRS-2, Landsats 4-8
NTS = os.path.join(gdb_path, config['VECTOR']['nationaltilesystem']) # For Ireland, the All-Ireland Raster Tile (AIRT) tile polygon layer
Sen2tiles = os.path.join(gdb_path, config['VECTOR']['Sen2tiles']) # Sentinel-2 tiles for Ireland
defaulterrorfile = os.path.join(logdir, 'errors.csv')
badlandsat = os.path.join(catdir, 'Landsat', 'badlist.txt')

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
    longmax = longmin + geotrans[1] * float(src_ds.RasterXSize)
    src_ds = None
    X = (float(longmax.getOutput(0)) + float(longmin.getOutput(0))) / 2.
    Y = (float(latmax.getOutput(0)) + float(latmin.getOutput(0))) / 2.
    print('{} scene centre coordinates are {:0.1f} E, {:0.1f} N.'.format(projacronym, X, Y))
    
    driver = ogr.GetDriverByName("FileGDB")
    gdb, wrs = os.path.split(polygon)
    ds = driver.Open(gdb, 0)
    layer = ds.Getlayer(wrs)
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
    

def calcvis(refitm, *args, **kwargs): # This should calculate a masked NDVI.
    # This function creates NDVI and EVI files.
    dirname, basename = os.path.split(refitm)
    sceneid = basename[:21]
    acqtime = envihdracqtime(refitm.replace('.dat', '.hdr'))
    fmaskfile = os.path.join(fmaskdir,'%s_cfmask.dat'%sceneid)
    parentrasters = [os.path.basename(refitm)]
    usefmask = True
    if not os.path.exists(fmaskfile):
        fmaskfile = fmaskfile.replace('_cfmask.dat', '_fmask.dat')
        if not os.path.exists(fmaskfile):
            print('ERROR: Fmask file does not exist, returning.')
            logerror(fmaskfile, 'File not found.')
            usefmask = False
        else:
            parentrasters.append(os.path.basename(fmaskfile))
        
    print('Calculating NDVI for scene %s.'%sceneid)
    refobj = gdal.Open(refitm)
    
    # Get file geometry
    geoTrans = refobj.GetGeoTransform()
    ns = refobj.RasterXSize
    nl = refobj.RasterYSize    
    
    if usefmask:
        fmaskobj = gdal.Open(fmaskfile)
        fmask = fmaskobj.GetRasterBand(1).ReadAsArray()
    else: 
        print('Warning: No Fmask file found for scene %s.'%sceneid)
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
        mask = numexpr.evaluate('(fmask > 0) | ((NIR < 0) | (NIR > 10000) | (red < 0) | (red > 10000) | (blue < 0) | (blue > 10000))') # reevaluate mask for EVI
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
        mask = numexpr.evaluate('(fmask > 0) | ((A < 0) | (A > 10000) | (B < 0) | (B > 10000))')
    data =  10000 * ((A - B)/(A + B))#numpy.divide(numpy.subtract(A, B),numpy.add(A, B))
    data[mask] = 0.0 # replace invalid pixels with zero
    mask = None
    return data.astype(numpy.int16)

def importespa(f, *args, **kwargs):
    # This function imports new ESPA-process LEDAPS data
    overwrite = kwargs.get('overwrite', False)
    tempdir = kwargs.get('tempdir', None)
    remove = kwargs.get('remove', False)
    btimg = None
    basename = os.path.basename(f)
    dirname = os.path.dirname(f)
    landsat = basename[2:3]
    outputdir = None 
    projection = prj.GetAttrValue('projcs')
    
    if landsat == '8': 
        bands=['1', '2', '3', '4', '5', '6', '7']
    elif basename[1:2] == 'M':
        print('Landsat MSS is not supported yet, returning.')
        return
    else:
        bands=['1', '2', '3', '4', '5', '7']
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
        sceneid = os.path.basename(xml[0]).replace('.xml', '')
    elif basename[:1] == 'L' and len(basename) > 21:
        sceneid = basename[:21]
    else: 
        print('No XML file found, returning.')
        logerror(f, 'No XML file found.')
        return
    
    if overwrite:
        print('Deleting existing output files.')
        for d in [fmaskdir, srdir, btdir, ndvidir, evidir]:
            dellist = glob.glob(os.path.join(d,'{}*.*'.format(sceneid)))
            if len(dellist) > 0:
                for entry in dellist:
                    os.remove(entry)
    
    # Fmask file
    in_raster = os.path.join(outputdir, '{}_cfmask.{}'.format(sceneid, ext))
    if not os.access(in_raster, os.F_OK):
        print('Error, CFmask file is missing. Returning.')
        logerror(f, 'CFmask file missing.')
        return
        
    out_raster = os.path.join(fmaskdir, '{}_cfmask.dat'.format(sceneid))
    if not os.path.exists(out_raster):  
        print('Reprojecting {} Fmask to {}.'.format(sceneid, projection))
        reproject(in_raster, out_raster, sceneid = sceneid, rastertype = 'Fmask')
    
    # Surface reflectance data
    out_itm = os.path.join(srdir,'{}_ref_{}.dat'.format(sceneid, projacronym))
    if not os.path.exists(out_itm): 
        print('Compositing surface reflectance bands to single file.')
        srlist = []
        out_raster = os.path.join(outputdir, '{}.vrt'.format(sceneid))  
        if not os.path.exists(out_raster): 
            mergelist = ['gdalbuildvrt', '-separate', out_raster]
            for band in bands:
                fname = os.path.join(outputdir, '{}_sr_band{}.{}'.format(sceneid, band, ext))
                srlist.append(os.path.basename(fname))
                if not os.path.isfile(fname):
                    print('Error, {} is missing. Returning.'.format(os.path.basename(fname)))
                    logerror(f, '{} band {} file missing.'.format(sceneid, band))
                    return
                mergelist.append(fname)
            p = Popen(mergelist)
            print(p.communicate())
        print('Reprojecting {} reflectance data to {}.'.format(sceneid, projection))
        reproject(out_raster, out_itm, rastertype = 'ref', sceneid = sceneid, parentrasters = srlist)
    
    # Thermal data
    if basename[2:3] != '8':
        outbtdir = btdir
        rastertype = 'Landsat Band6'
        btimg = os.path.join(outputdir,'{}_toa_band6.{}'.format(sceneid, ext))
        parentrasters = [os.path.basename(btimg)]
    else:
        outbtdir = os.path.join(btdir, 'Landsat8')
        rastertype = 'Landsat TIR'
        btimg = os.path.join(outputdir,'{}_BT.vrt'.format(sceneid))
        print('Stacking Landsat 8 TIR bands for scene {}.'.format(sceneid))
        mergelist = ['gdalbuildvrt', '-separate', in_raster]
        parentrasters = []
        for band in [10, 11]:
            fname = os.path.join(outputdir,'{}_toa_band{}.{}'.format(sceneid, band, ext))
            mergelist.append(fname)
            parentrasters.append(os.path.basename(fname))
        p = Popen(mergelist)
        print(p.communicate())
    if btimg:
        BT_ITM = os.path.join(btdir, '{}_BT_{}.dat'.format(sceneid, projacronym))
        if not os.path.exists(BT_ITM): 
            print('Reprojecting {} brightness temperature data to {}.'.format(sceneid, projection))
            reproject(btimg, BT_ITM, rastertype = rastertype, sceneid = sceneid, parentrasters = parentrasters)
        
    
    # Calculate EVI and NDVI
    if not os.path.exists(os.path.join(evidir, '{}_EVI.dat'.format(sceneid))): 
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


## LEDAPS-specific functions
# These functions have all been deprecated.

def fixespahdr(f, *args, **kwargs):
    xml = kwargs.get('xml', None)
    rastertype = kwargs.get('rastertype', None)
    if not f.endswith('.hdr'):
        j = f.rfind('.') + 1
        f = f.replace(f[j:], 'hdr')
    now = datetime.datetime.now()
    bak = '%s.%s.bak'%(f, now.strftime('%Y%m%d_%H%M%S'))
    basename = os.path.basename(f)
    sceneid = basename[:21]
    landsat = sceneid[2:3]
    if xml:
        tags=['acquisition_date','scene_center_time']
        dom = minidom.parse(xml)
        acquisitiontime='acquisition time = '
        for tag in tags:
            try:
                value = extract_xml(
                dom.getElementsByTagName(tag)[0].toxml())
                if tag == 'acquisition_date':
                    acquisitiontime += value + 'T'
                else:
                    acquisitiontime += value + '\n'
            except Exception as e:
                print(e)
                return
            print(tag, value)
    else: 
        acquisitiontime = None
    shutil.move(f,bak)
    ENVIfile(f, rastertype, SceneID = sceneid, acqtime = acquisitiontime).WriteHeader()
    print('Header file updated for %s'%basename)    
        

def makehdrfile(filename, b1, taglist): # This modifies GDAL generated an ENVI header into a more useful with improved metadata. This function is deprecrated in favor of functions provided by the ENVIfile submodule.
    hdrname = filename.replace('.dat', '.hdr')
    dirname = os.path.dirname(filename)
    temphdr = os.path.join(dirname, 'temp.hdr')
    b1dict = b1.GetMetadata_Dict()
    sensor = b1dict['Satellite'] + ' ' + b1dict['Instrument'] + '\n'
    linenum = 1
    outlines = ['ENVI\n']
    print('Processing ' + hdrname)
    inhdr = open(hdrname, 'r')
    lines = inhdr.readlines()
    basename = os.path.basename(hdrname)
    i = basename.rfind('_') + 1
    j = basename.rfind('.')
    dataset = basename[:i - 1]
    if 'fmask' in basename:
        imagetype = 'fmask'
    else:
        imagetype = basename[i:j]
    if imagetype == 'ref':
        description ='description = { LEDAPS Surface Reflectance (%s)}\n'%dataset
        bnames = 'band names = {Band 1 (Blue), Band 2 (Green), Band 3 (Red), Band 4 (NIR), Band 5 (SWIR1), Band 7 (SWIR2)}\n'
        if basename[0:3] == 'LE7':
            wavelength = 'wavelength = { 0.483000, 0.560000, 0.662000, 0.835000, 1.648000, 2.206000}\n'
            fwhm = 'fwhm = { 0.070000, 0.080000, 0.060000, 0.120000, 0.200000, 0.260000}\n'
        if (basename[0:3] == 'LT4') or (basename[0:3] == 'LT5'):
            wavelength = 'wavelength = { 0.485000, 0.560000, 0.662000, 0.830000, 1.648000, 2.215000}\n'
            fwhm = 'fwhm = { 0.070000, 0.080000, 0.060000, 0.130000, 0.200000, 0.270000}\n'
    elif imagetype == 'BT':
        description = 'description = { LEDAPS Brightness Temperature (%s)}\n'%dataset
        if (basename[0:3] != 'LT8') or (basename[0:3] != 'LC8'):
            bnames = 'band names = {Band 6 (TIR)}\n'
            wavelength = 'wavelength = { 11.450000}\n'
            fwhm = 'fwhm = { 2.100000}\n'
        else:
            bnames = 'band names = {Band 10 (TIR 1), Band 11 (TIR 2)}\n'
            wavelength = 'wavelength = { 10.895000, 12.005000}\n'
            fwhm = 'fwhm = { 0.590000, 1.010000}\n'
    elif imagetype == 'QA':
        description = 'description = { Quality Assurance Data (%s)}\n'%dataset
        bnames = 'band names = {'
        for t in taglist:
            bnames += t + ','
        bnames = bnames[:-1] + '}\n'
    elif imagetype == 'fmask':
        description = 'description = { Fmask Cloud and Shadow Classification (%s)}\n'%dataset
        bnames = 'band names = { Cloud and Shadow Classification}\n'
        classnum = 'classes = 6\n'
        classlookup = 'class lookup = {0, 255, 0, 0, 0, 255, 127, 127, 127, 0, 255, 255, 255, 255, 255, 0, 0, 0}\n'
        classnames = 'class names = {Clear land, Clear water, Cloud shadow, Snow/ ice, Cloud, No data}\n'
    outlines.append(description)
    for line in lines:
        linenum += 1
        if 'samples' in line:
            outlines.append(line.rstrip() + '\n')
        elif 'lines' in line:
            outlines.append(line.rstrip() + '\n')
        elif 'bands' in line:
            outlines.append(line.rstrip() + '\n')
        elif 'header offset' in line:
            outlines.append(line.rstrip() + '\n')
        elif 'header offset' in line:
            outlines.append(line.rstrip() + '\n')
        elif 'file type' in line:
            outlines.append(line.rstrip() + '\n')
        elif 'data type' in line:
            outlines.append(line.rstrip() + '\n')
        elif 'interleave' in line:
            outlines.append(line.rstrip() + '\n')
        elif 'byte order' in line:
            outlines.append(line.rstrip() + '\n')
        elif 'map info' in line:
            outlines.append(line.rstrip() + '\n')
            projcs = b1.GetProjection()
            if 'Unknown datum based upon the WGS 84 ellipsoid' in projcs:
                projcs = projcs.replace('Unknown datum based upon the WGS 84 ellipsoid','WGS 84')
            if 'Not specified (based on WGS 84 spheroid)' in projcs:
                projcs = projcs.replace('Not specified (based on WGS 84 spheroid)','WGS_1984')
            outlines.append('coordinate system string = ' + projcs + '}\n')
    if imagetype == 'ref':
        outlines.append('default bands = {4,3,2}\n')
    if (imagetype != 'QA') and (imagetype != 'fmask'):
        outlines.append(bnames)
        outlines.append(wavelength)
        outlines.append(fwhm)
        outlines.append('wavelength units = Micrometers\n')
        if imagetype == 'ref':
            outlines.append('data ignore value = -9999\n')
            outlines.append('reflectance scale factor = 10000.0\n')
        else:
            outlines.append('calibration scale factor = 100.0\n')
            outlines.append('data units = Celsius\n')
    if imagetype == 'QA':#This aggregates multiple lines of band names into one line
        outlines.append(bnames)
    if imagetype == 'fmask':
        outlines.append(bnames)
        outlines.append(classnum)
        outlines.append(classlookup)
        outlines.append(classnames)
    outlines.append('sensor type = Landsat ' + b1dict['Instrument'] + '\n')
    outlines.append('acquisition time = ' + b1dict['AcquisitionDate'] + '\n')
    output = open(temphdr, 'w')    
    for x in outlines:
        output.write(x)    
    output.close()
    inhdr.close()
    shutil.copy(hdrname, hdrname + '.bak')
    os.remove(hdrname)
    shutil.copy(temphdr, hdrname)
    os.remove(temphdr)
    print('ENVI header updated for ' + hdrname + '.')

def runledaps(filename, outdir):
    filename = os.path.abspath(filename)
    infiledir = os.path.dirname(os.path.abspath(filename))
    basename = os.path.basename(filename)
    outdir = os.path.abspath(outdir)
    rmfilelist = []
    if '_' in os.path.basename(filename):
        i = filename.rfind('_')
    else:
        i = filename.find('.tar.gz')
    h = filename.rfind('/')
    sceneid = filename[h:i]
    if infiledir != outdir:
        print('Copying files from ' + infiledir + ' to ' + outdir)
        if filename.endswith('.tar.gz'): 
            shutil.copy(filename, outdir)
            rmfilelist = untarfile(outdir + '/' + basename, outdir)
            os.remove(outdir + '/' + basename)
            filename=outdir + '/' + basename.replace('.tar.gz', '_MTL.txt')
        else:
            filelist = glob.glob(filename[:i] + '*')
            for f in filelist:
                shutil.copy(f, outdir)
            filename = outdir + '/' + basename
        print('Copying complete.')
    if not filename.endswith('_MTL.txt'):
        if os.access(filename[:i] + '_MTL.txt', os.F_OK):
            filename = filename[:i] + '_MTL.txt'
        elif filename.endswith('.tar.gz'):
            rmfilelist = untarfile(filename, outdir)
            filename = filename.replace('.tar.gz', '_MTL.txt')
        else:
            print('Error: ' + basename + ' is not in the correct format, skipping.')
            return
    elif filename.endswith('_MTL.txt') and not os.access(filename, os.F_OK):
        print('Error: ' + basename + ' does not exist, skipping.')
        return
    print('Running LEDAPS for ' + basename)
    p = Popen(['do_ledaps.py', '-f', filename])
    print(p.communicate())
    print('Processing complete for ' + sceneid + '.')   
