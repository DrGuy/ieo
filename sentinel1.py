#!/usr/bin/python
# -*- coding: utf-8 -*-
# By Guy Serbin, EOanalytics Ltd.
# Talent Garden Dublin, Claremont Ave. Glasnevin, Dublin 11, Ireland
# email: guyserbin <at> eoanalytics <dot> ie

# Irish Earth Observation (IEO) Python Module
# version 1.3

# Sentinel-1 submodule

# This contains code borrowed from the Python GDAL/OGR Cookbook: https://pcjericks.github.io/py-gdalogr-cookbook/

# This module uses ESA SNAP and the Sentinel-1 Toolbox: https://step.esa.int/main/

import os, datetime, time, shutil, sys, glob, csv, ENVIfile, numpy, numexpr
from xml.dom import minidom
from subprocess import Popen
from pkg_resources import resource_stream, resource_string, resource_filename, Requirement
from ENVIfile import *
import ieo

# Import GDAL
if not 'linux' in sys.platform: # this way I can use the same library for processing on multiple systems
    # if sys.version_info[0] !=3: # Attempt to load ArcPy and EnviPy libraries, if not, use GDAL.
    #     try:
    #         from arcenvipy import *
    #     except:
    #         print('There was an error loading either ArcPy or EnviPy. Functions requiring this library will not be available.')
    from osgeo import gdal, ogr, osr

else: # Note- this hasn't been used or tested with Linux in a long time. It probably doesn't work.
    try:
        from osgeo import gdal, ogr, osr
    except:
        import gdal, ogr, osr
        sys.path.append('/usr/bin')
        sys.path.append('/usr/local/bin')

# configuration data
if sys.version_info[0] == 2:
    import ConfigParser as configparser
else:
    import configparser


# Access configuration data inside Python egg
config = configparser.ConfigParser()
config_location = resource_filename(Requirement.parse('ieo'), 'config/ieo.ini')
config.read(config_location) # config_path
ingestdir = config['Sentinel1']['ingestdir']
scriptdir = config['DEFAULT']['scriptdir']
catdir = config['DEFAULT']['catdir']
archdir = config['Sentinel1']['archdir']
logdir = config['DEFAULT']['logdir']
prjstr = config['Projection']['proj']
projacronym = config['Projection']['projacronym']
ieogpkg = os.path.join(catdir, config['VECTOR']['ieogpkg'])
NTS = config['VECTOR']['nationaltilesystem'] # For Ireland, the All-Ireland Raster Tile (AIRT) tile polygon layer
catgpkg = os.path.join(catdir, config['catalog']['catgpkg'])
S1shp = config['catalog']['S1shp']
GRDdir = config['Sentinel1']['GRDdir']
GRDimportGraph = os.path.join(scriptdir, config['Sentinel1']['GRDimportGraph'])
GRDspkimportGraph = os.path.join(scriptdir, config['Sentinel1']['GRDspkimportGraph'])
SLCcalGraph = os.path.join(scriptdir, config['Sentinel1']['SLCcalGraph'])
SLCcaldir = config['Sentinel1']['SLCcaldir']
InSARdir = config['Sentinel1']['InSARdir']
gpt = config['Sentinel1']['gpt']

# gdb_path = os.path.join(catdir, config['DEFAULT']['GDBname'])

defaulterrorfile = os.path.join(logdir, 'errors.csv')

dimapdict = {
            'Dataset_Id' : {'tags' : ['DATASET_NAME']},
            'Production' : {'tags' : ['PRODUCT_SCENE_RASTER_START_TIME', 'PRODUCT_SCENE_RASTER_STOP_TIME']},
            'Coordinate_Reference_System' : {'tags' : ['WKT']},
            'Geoposition' : {'tags' : ['IMAGE_TO_MODEL_TRANSFORM']},
            'Image_Interpretation' : {'tags' : ['Spectral_Band_Info'],
                  'subtags' : ['DATA_TYPE', 'PHYSICAL_UNIT', 'SCALING_FACTOR', 'SCALING_OFFSET', 'LOG10_SCALED', 'NO_DATA_VALUE_USED', 'NO_DATA_VALUE', 'IMAGE_TO_MODEL_TRANSFORM']},
            'Dataset_Sources' : {'tags' : ['MDElem'],
                  'subtags' : ['MDElem'], 'sstags' : ['MDATTR']},
            }

def runGRDHimport(*args, **kwargs):
    procdir = kwargs.get('procdir', os.path.join(ingestdir, 'temp'))
    indir = kwargs.get('indir', ingestdir)
    spkproc = kwargs.get('spkproc', False) # Setting this to "True" will process with speckle filtering, but not run the non-speckle filtered version
    both = kwargs.get('both', False) # Setting this runs both GRDH import graphs 
#    dimlist = glob.glob(os.path.join(procdir, 'S1*GRDH*.dim'))
    ziplist = glob.glob(os.path.join(indir, 'S1*GRDH*.zip'))
    
    if both:
        graphs = [GRDimportGraph, GRDspkimportGraph]
    elif spkproc:
        graphs = [GRDspkimportGraph]
    else:
        graphs = [GRDimportGraph]
    
    for graph in graphs:
        for f in ziplist:
            basename = os.path.basename(f)[:-4]
            dim = os.path.join(procdir, '{}.dim'.format(basename))
            if not os.path.isfile(dim):
                print('({}/ {}) Now ingesting scene: {}'.format(ziplist.index(f) + 1, len(ziplist), basename))
                try:
                    p = Popen([gpt, graph, '-Pinfile={}'.format(f), '-Poutfile={}'.format(dim)])
                    p.communicate()
                    if os.path.isfile(dim):
                        print('Ingest successful. Archiving {}'.format(f))
                        shutil.move(f, os.path.join(archdir, os.path.basename(f)))
                    else:
                        ieo.logerror(f, 'File not properly processed.')
                except Exception as e:
                    ieo.logerror(f, e)

def importespatotiles(f, *args, **kwargs):
    # This function imports new ESPA-process LEDAPS data
    # Version 1.1.1: Landsat Collection 1 Level 2 data now supported
    overwrite = kwargs.get('overwrite', False)
    noupdate = kwargs.get('noupdate', False)
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
    tdir = os.path.join(outputdir, projacronym)
    if not os.path.isdir(tdir):
        os.mkdir(tdir)
    if isinstance(filelist, int) or len(filelist) == 0:
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
        ProductID = os.path.basename(xml[0]).replace('.xml', '') # Modified from sceneID in 1.1.1: sceneID will now be read from S1shp
    elif basename[:1] == 'L' and len(basename) > 40:
        ProductID = basename[:40]
    else:
        print('No XML file found, returning.')
        logerror(f, 'No XML file found.')
        return

    # open landsat shapefile (starting version 1.1.1)
    sceneid = None
    driver = ogr.GetDriverByName("GPKG")
    data_source = driver.Open(catgpkg, 1) # opened with write access as LEDAPS data will be updated
    layer = data_source.GetLayer(S1shp)
    ldefn = layer.GetLayerDefn()
    schema = [ldefn.GetFieldDefn(n).name for n in range(ldefn.GetFieldCount())]
    if not 'Tile_filename_base' in schema: # this will add two fields to the s
        tilebasefield = ogr.FieldDefn('Tile_filename_base', ogr.OFTString)
        layer.CreateField(tilebasefield)
    layer.StartTransaction()
    while not sceneid:
        feat = layer.GetNextFeature()
        if ProductID == feat.GetField('Landsat_Product_ID'):
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
            out_raster = os.path.join(tdir, '{}_cfmask.dat'.format(ProductID))
        else:
            out_raster = os.path.join(tdir, '{}_cfmask.dat'.format(sceneid))
        if not os.path.exists(out_raster):
            print('Reprojecting {} Fmask to {}.'.format(sceneid, projection))
            reproject(in_raster, out_raster, sceneid = sceneid, rastertype = 'Fmask')
        masktype = 'Fmask'
#        if feat.GetField('Fmask_path') != out_raster:
#            feat.SetField('Fmask_path', out_raster)
        if feat.GetField('MaskType') != masktype:
            feat.SetField('MaskType', masktype)
            layer.SetFeature(feat)
        qafile = out_raster
        feat = converttotiles(out_raster, fmaskdir, 'Fmask', pixelqa = False, feature = feat, overwrite = overwrite, noupdate = noupdate)
    # Pixel QA layer
    in_raster = os.path.join(outputdir, '{}_pixel_qa.{}'.format(ProductID, ext))
    if os.access(in_raster, os.F_OK):
        if useProdID:
            out_raster = os.path.join(tdir, '{}_pixel_qa.dat'.format(ProductID))
        else:
            out_raster = os.path.join(tdir, '{}_pixel_qa.dat'.format(sceneid))
        if not os.path.isfile(out_raster):
            print('Reprojecting {} Pixel QA layer to {}.'.format(sceneid, projection))
            reproject(in_raster, out_raster, sceneid = sceneid, rastertype = 'pixel_qa')
        masktype = 'Pixel_QA'
#        if feat.GetField('PixQA_path') != out_raster:
#            feat.SetField('PixQA_path', out_raster)
#        mt = feat.GetField('MaskType')
#        if not mt:
#            mt = '0'
        if feat.GetField('MaskType') != masktype:
            feat.SetField('MaskType', masktype)
            layer.SetFeature(feat)
        qafile = out_raster
        feat = converttotiles(out_raster, pixelqadir, 'pixel_qa', pixelqa = False, feature = feat, overwrite = overwrite, noupdate = noupdate)
        layer.SetFeature(feat)
        
    # Surface reflectance data
    if useProdID:
        out_itm = os.path.join(tdir,'{}_ref_{}.dat'.format(ProductID, projacronym))
    else:
        out_itm = os.path.join(tdir,'{}_ref_{}.dat'.format(sceneid, projacronym))
#    if not os.path.isfile(out_itm):
    print('Compositing surface reflectance bands to single file.')
    srlist = []
    out_raster = os.path.join(outputdir, '{}.vrt'.format(sceneid))  # no need to update to ProductID for now- it is a temporary file
    if not os.path.isfile(out_raster):
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
#        feat.SetField('SR_path', out_itm) # Update LEDAPS info in shapefile
    feat = converttotiles(out_itm, srdir, 'ref', pixelqa = True, overwrite = overwrite, feature = feat, noupdate = noupdate)
    layer.SetFeature(feat)

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
        feat = converttotiles(BT_ITM, btdir, rastertype, pixelqa = True, feature = feat, overwrite = overwrite, noupdate = noupdate)
        layer.SetFeature(feat)
#        if feat.GetField('BT_path') != BT_ITM:
#            feat.SetField('BT_path', BT_ITM)

    # Calculate EVI and NDVI
    print('Processing vegetation indices.')
    if useProdID:
        evibasefile = '{}_EVI.dat'.format(ProductID)
    else:
        evibasefile = '{}_EVI.dat'.format(sceneid)
    evifile = os.path.join(tdir, evibasefile)
    ndvifile = os.path.join(tdir, evibasefile.replace('_EVI', '_NDVI'))
    if not os.path.isfile(evifile):
        try:
            calcvis(out_itm, qafile = qafile)
            feat = converttotiles(ndvifile, ndvidir, 'NDVI', pixelqa = True, feature = feat, overwrite = overwrite, noupdate = noupdate)
            layer.SetFeature(feat)
            feat = converttotiles(evifile, evidir, 'EVI', pixelqa = True, feature = feat, overwrite = overwrite, noupdate = noupdate)
            layer.SetFeature(feat)
        except Exception as e:
            print('An error has occurred calculating VIs for scene {}:'.format(sceneid))
            print(e)
            logerror(out_itm, e)
#    if os.path.isfile(evifile) and feat.GetField('EVI_path') != evifile:
#        feat.SetField('EVI_path', evifile)
#    if os.path.isfile(ndvifile) and feat.GetField('NDVI_path') != ndvifile:
#        feat.SetField('NDVI_path', ndvifile)

    # Set feature in shapefile to preserve processed file metadata
    print('Updating information in shapefile.')
#    layer.SetFeature(feat)
    layer.CommitTransaction()
    data_source = None # Close the shapefile

    # Clean up files.

    if basename.endswith('.tar.gz'):
        print('Moving {} to archive: {}'.format(basename, archdir))
        if not os.access(os.path.join(archdir, os.path.basename(f)), os.F_OK):
            shutil.move(f, archdir)
    if remove:
        print('Cleaning up files in directory.')
        for d in [tdir, outputdir]:
            filelist = glob.glob(os.path.join(d, '{}*.*'.format(sceneid)))
            try:
                for fname in filelist:
                    if os.access(fname, os.F_OK):
                        os.remove(fname)
                os.rmdir(d)
            except Exception as e:
                print('An error has occurred cleaning up files for scene {}:'.format(sceneid))
                print(e)
                logerror(f, e)

    print('Processing complete for scene {}.'.format(sceneid))

def converttotiles(infile, outdir, rastertype, *args, **kwargs):
    # This function converts existing data to NTS tiles
    # Code addition started on 11 July 2019
    # includes code from https://gis.stackexchange.com/questions/220844/get-field-names-of-shapefiles-using-gdal
    inshp = kwargs.get('inshape', S1shp) #input shapefile containing data inventory
    tileshp = kwargs.get('tileshp', NTS) 
    rewriteheader = kwargs.get('rewriteheader', True)
    overwrite = kwargs.get('overwrite', True) # overwrite existing files without updating, deleting any tiles first.
    noupdate = kwargs.get('noupdate', False) # if set to True, will not update existing tiles with new data.
    feat = kwargs.get('feature', None)
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
    driver = ogr.GetDriverByName("GPKG")
    if not feat:
        data_source = driver.Open(catgpkg, 1) # opened with write access as LEDAPS data will be updated
        layer = data_source.GetLayer(S1shp)
        closeinfunc = True
    else:
        closeinfunc = False
        
#        tilesfield = ogr.FieldDefn('tiles', ogr.OFTString)
#        layer.CreateField(tilesfield)
    indir, inbasename = os.path.split(infile)
    sceneid = inbasename[:21] # optimised now for Landsat. Must change for IEO 2.0
    outbasename = '{}_{}'.format(inbasename[:3], inbasename[9:16])
    
    tile_ds = driver.Open(ieogpkg, 0)
    tilelayer = tile_ds.GetLayer(NTS)
    
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
    
    fieldnamedict = {'Fmask' : 'Fmask_tiles',
        'pixel_qa' : 'Pixel_QA_tiles',
        'Landsat TIR' : 'Brightness_temperature_tiles', #[, 'Landsat Band6'],
        'Landsat Band6' : 'Brightness_temperature_tiles', #[, ],
        'ref' : 'Surface_reflectance_tiles', #['Landsat TM', 'Landsat ETM+', 'Landsat OLI', 'Sentinel-2'],
        'NDVI' : 'NDVI_tiles',
        'EVI' : 'EVI_tiles'}
    fieldname = None
    if rastertype in fieldnamedict.keys():
         fieldname = fieldnamedict[rastertype]
    found = False
    if not feat:
        while not found:
            feat = layer.GetNextFeature()
            if len(sceneids) > 0:
                sid = sceneids[0]
            else:
                sid = sceneid
            if sid == feat.GetField('sceneID'):
                found = True
        if not found:
            print('ERROR: Feature for SceneID {} not found in ieo.S1shp.'.format(sceneid))
            ieo.logerror(sceneid, 'ERROR: Feature not found in ieo.S1shp.')
            return None
    else: 
        sid = sceneid
    tilebaseset = False
    setfieldnamestr = False
    tilebasestr = feat.GetField('Tile_filename_base')
    if fieldname:
        fieldnamestr = feat.GetField(fieldname)
    
    for tile in tilelayer:
        tilegeom = tile.GetGeometryRef()
        tilename = tile.GetField('Tile')
#                print(tilename)
#                print(tilegeom.Intersect(featgeom))
        if tilegeom.Intersect(rasterGeometry):
            print('Now creating tile {} of type {} for SceneID {}.'.format(tilename, rastertype, sid))
            result = makerastertile(tile, src_ds, gt, outdir, outbasename, infile, rastertype, SceneID = sid, rewriteheader = rewriteheader, acqtime = acqtime, noupdate = noupdate, overwrite = overwrite)
            if result:  
                if not tilebasestr == outbasename and not tilebaseset:
                    feat.SetField('Tile_filename_base', outbasename)
                    tilebaseset = True
                if fieldname:
                    
                    if not fieldnamestr:
                        fieldnamestr = ''
                    if not tilename in fieldnamestr:
                        if len(fieldnamestr) == 0:
                            fieldnamestr = tilename
                        else:
                            fieldnamestr += ',{}'.format(tilename)
                        setfieldnamestr = True
    
    if setfieldnamestr:
        feat.SetField(fieldname, fieldnamestr)
    if closeinfunc:
        layer.SetFeature(feat)
    
    del tile_ds
    if not closeinfunc:
        return feat
    else:
        del src_ds        
        del pixelqadata
        del data_source
        return None


def makerastertile(tile, src_ds, gt, outdir, outbasename, inrastername, rastertype, *args, **kwargs):
    # Adapted from IForDEO code starting on 9 July 2019
    # This function only processes individual tiles, and should be called from another function
    # raster tile and pixelQA files should be VRTs
    # optional pixelQA data should be an array of zeroes and ones, where pixels that equal one are included for processing. Valid pixels will be identified as clear land, water, or snow/ice.
    overwrite = kwargs.get('overwrite', False) # setting this option will delete an existing raster tile
    update = kwargs.get('update', True) # setting this option will replace pixels with no data with any new data.
    SceneID = kwargs.get('SceneID', None)
    rewriteheader = kwargs.get('rewriteheader', True)
    acqtime = kwargs.get('acqtime', None)
    noupdate = kwargs.get('noupdate', False) # This will prevent the function from updating the tile with new data
    overwrite = kwargs.get('overwrite', False) # This will delete any existing tile data
    tilename = tile.GetField('Tile')
    tilegeom = tile.GetGeometryRef()
    outfile = os.path.join(outdir, '{}_{}.dat'.format(outbasename, tilename))
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
                outheaderdict = readenvihdr(outfile.replace('.dat', '.hdr'))
                parentrasters = outheaderdict['parent rasters']
                if not os.path.basename(inrastername) in parentrasters:
                    parentrasters.append(os.path.basename(inrastername))
                else:
                    print('This scene has already been ingested into the tile. Skipping.')
                    return False
    #        else:
    #            outheaderdict = headerdict['default'].copy()
        else:
            parentrasters = makeparentrastersstring([os.path.basename(inrastername)])
    
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
        if isinstance(parentrasters, list):
            pr = parentrasters[0]
            if len(parentrasters) > 0:
                for i in range(1, len(parentrasters)):
                    pr += ',{}'.format(parentrasters[i])
            parentrasters = pr
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

def S1vrtforimport(dirname):
    filelist = glob.glob(os.path.join(dirname, '*.img'))
    polorder = ['HH', 'HV', 'VV', 'VH']
    dorder = ['Beta', 'Gamma', 'Sigma']
    proclist = []
    blist = []
    out_raster = '{}.vrt'.format(dirname[:-5])  # no need to update to ProductID for now- it is a temporary file
    if not os.path.exists(out_raster):
        mergelist = ['gdalbuildvrt', '-separate', out_raster]
    if len(filelist)> 0:
        for d in dorder:
            for pol in polorder:
                imagename = os.path.join(dirname,'{}0_{}.img'.format(d, pol))
                if any(imagename in f for f in filename):
                    print('Adding {} band to the processing list.'.format(d, pol))
                    mergelist.append(imagename)
                    blist.append('{}0_{}'.format(d, pol))
        if len(blist) > 0:
            print('Compositing {} bands to single file.'.format(len(blist)))
            p = Popen(mergelist)
            print(p.communicate())
            return out_raster, blist
    else:
        return False, None

def parseDIMAP(f):
    
    
    