    # =============================================================================
# !/usr/bin/env python3
# 
# Guy Serbin, EOanalytics Ltd.
# Talent Garden Dublin, Claremont Ave. Glasnevin, Dublin 11, Ireland
# email: guyserbin <at> eoanalytics <dot> ie
# 
# version 1.5
# 
# This script will create and update a geopackage layer of all available Sentinel-2 scenes, including available metadata
# Modified from updatelandsat.py
# 
# Changes:
#     
# 23 May 2018: XML functionality deprecated in favor of JSON queries, as the former is no longer available or efficient
# 25 March 2019: This script will now read configuration data from ieo.ini
# 14 August 2019: This now creates and updates a layer within a geopackage, and will migrate data from an old shapefile to a new one
# 12 January 2021: Modified to support Landsat Collection 2
# 23 April 2021: Refactored as version 2.0 by Sean O'Riogain
# 07 May 2021: Version 2.1: Sean O'Riogain modified version 2.0 to use API v1.5, improve progress updates, and validate arguments in main() 
# 06 July 2021: Reversioned to 1.5. Added query logging to readUSGS()
# 02 November 2021: updateSentinel2.py created from updatelandsat.py code
# =============================================================================

# =============================================================================
# Import the needed modules
# =============================================================================

import argparse, datetime, getpass, json, math, os, requests, shutil, sys

import xml.etree.ElementTree as et

from osgeo import ogr, osr

from PIL import Image

from urllib.parse import urlparse

# As the ieo module is typically in a directory other than the working directory, 
#    we must prompt for its path
try:
    import ieo
except:
    # ieodir = os.getenv('IEO_INSTALLDIR')
    # if not ieodir:
    ieodir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    # print('Error: IEO failed to load. Please input the location of the directory containing the IEO installation files.')
    # ieodir = input('IEO installation path: ')
    if os.path.isfile(os.path.join(ieodir, 'ieo.py')):
        sys.path.append(ieodir)
        import ieo, S3ObjectStorage
    else:
        print('Error: that is not a valid path for the IEO module. Exiting.')
        sys.exit()
        
# =============================================================================
# Close a connection to the specified USGS EarthExplorer service (catalog) #
# =============================================================================

def closeUSGS(baseURL, version, headers):
    
    URL = '{}{}/logout'.format(baseURL, version)
   
    response = requests.post(URL, headers  = headers)
        
    # Handle any HTTP error encountered
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        ieo.logerror(URL, ''.join(map(str, e.args)).split(',')[0])
        sys.exit(e)
    
    return

# =============================================================================
# Download a thumbnail jpg file from the USGS
# =============================================================================

def dlthumb(dlurl, jpg):
    
    # Download the thumbnail jpg
    try: # Trap for timeout (max retries exceeded)
        r = requests.get(dlurl, allow_redirects=True)
    except requests.exceptions.RequestException as e:
        raise SystemExit(e)    
        
    # Handle any HTTP error encountered
    try:
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        ieo.logerror(dlurl, ''.join(map(str, e.args)).split(',')[0])
        sys.exit(e)    
    
    # Create and populate the thumbnail jpg file
    with open(jpg, 'wb') as file:
        file.write(r.content)

    return

def scanMTDfile(f, *args, **kwargs):
    # featuredict = kwargs.get('featuredict', {})
    verbose = kwargs.get('verbose', False)
    if verbose: print(f'Now parsing file: {f}')
    fieldlist = ['PRODUCT_START_TIME', 'PRODUCT_STOP_TIME', 'PRODUCT_URI', 'PRODUCT_URI_2A', 'PROCESSING_LEVEL', 'PRODUCT_TYPE', 'PROCESSING_BASELINE', 'GENERATION_TIME', 'PREVIEW_IMAGE_URL', 'PREVIEW_GEO_INFO', 'SPACECRAFT_NAME', 'DATATAKE_TYPE', 'DATATAKE_SENSING_START', 'SENSING_ORBIT_NUMBER', 'SENSING_ORBIT_DIRECTION', 'PRODUCT_FORMAT', 'RASTER_CS_TYPE', 'PIXEL_ORIGIN', 'GEO_TABLES', 'HORIZONTAL_CS_TYPE', 'SNOW_CLIMATOLOGY_MAP', 'ESACCI_WaterBodies_Map', 'ESACCI_LandCover_Map', 'ESACCI_SnowCondition_Map_Dir', 'Cloud_Coverage_Assessment', 'DEGRADED_ANC_DATA_PERCENTAGE', 'DEGRADED_MSI_DATA_PERCENTAGE', 'NODATA_PIXEL_PERCENTAGE', 'SATURATED_DEFECTIVE_PIXEL_PERCENTAGE', 'DARK_FEATURES_PERCENTAGE', 'CLOUD_SHADOW_PERCENTAGE', 'VEGETATION_PERCENTAGE', 'NOT_VEGETATED_PERCENTAGE', 'WATER_PERCENTAGE', 'UNCLASSIFIED_PERCENTAGE', 'MEDIUM_PROBA_CLOUDS_PERCENTAGE', 'HIGH_PROBA_CLOUDS_PERCENTAGE', 'THIN_CIRRUS_PERCENTAGE', 'SNOW_ICE_PERCENTAGE', 'RADIATIVE_TRANSFER_ACCURACY', 'WATER_VAPOUR_RETRIEVAL_ACCURACY', 'AOT_RETRIEVAL_ACCURACY']
    intlist = ['SENSING_ORBIT_NUMBER', 'ORBIT_NUMBER']
    floatlist = ['Cloud_Coverage_Assessment', 'DEGRADED_ANC_DATA_PERCENTAGE', 'DEGRADED_MSI_DATA_PERCENTAGE', 'NODATA_PIXEL_PERCENTAGE', 'SATURATED_DEFECTIVE_PIXEL_PERCENTAGE', 'DARK_FEATURES_PERCENTAGE', 'CLOUD_SHADOW_PERCENTAGE', 'VEGETATION_PERCENTAGE', 'NOT_VEGETATED_PERCENTAGE', 'WATER_PERCENTAGE', 'UNCLASSIFIED_PERCENTAGE', 'MEDIUM_PROBA_CLOUDS_PERCENTAGE', 'HIGH_PROBA_CLOUDS_PERCENTAGE', 'THIN_CIRRUS_PERCENTAGE', 'SNOW_ICE_PERCENTAGE', 'RADIATIVE_TRANSFER_ACCURACY', 'WATER_VAPOUR_RETRIEVAL_ACCURACY', 'AOT_RETRIEVAL_ACCURACY']
    tree = et.parse(f)
    root = tree.getroot()
    
    featuredict = walk_tree_recursive(root, fieldlist, intlist, floatlist)
    return featuredict


def walk_tree_recursive(root, fieldlist, intlist, floatlist, *args, **kwargs):
    # code modified from https://stackoverflow.com/questions/28194703/recursive-xml-parsing-python-using-elementtree
    coordfieldname = kwargs.get('coordfieldname', 'EXT_POS_LIST')
    outdict = kwargs.get('outdict', {})
    #do whatever with .tags here
    for child in root:
        outdict = walk_tree_recursive(child, fieldlist, intlist, floatlist, outdict = outdict)
        if child.tag in fieldlist:
            if child.tag in intlist:
                outdict[child.tag] = int(child.text)
            elif child.tag in floatlist:
                outdict[child.tag] = float(child.text)
            else:
                outdict[child.tag] = child.text
        elif child.tag == coordfieldname:
            coords = []
            s = child.text.split()
            numcoords = int(len(s) / 2)
            for i in range(numcoords):
                coords.append([float(s[i * 2]), float(s[i * 2 + 1])])
            outdict['coords'] = coords
    return outdict


def makeS2feature(f, *args, **kwargs):
    source = osr.SpatialReference() # Lat/Lon WGS-64
    source.ImportFromEPSG(4326)
    transform = osr.CoordinateTransformation(source, ieo.prj) 
    ring = ogr.Geometry(ogr.wkbLinearRing)
    featuredict = scanMTDfile(f)
    for coord in featuredict['coords']:
        ring.AddPoint(coord[0], coord[1])
    # Create polygon
    poly = ogr.Geometry(ogr.wkbPolygon)
    poly.AddGeometry(ring)
    featuredict['WKT'] = poly.ExportToWkt()
    poly.Transform(transform)
    featuredict['poly'] =  poly
    return featuredict
     
def makeS2polygon(f, *args, **kwargs):
    source = osr.SpatialReference() # Lat/Lon WGS-64
    source.ImportFromEPSG(4326)
    transform = osr.CoordinateTransformation(source, ieo.prj) 
    ring = ogr.Geometry(ogr.wkbLinearRing)
    featuredict = scanMTDfile(f)
    if verbose_g: print(f'Total points for polygon: {len(featuredict["coords"])}')
    for coord in featuredict['coords']:
        ring.AddPoint(coord[0], coord[1])
    # Create polygon
    poly = ogr.Geometry(ogr.wkbPolygon)
    poly.AddGeometry(ring)
    featuredict['WKT'] = poly.ExportToWkt()
    poly.Transform(transform)
    # featuredict['poly'] =  poly
    produri = 'PRODUCT_URI_2A'
    for tag in ['PRODUCT_URI_2A', 'PRODUCT_URI']:
        if tag in featuredict.keys():
            produri = tag
            break
    featuredict['ProductID'] = featuredict[produri][:-5]
    if len(featuredict['ProductID']) > 60:
        featuredict['ProductID'] = featuredict['ProductID'][:60]
    if verbose_g: print(f'ProductID: {len(featuredict["ProductID"])}')
    parts = featuredict['ProductID'].split('_')
    featuredict['acquisitionDate'] = datetime.datetime.strptime(parts[2], '%Y%m%dT%H%M%S')
    featuredict['sceneID'] = f'{parts[0]}{parts[5]}{featuredict["acquisitionDate"].strftime("%Y%j")}ESA00' # Creates a fake USGS-like Scene Identifier
    featuredict['MGRS'] = parts[5][1:]
    if verbose_g: print(f'sceneID: {len(featuredict["sceneID"])}')
    return featuredict, poly
     
def Sen2updateIEO(f, layer, bucket, bucketpath, *args, **kwargs):
    if verbose_g: print('Parsing XML data and creating polygon.')
    featuredict, poly = makeS2polygon(f)
    if verbose_g: print('Creating feature in layer.')
    feature = ogr.Feature(layer.GetLayerDefn())
    
    # Add field attributes
    if verbose_g: print('Setting field attributes.')
    for fieldname in featuredict.keys():
        if not fieldname in ['poly', 'coords']:
            if fieldname.endswith('Date') or fieldname.endswith('TIME') or fieldname == 'DATATAKE_SENSING_START':
                if isinstance(featuredict[fieldname], str):
                    if '.' in featuredict[fieldname]:
                        formatstr = '%Y-%m-%dT%H:%M:%S.%fZ'
                    else:
                        formatstr = '%Y-%m-%dT%H:%M:%SZ'
                    featuredict[fieldname] = datetime.datetime.strptime(featuredict[fieldname], formatstr)
                feature.SetField(fieldname, featuredict[fieldname].strftime('%Y-%m-%d %H:%M:%S')) #\
                                             # featuredict[fieldname].year, \
                                             # featuredict[fieldname].month, \
                                             # featuredict[fieldname].day, \
                                             # featuredict[fieldname].hour, \
                                             # featuredict[fieldname].minute, \
                                             # featuredict[fieldname], 100)
            else:
                feature.SetField(fieldname, featuredict[fieldname])
    
    feature.SetField('S3_endpoint_URL',  S3ObjectStorage.url)
    feature.SetField('S3_ingest_bucket', bucket)
    feature.SetField('S3_endpoint_path', bucketpath)
    now = datetime.datetime.now()
    feature.SetField('Metadata_Ingest_Time', now.strftime('%Y-%m-%d %H:%M:%S')) #now.year, \
                                             # now.month, \
                                             # now.day, \
                                             # now.hour, \
                                             # now.minute, \
                                             # now.second, 100)
    feature.SetGeometry(poly)
            
    # Create the new feature
    layer.CreateFeature(feature)
    if verbose_g: print('Feature created.')
    # Free the new features' resources 
    feature.Destroy()    
    
    return layer


# =============================================================================
# Create the Minimum Bounding Rectangle (MBR) for JSON queries
# =============================================================================

def getMBR(baseURL, version, paths, rows):
    
    # Declare/initialise the required local data objects
    URL = '{}{}/grid2ll'.format(baseURL, version)
    
    Xcoords = []
    Ycoords = []

    # Express the MBR coordinates in path-row terms
    prs = [[min(paths), min(rows)], [min(paths), max(rows)], \
           [max(paths), max(rows)], [max(paths), min(rows)]]

    # Convert the path-row coordinates to lat-long coordinates
    for pr in prs:
        print('Requesting coordinates for WRS-2 Path {} Row {}.'.format(pr[0], pr[1]))
        jsonRequest = json.dumps({"gridType" : "WRS2", "responseShape" : "point", "path" : str(pr[0]), "row" : str(pr[1])}).replace(' ','')
   
        response = requests.post(URL, jsonRequest)
            
        # Handle any HTTP error encountered
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            ieo.logerror(URL, ''.join(map(str, e.args)).split(',')[0])
            sys.exit(e)
            
        json_data = json.loads(response.text)
        
        # API version problem?  Log error and exit
        if json_data['errorCode']:
            print(json_data['api_version'], json_data['error']) 
            ieo.logerror(json_data['api_version'] + ':', json_data['error'])
            sys.exit()
    
        Xcoords.append(float(json_data["data"]["coordinates"][0]["longitude"]))
        Ycoords.append(float(json_data["data"]["coordinates"][0]["latitude"]))

    return [min(Ycoords), min(Xcoords), max(Ycoords), max(Xcoords)]

# =============================================================================
# Get the WRS-2 Path-Row Numbers
# =============================================================================

def getPathsRows(useWRS2, pathrowvals):
    
    # Declare the lists of path, row and path-row numbers to be returned
    paths, rows, pathrowstrs = ([] for i in range(3))
    
    # Use the geopackage specified by WRS2 setting in the VECTOR section of the 
    #    ieo.ini to construct the path-row number lists?
    if useWRS2.lower() == 'yes':
        print('Getting WRS-2 Path/Row combinations from geopackage: {}'.format(ieo.WRS2))
        if not ieo.usePostGIS:
            driver = ogr.GetDriverByName("GPKG")
            ds = driver.Open(ieo.ieogpkg, 0)
        else:
            ds = ogr.Open(ieo.ieogpkg, 0)
        layer = ds.GetLayer(ieo.WRS2)
        
        for feature in layer:
            path = feature.GetField('PATH')
            if not path in paths:
                paths.append(path)
            row = feature.GetField('ROW')
            if not row in rows:
                rows.append(row)
            pathrowstrs.append('{:03d}{:03d}'.format(path, row))
            
        ds = None
        
    # Use the path and row number ranges configured in the updateshp.ini file?
    else:
        print('Using WRS-2 Path/Row combinations from INI file.')
        pathrowvals = pathrowvals.split(',')
        iterations = int(len(pathrowvals) / 4)
        
        for i in range(iterations):
            
            for j in range(int(pathrowvals[i * 4]), int(pathrowvals[i * 4 + 1]) + 1):
                if not j in paths:
                    paths.append(j)
                    
                for k in range(int(pathrowvals[i * 4 + 2]), int(pathrowvals[i * 4 + 3]) + 1):
                    pathrowstrs.append('{:03d}{:03d}'.format(j, k))
                    if not k in rows:
                        rows.append(k)
                           
    return paths, rows, pathrowstrs

# =============================================================================
# Download the thumbnails images for any new or modified scenes
# =============================================================================

def getThumbnails(layer, updatemissing, badgeom):
    
    # Construct the download target path 
    jpgdir = os.path.join(ieo.catdir, 'Landsat', 'Thumbnails')
    
    # Get the feature count
    featureCount = layer.GetFeatureCount()
    
    # Reposition at the first feature in the geopackage 
    layer.ResetReading()
    
    # Initialise the scene (download) count
    sceneCount = 0
    
    # Loop through the features in the geopackage
    for feat_num, feature in enumerate(layer):
                
        # Retrieve the values of the feature's fields of interest
        sceneID = feature.GetField('sceneID') 
        dlurl = feature.GetField('browseUrl')
        PID = feature.GetField('LANDSAT_PRODUCT_ID')
        
        # Display progress
        print('\rChecking feature {:5d} of {}, scene {}.{}\r'\
              .format(feat_num, featureCount, sceneID, ' ' * 40), end='')
        
        # Specify the thumbail file's path
        basename = '{}.jpg'.format(PID)
        jpg = os.path.join(jpgdir, basename)
        
        # Initialise the file created flag 
        created = False
        
        # Thumbnail does not exist or scene data just updated?: Download thumbnail for scene
        if not os.access(jpg, os.F_OK) or \
            sceneID in badgeom or \
                sceneID in updatemissing:
            print('\rCreating .jpg file for feature {:5d} of {}, scene {}.\r'\
                  .format(feat_num + 1, featureCount, sceneID), end='')

            # Create (download) the thumbnail file (.jpg) and record its path on the feature
            dlthumb(dlurl, jpg)
            feature.SetField('Thumbnail_filename', jpg)
            
            # Set the file created flag
            created = True
            
        # Worldfile does not exist or scene data just updated?:
        if not os.access(jpg.replace('.jpg', '.jpw'), os.F_OK) or \
            sceneID in badgeom or \
                sceneID in updatemissing:
            print('\rCreating .jpw file for feature {:5d} of {}, scene {}.\r'\
                  .format(feat_num + 1, featureCount, sceneID), end='')
                    
            # Create the world file (.jpw)
            geom = feature.GetGeometryRef()
            makeworldfile(jpg, geom)
            
            # Set the file created flag
            created = True
            
        # Increment the scene count
        if created:
            sceneCount += 1

    print('\rCreated thumbnail and/or world files for {} of {} scenes.\n'\
          .format(sceneCount, featureCount))

    return

# =============================================================================
# Create a world file (.jpw) containing the geometry data for the thumbnail 
#     thumbnai(.jpg) image pertaining to a particular Landsat scene so that
#       the thumbnail image can be viewed in a GIS
# =============================================================================

def makeworldfile(jpg, geom):
    
    # Open the thumbnail image file
    img = Image.open(jpg)
    
    # Get its size (in pixels)
    width, height = img.size
    width = float(width)
    height = float(height)
    
    # Get the coordinates of its bounding box
    minX, maxX, minY, maxY = geom.GetEnvelope()
    
    # Extract its filename from its path    
    basename = os.path.basename(jpg)

    # Landsat 7 thumbnail?: Need the coordinates to derive the worldfile data  
    if basename[:3] == 'LE7':
        wkt = geom.ExportToWkt()
        start = wkt.find('(') + 2
        end = wkt.find(')')
        vals = wkt[start:end]
        vals = vals.split(',')
        corners = []
        
        for val in vals:
            val = val.split()
            
            for v in val:
                corners.append(float(v))
                
        A = (maxX - corners[0]) / width
        B = (corners[0] - minX) / height
        C = corners[0]
        D = (maxY - corners[3]) / width
        E = (corners[3] - minY) / height
        F = corners[1]
    
    # Not Landsat 7 thumbnail?: Can derive the worldfile data using image size and bounding box only
    else:
        A = (maxX - minX) / width
        B = 0.0
        C = minX
        D = (maxY - minY) / height
        E = 0.0
        F = maxY
        
    # Derive the worldfile's path from that of the thumbnail
    jpw = jpg.replace('.jpg', '.jpw')
    
    # Worldfile already exists?: Rename it as a backup 
    if os.access(jpw, os.F_OK):
        bak = jpw.replace('.jpw', '.jpw.{}.bak'\
                          .format(datetime.datetime.today().strftime('%Y%m%d-%H%M%S')))
        shutil.move(jpw, bak)
        
    # Open a new worldfile and populate it with data    
    with open(jpw, 'w') as file:
        file.write('{}\n-{}\n-{}\n-{}\n{}\n{}\n'.format(A, D, B, E, C, F))
    
    # Delete the thumbnail data from memory
    del img
    
    return  

# =============================================================================
# Open the IEO geopackage that stores its Landsat scene metadata
#    - and create it if it does not already exist
# =============================================================================

def openIEO():
 
    # If the required geopackage does not exist, create it; otherwise, open it.
    if not ieo.usePostGIS:
        driver = ogr.GetDriverByName("GPKG")
        
        if not os.access(ieo.catgpkg, os.F_OK):
            data_source = driver.CreateDataSource(ieo.catgpkg)
        else:
            data_source = driver.Open(ieo.catgpkg, 1)
    else:
        data_source = ogr.Open(ieo.catgpkg, 1)   

    # Set up the defintions of the main geopackage fields (attributes)
    fieldlist = ['PRODUCT_START_TIME', 'PRODUCT_STOP_TIME', 'PRODUCT_URI', 'PROCESSING_LEVEL', 'PRODUCT_TYPE', 'PROCESSING_BASELINE', 'GENERATION_TIME', 'PREVIEW_IMAGE_URL', 'PREVIEW_GEO_INFO', 'SPACECRAFT_NAME', 'DATATAKE_TYPE', 'DATATAKE_SENSING_START', 'SENSING_ORBIT_NUMBER', 'SENSING_ORBIT_DIRECTION', 'PRODUCT_FORMAT', 'RASTER_CS_TYPE', 'PIXEL_ORIGIN', 'GEO_TABLES', 'HORIZONTAL_CS_TYPE', 'SNOW_CLIMATOLOGY_MAP', 'ESACCI_WaterBodies_Map', 'ESACCI_LandCover_Map', 'ESACCI_SnowCondition_Map_Dir', 'Cloud_Coverage_Assessment', 'DEGRADED_ANC_DATA_PERCENTAGE', 'DEGRADED_MSI_DATA_PERCENTAGE', 'NODATA_PIXEL_PERCENTAGE', 'SATURATED_DEFECTIVE_PIXEL_PERCENTAGE', 'DARK_FEATURES_PERCENTAGE', 'CLOUD_SHADOW_PERCENTAGE', 'VEGETATION_PERCENTAGE', 'NOT_VEGETATED_PERCENTAGE', 'WATER_PERCENTAGE', 'UNCLASSIFIED_PERCENTAGE', 'MEDIUM_PROBA_CLOUDS_PERCENTAGE', 'HIGH_PROBA_CLOUDS_PERCENTAGE', 'THIN_CIRRUS_PERCENTAGE', 'SNOW_ICE_PERCENTAGE', 'RADIATIVE_TRANSFER_ACCURACY', 'WATER_VAPOUR_RETRIEVAL_ACCURACY', 'AOT_RETRIEVAL_ACCURACY']
    intlist = ['SENSING_ORBIT_NUMBER', 'ORBIT_NUMBER']
    floatlist = ['Cloud_Coverage_Assessment', 'DEGRADED_ANC_DATA_PERCENTAGE', 'DEGRADED_MSI_DATA_PERCENTAGE', 'NODATA_PIXEL_PERCENTAGE', 'SATURATED_DEFECTIVE_PIXEL_PERCENTAGE', 'DARK_FEATURES_PERCENTAGE', 'CLOUD_SHADOW_PERCENTAGE', 'VEGETATION_PERCENTAGE', 'NOT_VEGETATED_PERCENTAGE', 'WATER_PERCENTAGE', 'UNCLASSIFIED_PERCENTAGE', 'MEDIUM_PROBA_CLOUDS_PERCENTAGE', 'HIGH_PROBA_CLOUDS_PERCENTAGE', 'THIN_CIRRUS_PERCENTAGE', 'SNOW_ICE_PERCENTAGE', 'RADIATIVE_TRANSFER_ACCURACY', 'WATER_VAPOUR_RETRIEVAL_ACCURACY', 'AOT_RETRIEVAL_ACCURACY']
    
    fieldvaluelist = [
        ['ProductID', ogr.OFTString, 60],
        ['sceneID', ogr.OFTString, 21],
        ['acquisitionDate', ogr.OFTDateTime, 0],
        ['MGRS', ogr.OFTString, 5],
        ]
    for fieldname in fieldlist:
        if fieldname in intlist:
            pair = [fieldname, ogr.OFTInteger, 0]
        elif fieldname in floatlist:
            pair = [fieldname, ogr.OFTReal, 0]
        elif fieldname.endswith('TIME') or fieldname == 'DATATAKE_SENSING_START':
            pair = [fieldname, ogr.OFTDateTime, 0]
        else:
            pair = [fieldname, ogr.OFTString, 0]
        fieldvaluelist.append(pair)
    appendlist = [
        ['WKT', ogr.OFTString, 0],
        ['MaskType', ogr.OFTString, 0],
        ['Thumbnail_filename', ogr.OFTString, 0],
        ['S3_endpoint_URL', ogr.OFTString, 0],
        ['S3_ingest_bucket', ogr.OFTString, 0],
        ['S3_endpoint_path', ogr.OFTString, 0],
        ['S3_tile_bucket', ogr.OFTString, 0],
        ['Metadata_Ingest_Time', ogr.OFTDateTime, 0],
        ['Raster_Ingest_Time', ogr.OFTDateTime, 0],
        ['Surface_reflectance_tiles', ogr.OFTString, 0],
        ['Brightness_temperature_tiles', ogr.OFTString, 0],
        ['Surface_temperature_tiles', ogr.OFTString, 0],
        ['Pixel_QA_tiles', ogr.OFTString, 0],
        ['Radsat_QA_tiles', ogr.OFTString, 0],
        ['Aerosol_QA_tiles', ogr.OFTString, 0],
        ['NDVI_tiles', ogr.OFTString, 0],
        ['EVI_tiles', ogr.OFTString, 0],
        ['NDTI_tiles', ogr.OFTString, 0],
        ['NBR_tiles', ogr.OFTString, 0],
        ['Tile_filename_base', ogr.OFTString, 0],
        ]
    for item in appendlist:
        fieldvaluelist.append(item)

    # Get list of field names (expected)
    fnames = []
    
    for element in fieldvaluelist:
        fnames.append(element[0])

    # Check if the geopackage contains a layer, and create it, and its fields
    #    (attributes), if it does not
    layernames = []
    layers = data_source.GetLayerCount()
    if layers > 0:
        for i in range(layers):
            layername = data_source.GetLayer(i).GetName()
            layernames.append(layername)
            if verbose_g:
                print(f'Found layer: {layername}')
    else:
        print('No layers found.')
    
    # No layer found?
    if not ieo.Sen2shp in layernames:
        print(f'Creating layer {ieo.Sen2shp} in: {ieo.catgpkg}')
        layer = data_source.CreateLayer(ieo.Sen2shp, ieo.prj, ogr.wkbPolygon)
        
        for element in fieldvaluelist:
            if verbose_g: print(f'Creating field {element[0]} of type {element[1]}, with width {element[2]}.')
            field_name = ogr.FieldDefn(element[0], element[1])
            if element[2] > 0:
                field_name.SetWidth(element[2])
            layer.CreateField(field_name)
    
    # One layer found?
    else:
        i = layernames.index(ieo.Sen2shp)
        layer_name = data_source.GetLayer(i).GetName()
        if verbose_g: print(f'Opening layer: {layer_name}')
        # Check the name of the layer is that specified in the config (.ini) file
        # if layer_name != ieo.Sen2shp:                
        #     ieo.logerror(ieo.catgpkg, \
        #                  'Geopackage layer name is incorrect (expected={}; actual={}).'.format(ieo.catgpkg, layer_name), \
        #                  errorfile = errorfile)
        #     print('Error: Geopackage layer name is incorrect (expected={}; actual={}). Exiting.'.format(ieo.catgpkg, layer_name))
        #     sys.exit()
        # else:   # Layer name is as expected
        layer = data_source.GetLayer(layer_name)
        if verbose_g: print(f'Opened layer: {layer_name}')
        # Get list of field names (actual)  
        layerDefinition = layer.GetLayerDefn()
        if verbose_g: print(f'Getting layer definition: {layer_name}')
        shpfnames = []
        if verbose_g: print(f'Total number of fields currently in {layer_name}: {layerDefinition.GetFieldCount()}')        
        for i in range(layerDefinition.GetFieldCount()):
            shpfname = layerDefinition.GetFieldDefn(i).GetName()
            if verbose_g: print(f'Found fieldname {shpfname}.')
            shpfnames.append(shpfname)
            
        # Find missing fields and create them
        for element in fieldvaluelist:
            if verbose_g: print(f'Analyzing field name: {element[0]}')
            if not element[0] in shpfnames:
                if verbose_g: print(f'Creating missing field {element[0]} of type {element[1]}, with width {element[2]}.')
                # i = fnames.index(fname)
                field_name = ogr.FieldDefn(element[0], element[1])
                if element[2] > 0:
                    field_name.SetWidth(element[2])
                layer.CreateField(field_name)
                    
        print('Catalog opened successfully.')
    
    # More than one layer found?
    # else:
    #     ieo.logerror(ieo.catgpkg, 'More than 1 layer found in geopackage.', errorfile = errorfile)
    #     print('Error: More than 1 layer found in geopackage. Exiting.')
    #     sys.exit()
             
    return data_source, layer, fieldvaluelist, fnames
            
# =============================================================================
# Open a connection to the specified USGS EarthExplorer service (catalog) 
#    using the credentials provided - and returns the connection's token (apiKey)
# =============================================================================

def openUSGS(baseURL, version, catalogID, username, password):
    
    URL = '{}{}/login'.format(baseURL, version)
    print('Logging in using: {}'.format(URL))
    data = json.dumps({'username': username, 'password': password, 'catalog_ID': catalogID})
    
    response = requests.post(URL, data)
        
    # Handle any HTTP error encountered
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        ieo.logerror(URL, ''.join(map(str, e.args)).split(',')[0])
        sys.exit(e)
    
    json_data = json.loads(response.text)
    apiKey = json_data['data']
    
    return apiKey

# =============================================================================
# Open a connection to the specified USGS EarthExplorer service (catalog) 
#    using the credentials provided - and returns the connection's token (apiKey)
# =============================================================================

def queryUSGS(baseURL, version, headers, scenedict, badgeom, updatemissing, \
              fieldvaluelist, querylist, queryfieldnames, savequeries,\
              datasetName):
    # This function is being deprecated in Version 1.5 as it appears to be 
    # redundant for Landsat Collection 2
    
    # Break up queries into blocks of 100 or less scenes
    iterations = math.ceil(len(querylist) / 100) # Number of blocks needed
       
    # =============================================================================
    # Process each of the scene blocks
    # =============================================================================

    for iteration in range(iterations):
        
        # Derive the start and end values of the querylist index for the current
        #    block of scenes
        startval = iteration * 100
        if (iteration + 1) * 100 > len(querylist):
            endval = len(querylist)
        else:
            endval = startval + 100
            
        # =============================================================================
        # Initialise (empty) an USGS/ERS scene list instance called querylist 
        # =============================================================================

        RemoveURL = '{}{}/scene-list-remove'.format(baseURL, version) 

        removeparams = json.dumps({"listId":"querylist"})

        response = requests.post(RemoveURL, data = removeparams, headers = headers)
        
        # Handle any HTTP error encountered
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            ieo.logerror(RemoveURL, ''.join(map(str, e.args)).split(',')[0])
            sys.exit(e)            
            
        # =============================================================================
        # Load the scene list
        # =============================================================================

        AddURL = '{}{}/scene-list-add'.format(baseURL, version) 

        addparams = json.dumps({"listId":"querylist",
                               "datasetName":datasetName,
                               "entityIds":querylist[startval: endval]})

        response = requests.post(AddURL, data = addparams, headers = headers)
        
        # Handle any HTTP error encountered
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            ieo.logerror(AddURL, ''.join(map(str, e.args)).split(',')[0])
            sys.exit(e)
        
        # =============================================================================
        # Execute the USGS/ERS query for the current block of scenes
        # =============================================================================

        print('\rNow querying {:3d} scenes, query {}/{}.\r'.format((endval - startval), iteration + 1, iterations), \
              end='')

        QueryURL = '{}{}/scene-metadata-list'.format(baseURL, version)
            
        queryparams = json.dumps({"datasetName":datasetName,
                    "listId":"querylist",
                    "metadataType":"full"})

        query = requests.post(QueryURL, data = queryparams, headers = headers)
        
        # Handle any HTTP error encountered
        try:
            query.raise_for_status()
        except requests.exceptions.HTTPError as e:
            ieo.logerror(QueryURL, ''.join(map(str, e.args)).split(',')[0])
            sys.exit(e)        
        
        # Save query results?
        if savequeries:
            now = datetime.datetime.now()
            querydir = os.path.join(ieo.logdir, 'json_query_data')
            if not os.path.isdir(querydir):
                os.mkdir(querydir)
            outfile = os.path.join(querydir, 'query_{}_{}.txt'.format(datasetName, now.strftime('%Y%m%d-%H%M%S')))
            with open(outfile, 'w') as output:
                output.write(query.text)

        # =============================================================================
        # Extract and store the required metatdata values from the query results
        # =============================================================================
                
        querydict = json.loads(query.text)
        
        # Data returned by query?
        if len(querydict['data']) > 0:
            
            # Loop through the retrieved data elements/items
            for item in querydict['data']:
                
                # Metadata returned by query?
                if len(item['metadata']) > 0:
                    
                    # Extract the scene ID value
                    if item['metadata'][1]['fieldName'] == 'Landsat Scene Identifier':
                        sceneID = item['metadata'][1]['value']
                    else:
                        for subitem in item['metadata']:
                            if subitem['fieldName']  == 'Landsat Scene Identifier':
                                sceneID = subitem['value']
                                break
                            
                    # Loop through the returned metadata elements/items
                    for subitem in item['metadata']:
                        if subitem['fieldName'] != 'Landsat Scene Identifier':
                            fieldname = subitem['fieldName'].rstrip().lstrip().replace('L-1', 'L1')
                            # fieldname = fieldname.replace(' ', '_')
                        
                        # Metadata element/item required by OEI and not already stored during the scene search phase?
                        if fieldname in queryfieldnames and not fieldname in scenedict[sceneID].keys() and fieldname != 'Landsat Scene Identifier':
                            value = subitem['value']
                            
                            # Value provided for metadata element/item?
                            if value:
                                
                                # Format its value vased on its expected data type
                                i = queryfieldnames.index(fieldname)
                                
                                # Time or Date?
                                if fieldvaluelist[i][3] == ogr.OFTDateTime or fieldname.endswith('Date'):
                                    
                                    if 'Time' in fieldname:
                                        value = datetime.datetime.strptime(value[:-1], '%Y:%j:%H:%M:%S.%f')
                                    elif '/' in value:
                                        value = datetime.datetime.strptime(value, '%Y/%m/%d')
                                    else:
                                        value = datetime.datetime.strptime(value, '%Y-%m-%d')
                                
                                # Numeric (Non-integer)?
                                elif fieldvaluelist[i][3] == ogr.OFTReal:
                                    value = float(value)
                                
                                # Numeric (Integer)?
                                elif fieldvaluelist[i][3] == ogr.OFTInteger:
                                    try:
                                        value = int(value)
                                    except:
                                        print('Error: fieldname {} has a value of {}, changing to -9999.'.format(fieldname, value))
                                        value = -9999
                                
                                # URL? 
                                elif fieldname == 'browseUrl':
                                    
                                    # Set and store browse flag?
                                    if value:
                                        if value.lower() != 'null':
                                            scenedict[sceneID]['browse'] = 'Y'
                                        else:
                                            scenedict[sceneID]['browse'] = 'N'
                                
                                # Level 1?
                                elif fieldname == 'Data Type Level-1':
                                    j = value.rfind('_') + 1
                                    value = value[j:]
                                    
                                # Store the metatdata key-value pair in the scene dictionary    
                                scenedict[sceneID][fieldname] = value
                                
                    # Modification Date update required?
                    if sceneID in badgeom or sceneID in updatemissing:
                        scenedict[sceneID]['updatemodifiedDate'] = True
                    else: 
                        scenedict[sceneID]['updatemodifiedDate'] = False
                        
                    # Geometry data update required?
                    if sceneID in badgeom:
                        scenedict[sceneID]['updategeom'] = True
                    else: 
                        scenedict[sceneID]['updategeom'] = False
                        
                    # Extract and store  the scene's coordinate metadata
                    scenedict[sceneID]['coords'] = item['spatialCoverage']['coordinates'][0]

    return scenedict

# =============================================================================
# Retrieve the details of the features (scenes) that have alrready been added
#     to the IEO geopackage (catalog)
# =============================================================================

def readIEO(layer):

    # Initialise the error counts
    errors = {'total' : 0,
              'metadata' : 0,
              'date' : 0,
              'geometry' : 0}
    
    # Declare the output data objects
    badgeom, scenelist, updatemissing = ([] for i in range(3))
    
    lastmodifiedDate = None
    
    # Declare the other main local data objects
    reimport = []
    
    lastupdate = None
    
    # =============================================================================
    # Iterate through features and build the list of Scene IDs for which
    #     a valid entry (feature) already exists in the IEO catalog
    # =============================================================================
    
    # Initialise feature number
    fnum = 0
    
    featureCount = layer.GetFeatureCount()
    
    if featureCount == 0:
        print('The IEO Sentinel2 catalog is currently empty.')
    else:
        layer.StartTransaction()
        
        feature = layer.GetNextFeature()
                
        while feature:
            datetuple = None
            
            # Check if bad feature and, if so, delete it without requesting reimport
            try:
                ProductID = feature.GetField("ProductID")
            except:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                if verbose_g:
                    print(exc_type, fname, exc_tb.tb_lineno)
                    print('ERROR: bad feature, deleting.')
                layer.DeleteFeature(feature.GetFID())
                ieo.logerror('{}/{}'.format(ieo.catgpkg, ieo.Sen2shp), '{} {} {}'.format(exc_type, fname, exc_tb.tb_lineno), errorfile = errorfile)
                feature = layer.GetNextFeature()
                continue
            
            # Display progress
            fnum += 1
            
            print('\rInspecting feature {:5d} of {}, scene {}.\r'\
                  .format(fnum, featureCount, ProductID), end = '')
            
            # Add scene to list
            scenelist.append(ProductID)
                        
            # Check that feature has invalid Sensor ID (proxy for invalid metadata)
            #    - and, if so, delete it and flag for reimportation
            if not feature.GetField('PRODUCT_TYPE').startswith('S2MSI2A'):
                if verbose_g:
                    print(f'PRODUCT_TYPE = {feature.GetField("PRODUCT_TYPE")}')
                    print('ERROR: missing metadata for scene {}. Feature will be deleted from shapefile and reimported.'.format(ProductID))
                ieo.logerror(ProductID, 'Feature missing metadata, deleted, reimportation required.')
                reimport.append(datetime.datetime.strptime(ProductID[11:19], '%Y%m%d'))
                layer.DeleteFeature(feature.GetFID())
                errors['total'] += 1
                errors['metadata'] += 1
                
            else:    # Sensor ID is valid
                
                # Check that feature has a valid Modification Date field value
                #    - and, if not, flag the feature for updating of that field
                try:
                    mdate = feature.GetField('GENERATION_TIME')
                    if isinstance(mdate, str):
                        if '+' in mdate:
                            datetimestr = '%Y/%m/%d %H:%M:%S+00'
                        else:
                            datetimestr = '%Y/%m/%d %H:%M:%S'
                        datetuple = datetime.datetime.strptime(mdate, datetimestr)
                    if not lastupdate or datetuple > lastupdate:
                        lastupdate = datetuple
                        lastmodifiedDate = mdate
                except:
                    if verbose_g:
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                        print(exc_type, fname, exc_tb.tb_lineno)
                        # print('ERROR: GENERATION_TIME information missing for scene {}, adding to list.'.format(ProductID))
                        if mdate:
                            print(mdate)
                    ieo.logerror(ProductID, 'GENERATION_TIME missing.', errorfile = errorfile)
                    # updatemissing.append(ProductID)
                    errors['total'] += 1
                    errors['date'] += 1
                
                # Check that feature has a valid geometry setting value
                #    - and, if not, flag the feature for updating of that setting
                try:
                    geom = feature.GetGeometryRef()
                    env = geom.GetEnvelope()
                    if env[0] == env[1] or env[2] == env[3]:
                        if verbose_g:
                            print('Bad geometry identified for scene {}, adding to the list.'.format(ProductID))
                        ieo.logerror(ProductID, 'Bad/missing geometry.')
                        badgeom.append(ProductID)
                except:
                    if verbose_g:
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                        print(exc_type, fname, exc_tb.tb_lineno)
                        print('Bad geometry identified for scene {}, adding to the list.'.format(ProductID), errorfile = errorfile)
                    ieo.logerror(ProductID, 'Bad/missing geometry.')
                    badgeom.append(ProductID)
                    errors['total'] += 1
                    errors['geometry'] += 1
            
            # Display a summary of the error count to date each time the total number of errors is a multiple of 100
            if errors['total'] > 0 and (errors['total'] % 100 == 0):
                print('\r{} errors found in layer of types: metadata: {}, missing modification date: {}, missing/bad geometry: {}.\r'\
                      .format(errors['total'], errors['metadata'], errors['date'], errors['geometry']), end = '')
            
            # Get the next feature to continue this while loop
            feature = layer.GetNextFeature()
            
        # Complete the transaction that was started above
        layer.CommitTransaction()
        
        print('The IEO Sentinel 2 catalog currently contains {} valid features.'.format(len(scenelist)))
        
        # Display the final error counts
        if errors['total'] > 0 and (errors['total'] % 100 != 0):
            print('\n{} errors found in layer of types: metadata: {}, missing modification date: {}, missing/bad geometry: {}.'.format(errors['total'], errors['metadata'], errors['date'], errors['geometry']))
        
    # If the earliest modification date of the scenes flagged for reimport precedes 
    #    precedes the earliest modification date of all of the other scenes,
    #    use the former as the earliest modification date
    if len(reimport) > 0 and lastupdate:
        if min(reimport) < lastupdate:
            lastmodifiedDate = datetime.datetime.strftime('%Y-%m-%d', min(reimport))
            
    return scenelist, updatemissing, badgeom, lastmodifiedDate 

# =============================================================================
# Search the selected USGS/ERS collections for the basic details (metadata) of
#    scenes that pertain to the region of intereast and the specified date range
#       and then query USGS/ERS to retrieve the remainder of the metadata for
#          the selected scenes  
# =============================================================================

def readUSGS(baseURL, version, headers, \
                 MBR, pathrowstrs, \
                 startdate, enddate, \
                 maxResults, savequeries, \
                 scenelist, updatemissing, badgeom, lastmodifiedDate, \
                 fieldvaluelist):

    # Compile a list of the required USGS/ERS metadata elements
    queryfieldnames = []
    
    for element in fieldvaluelist:
        queryfieldnames.append(element[2])    
    
    # Declare the metadata dictionary to be populated
    scenedict = {}
    
    # Initialise the USGS/ERS URLs to be used
    RequestURL = '{}{}/scene-search'.format(baseURL, version)
    
    # Specify the in-scope USGS/ERS collection names and their start dates
    datasetNames = {'landsat_ot_c2_l2' : '2013-02-11', \
                    'landsat_etm_c2_l2' : '1999-04-15', \
                    'landsat_tm_c2_l2' : '1982-07-16'}

    # =============================================================================
    # Loop through the specified collections
    # =============================================================================
    
    for datasetName in datasetNames.keys():
        print('\r***** Querying collection: {}.....'.format(datasetName))
        
        # =============================================================================
        #  Set the query-from date for the current collection
        # =============================================================================
        
        # Override the start dates with the last modified date in the IEO catalog
        #    - unless there are catalog entries (features) which have been flagged 
        #         for update
        # if lastmodifiedDate and not (len(updatemissing) > 0 or len(badgeom) > 0):
        #     coll_startdate = lastmodifiedDate
        # else:
        #     coll_startdate = startdate
        # if startdate:
        #     coll_startdate = startdate
        # else:
        #     coll_startdate = datasetNames[datasetName]
        coll_startdate = datasetNames[datasetName]
            
        # Ensure that the date separator is - instead of /
        if '/' in coll_startdate:
            coll_startdate = coll_startdate.replace('/', '-')
            
        datetuple = datetime.datetime.strptime(coll_startdate, '%Y-%m-%d')
        sensorstarttuple = datetime.datetime.strptime(datasetNames[datasetName], '%Y-%m-%d') # restrict searches to times from which sensor was in orbit
        
        # Override the start date with the collection's start date 
        #    - if the latter is later
        if datetuple < sensorstarttuple:
            datetuple = sensorstarttuple
  
        # =============================================================================
        # Set the query-to date for the current collection             
        # =============================================================================
        
        enddatetuple = datetime.datetime.strptime(enddate, '%Y-%m-%d')
        
        # For the Landsat 5 collection, override its query-to date with its
        #    mission end date if the latter is earlier than the former
        if datasetName == 'landsat_tm_c2_l2':
            l5enddatetuple = datetime.datetime.strptime('2013-06-05', '%Y-%m-%d') # end of Landsat 5 mission
            
            if l5enddatetuple < enddatetuple:
                enddatetuple = l5enddatetuple
        
        # Null date range?
        # if datetuple >= enddatetuple:
        #     print('0 new scenes have been found or require updating.')
        
        # =============================================================================
        # Loop through the specified collections in the specified date range
        #     in 365-day chunks, searching for eligible scenes and retrieving
        #     some basic metatdata for them
        # =============================================================================
         
        while datetuple < enddatetuple:
            edatetuple = datetuple + datetime.timedelta(days = 365) # iterate by year
            
            # Use the collection's end date if the chunk's end date exceeds it 
            if edatetuple > enddatetuple:
                edatetuple = enddatetuple
            
            coll_startdate = datetuple.strftime('%Y-%m-%d')
            coll_enddate = edatetuple.strftime('%Y-%m-%d')
            
            # Construct the USGS/ERS search query, and limit its scope based on the
            #    region of interest's MBR and the current chunk's date range
            print('\rNow searching for scene data from collection {} from {} through {}.'.format(datasetName, coll_startdate, coll_enddate))
            searchparams = json.dumps({"datasetName": datasetName,
                            "sceneFilter" : {                                          
                                "spatialFilter":{"filterType": "mbr",
                                                 "lowerLeft":{"latitude": MBR[0],
                                                              "longitude": MBR[1]},
                                                 "upperRight":{"latitude": MBR[2],
                                                               "longitude": MBR[3]}},
                                "temporalFilter":{"start": coll_startdate,
                                                  "end": coll_enddate},
                                "cloudCoverFilter" : {
                                    "includeUnknown":False,
                                    "max": 100,
                                    "min": 0}},
                            "metadataType": "full",
                            "maxResults": maxResults,
                            "sortDirection": "ASC"})

            # Construct the USGS/ERS query      
            response = requests.post(RequestURL, searchparams, headers = headers)
            
            # Handle any HTTP error encountered
            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                ieo.logerror(RequestURL, ''.join(map(str, e.args)).split(',')[0])
                sys.exit(e)
            
            # Save query results?
            if savequeries:
                now = datetime.datetime.now()
                querydir = os.path.join(ieo.logdir, 'json_query_data')
                if not os.path.isdir(querydir):
                    os.mkdir(querydir)
                outfile = os.path.join(querydir, 'query_{}_{}_{}_{}.txt'.format(datasetName, coll_startdate, coll_enddate, now.strftime('%Y%m%d-%H%M%S')))
                with open(outfile, 'w') as output:
                    output.write(response.text)
            
            # =============================================================================
            # Extract the scene metadata from the results of the current query (chunked)            
            # =============================================================================
            
            json_data = json.loads(response.text)
            
            querylist = []
            
            for i in range(len(json_data['data']['results'])):
                sceneID = json_data['data']['results'][i]['entityId']
                
                # Scene to be added or updated?: Create or update its scene dictionary entry
                if sceneID[3:9] in pathrowstrs and \
                    (not sceneID in scenelist or \
                     sceneID in updatemissing or sceneID in badgeom):
                    
                    # List for scene-level USGS/ERS query
                    querylist.append(sceneID)
                    tdict =  {}
                    for item in json_data['data']['results'][i]["metadata"]:
                        if 'fieldName' in item.keys() and 'value' in item.keys():
                            fieldname = item['fieldName'].rstrip().lstrip().replace('L-1', 'L1')
                            value = item['value']
                            if fieldname == "Date Acquired":
                                acquisitionDate = datetime.datetime.strptime(value, '%Y/%m/%d')
                                value = acquisitionDate
                            elif fieldname == "Date Product Generated L2":
                                modifiedDate = datetime.datetime.strptime(value,'%Y/%m/%d')
                                value = modifiedDate
                            elif "Date" in fieldname:
                                value = datetime.datetime.strptime(value, '%Y/%m/%d')
                            tdict[fieldname] = value
                            
                    # json_data['data']['results'][i]["metadata"][3]["value"]
                    
                    # if json_data['data']['results'][i]['metadata']['Date Product Generated L2'] == 'Unknown': 
                    #     modifiedDate = acquisitionDate
                    # else:
                    #     if '.' in json_data['data']['results'][i]["publishDate"]: # Sub-second data provided?
                    #         modifiedDate = datetime.datetime.strptime(json_data['data']['results'][i]['metadata']['Date Product Generated L2'], '%Y-%m-%d %H:%M:%S.%f')
                    #     else:
                    #         modifiedDate = datetime.datetime.strptime(json_data['data']['results'][i]['metadata']['Date Product Generated L2'], '%Y-%m-%d %H:%M:%S')
                    
                    scenedict[sceneID] = {'Landsat Product Identifier': json_data['data']['results'][i]["displayId"],
                             # "browseUrl": json_data['data']['results'][i]["browse"][0]["thumbnailPath"],
                             # "dataAccessUrl": json_data['data']['results'][i]["dataAccessUrl"],
                             # "downloadUrl": json_data['data']['results'][i]["downloadUrl"],
                             # "metadataUrl": json_data['data']['results'][i]["metadataUrl"],
                             # "fgdcMetadataUrl": json_data['data']['results'][i]["fgdcMetadataUrl"],
                             'Acquisition_Date': acquisitionDate,
                             'publishDate': modifiedDate,
                             # "orderUrl": json_data['data']['results'][i]["orderUrl"],
                             'Dataset_Identifier': datasetName,
                             'updatemodifiedDate': False,
                             'updategeom': False}
                    
                    # Extract and store the scene's coordinate metadata
                    scenedict[sceneID]['coords'] = json_data['data']['results'][i]['spatialCoverage']['coordinates'][0]
                    
                    # Update scenedict with values stored in tdict 
                    for key in tdict.keys():
                        if not key in scenedict[sceneID].keys():
                            scenedict[sceneID][key] = tdict[key]
                     

            # =============================================================================
            # Loop through all of the scenes that have been selected by the scene search
            #    process above, in batches containing up to 100 scenes each, and retrieve 
            #       (query) the additional metadata needed by IEO   
            # =============================================================================
            
            # print('{} new scenes have been found or require updating, querying metadata.'.format(len(querylist)))
            
            # if len(querylist) > 0:
            #     scenedict = queryUSGS(baseURL, version, headers, \
            #           scenedict, badgeom, updatemissing, fieldvaluelist, \
            #           querylist, queryfieldnames, savequeries,\
            #           datasetName)
            
            # Increment the date controlling the current while loop
            datetuple = edatetuple + datetime.timedelta(days = 1)

    return scenedict, queryfieldnames

# =============================================================================
# Use the contents of the scene dictionary to add new entries to, and/or to 
#    update entries in the IEO catalog (geopackage layer)
# =============================================================================

def updateIEO(layer, fieldvaluelist, fnames, \
              ProductIDs, scenedict, *args, **kwargs):
    bucketname = kwargs.get('bucket', None)
    if bucketname:
        import zipfile
    # This section borrowed from https://pcjericks.github.io/py-gdalogr-cookbook/projection.html
    # Lat/ Lon WGS-84 to local projection transformation
    # source = osr.SpatialReference() # Lat/Lon WGS-64
    # source.ImportFromEPSG(4326)
    
    # target = ieo.prj
    
    # transform = osr.CoordinateTransformation(source, target)
    proddir = os.path.join(ieo.Sen2ingestdir, 'metadata')
    if not os.path.isdir(proddir):
        os.makedirs(proddir)
    
    # Initialise the iteration counter for the following loop
    
    for bucket in sorted(scenedict.keys()):
   
        print(f'Now processing scene metadata in bucket {bucket}.')
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
                            ProductID = os.path.basename(f)[:60]
                            if not ProductID in ProductIDs:
                                # satellite = ProductID[:3]
                                lmtdfile = os.path.join(proddir, 'MTD_MSIL2A.xml')
                                if os.path.isfile(lmtdfile):
                                    if verbose_g: print(f'Deleting: {lmtdfile}')
                                    os.remove(lmtdfile)
                                if bucketname:
                                    zfile = os.path.join(ieo.Sen2ingestdir, f)
                                    if not os.path.isfile(zfile):
                                        print(f'\nDownloading {ProductID} metadata from bucket {bucket} ({filenum}/ {numfiles}).\n')
                                        S3ObjectStorage.downloadfile(ieo.Sen2ingestdir, bucket, f)
                                    if os.path.isfile(zfile):
                                        print(f'Extracting metadatafile from {f} to: {lmtdfile}')
                                        with zipfile.ZipFile(zfile, 'r') as z:
                                            z.extract('MTD_MSIL2A.xml', proddir)
                                    
                                else:
                                    mtdfile = os.path.join(f, 'MTD_MSIL2A.xml')
                                    # lmtdfile = os.path.join(proddir, 'MTD_MSIL2A.xml')
                                    if os.path.isfile(lmtdfile):
                                        if verbose_g: print(f'Deleting: {lmtdfile}')
                                        os.remove(lmtdfile)
                                    # if verbose_g:
                                    #     print(f)                            
                                #        try:
                                    # proddir = os.path.join(ieo.Sen2ingestdir, ProductID)
                                    print(f'\nDownloading {ProductID} metadata from bucket {bucket} ({filenum}/ {numfiles}).\n')
                                    S3ObjectStorage.downloadfile(proddir, bucket, mtdfile)
                                if os.path.isfile(lmtdfile):
                                    layer = Sen2updateIEO(lmtdfile, layer, bucket, f)
                                else:
                                    print(f'ERROR: Missing file for {ProductID}: {lmtdfile}')
                                    ieo.logerror(ProductID, f'Missing file: {lmtdfile}')
                                ProductIDs.append(ProductID)
                            filenum += 1
    

    return layer

# =============================================================================
# Perform basic validation on the values of selected arguments
#
# Note: The USGS/ERS service-related arguments (username, passwordv catalogID & 
#            version) will be validated later by the API call that attempts to
#               establish the connections to that service.
#
#       The MBR argument is also validated elsewhere in this script
#
# =============================================================================

def valArgs(startdate, enddate, rescan, verbose):
            # baseURL, maxResults, \
            # thumbnails, savequeries
       
    # Validate start date
    try:
        datetime.datetime.strptime(startdate, '%Y-%m-%d')
    except ValueError:
        errMsg = 'Invalid argument value: Start date must be a valid date in YYYY-MM-DD format.'
        print(errMsg)
        ieo.logerror(startdate, errMsg)
        sys.exit()
    
    # Validate end date
    if enddate:
        try:
            datetime.datetime.strptime(enddate, '%Y-%m-%d')
        except ValueError:
            errMsg = 'Invalid argument value: End date must be a valid date in YYYY-MM-DD format.'
            print(errMsg)
            ieo.logerror(enddate, errMsg)
            sys.exit()
    
        if enddate <= startdate:
            errMsg = 'Invalid argument value: End date must later than start date.'
            print(errMsg)
            ieo.logerror(enddate, errMsg)
            sys.exit()
            
    # Validate Base URL
    # if urlparse(baseURL).scheme not in ('http', 'https'):
    #     errMsg = 'Invalid argument value: Base URL is not valid.'
    #     print(errMsg)
    #     ieo.logerror(baseURL, errMsg)
    #     sys.exit()    

    # # Validate Naximum Results
    # if not isinstance(maxResults, int):
    #     errMsg = 'Invalid argument value: Maximum Results must be an integer value.'
    #     print(errMsg)
    #     ieo.logerror(maxResults, errMsg)
    #     sys.exit()   
    # elif maxResults < 1:
    #     errMsg = 'Invalid argument value: Maximum Results must be greater than zero.'
    #     print(errMsg)
    #     ieo.logerror(baseURL, errMsg)
    #     sys.exit()
        
    # # Validate Thumbnails flag
    # if not isinstance(thumbnails, bool):
    #     errMsg = 'Invalid argument value: Thumbnails setting must be True or False.'
    #     print(errMsg)
    #     ieo.logerror(thumbnails, errMsg)
    #     sys.exit()          

    # # Validate Save Queries flag
    # if not isinstance(savequeries, bool):
    #     errMsg = 'Invalid argument value: Save Queries setting must be True or False.'
    #     print(errMsg)
    #     ieo.logerror(savequeries, errMsg)
    #     sys.exit()   
    
    # Validate rescan flag
    if not isinstance(rescan, bool):
        errMsg = 'Invalid argument value: Rescan setting must be True or False.'
        print(errMsg)
        ieo.logerror(rescan, errMsg)
        sys.exit()         

    # Validate Verbose flag
    if not isinstance(verbose, bool):
        errMsg = 'Invalid argument value: Verbose setting must be True or False.'
        print(errMsg)
        ieo.logerror(verbose, errMsg)
        sys.exit()  

    return

# =============================================================================
# Mainline Processing
# =============================================================================

def main( #username=None, password=None, catalogID='EE', version='stable', \
         startdate = '2015-06-23', enddate = None, rescan = False,\
         # MBR=None, baseURL='https://m2m.cr.usgs.gov/api/api/json/', \
         #     maxResults=50000, thumbnails=False, savequeries=False, \
             verbose = False, bucket = None):

    # =============================================================================
    # Declare and initialise the needed global variables
    # =============================================================================
    scenedict = {}
    global errorfile, errorsfound, pathrows, verbose_g
    
    errorfile = os.path.join(ieo.logdir, 'Sentinel2_inventory_download_errors.csv')
    
    verbose_g = verbose

    # =============================================================================
    # Validate selected argument values
    # =============================================================================
    
    print('\n***** Validating the argument values.....')
    
    valArgs(startdate, enddate, rescan, verbose)
            # baseURL, maxResults, \
            # thumbnails, savequeries, 

    # =============================================================================
    # Handle any missing arguments
    # =============================================================================
    
    print('\n***** Checking some key configuration settings.....')
    
    # # USGS username or password arguments not provided?: Prompt for them now
    # if not (username and password):
    #     if not username:
    #         username = input('USGS/ERS username: ')
    #     if not password:
    #         password = getpass.getpass('USGS/ERS password: ')

    # End date argument not provided?: Use the current date
    if not enddate:
       enddate = datetime.datetime.today().strftime('%Y-%m-%d')
    enddatetuple = datetime.datetime.strptime(enddate, '%Y-%m-%d')
       
    print('\nChecks completed.')
 
    print('\n***** Getting the list of Sentinel 2 MGRS tiles for the region of interest.....\n')
 
    # Get WRS-2 Path-Row numbers - using either the ieo.WRS2 geopackage or the updateshp.ini file's path-row range specification
    # paths, rows, pathrowstrs = getPathsRows(ieo.config['Landsat']['useWRS2'], \
    #                                         ieo.config['Landsat']['pathrowvals'])
    MGRStilelist = ieo.Sen2tilelist
    
    # Parse the Minimum Bounding Rectangle argument if provided;
    #    otherwise, construct it
    # if MBR:
    #     MBR = MBR.split(',')
    #     if len(MBR) != 4:
    #         ieo.logerror('--MBR', 'Total number of coordinates does not equal four.', errorfile = errorfile)
    #         print('Error: Improper number of coordinates for --MBR set (must be four). Either remove this option (will use default values) or fix. Exiting.')
    #         sys.exit()
    # else:
    #     MBR = getMBR(baseURL, version, paths, rows)
    
    # print('\nMBR set: {}.'.format(MBR))

    # =============================================================================
    # Mainline processing proper begins here
    # =============================================================================
    
    print('\n***** Opening the IEO Sentinel 2 catalog ({}).....\n'.format(ieo.catgpkg))
    
    # Open the IEO geopackage - or create it if it does not exist
    data_source, layer, fieldvaluelist, fnames = openIEO()
    
    print('\n***** Inspecting the current contents of the IEO Sentinel 2 catalog.....\n')
    
    # Retrieve the details of the features (scenes) that have already been added
    #    to the IEO geopackage (catalog)
    ProductIDs, updatemissing, badgeom, lastmodifiedDate = \
        readIEO(layer)
    
    
    if lastmodifiedDate and not rescan:
        startdate = lastmodifiedDate #.strftime('%Y-%m-%d')
        if ieo.usePostGIS:
            datetimestr = '%Y/%m/%d %H:%M:%S+00'
        else:
            datetimestr = '%Y/%m/%d %H:%M:%S'
        startdatetuple = datetime.datetime.strptime(lastmodifiedDate, datetimestr)
    else:
        startdatetuple = datetime.datetime.strptime(startdate, '%Y-%m-%d')
    
    
    
    # print('\n***** Opening a connection to the USGS/ERS {} service.....\n'.format(catalogID))
    print(f'\nNow querying S3 buckets for Sentinel 2 scenes acquired between {startdate} and {enddate}.')
    if not bucket:
        scenedict = S3ObjectStorage.getSentinel2scenedict(MGRStilelist, startdate = startdatetuple, enddate = enddatetuple, verbose = verbose)
    else:
        scenedict = S3ObjectStorage.getSentinel2scenedictFromFlatBucket(MGRStilelist, bucket, startdate = startdatetuple, enddate = enddatetuple, verbose = verbose)
    # # Connect to the specified catalog (EarthExplorer) on the specified USGS server
    # apiKey = openUSGS(baseURL, version, catalogID, username, password)
    
    # # Place the API key in the HTTP headers
    # headers = {'X-Auth-Token': apiKey}  
    
    print('\n***** Retrieving the details of the in-scope USGS/ERS Landsat scenes.....\n')
    
    # scenedict, queryfieldnames = \
    #     readUSGS(baseURL, version, headers, \
    #                 MBR, pathrowstrs, \
    #                 startdate, enddate, \
    #                 maxResults, savequeries, \
    #                 scenelist, updatemissing, badgeom, lastmodifiedDate, \
    #                 fieldvaluelist)
    buckets = sorted(scenedict.keys())
    
    print(f'A total of {len(ProductIDs)} scenes currently exist in the geodatabase.')
    
    if len(buckets) == 0:
        print('\nNo scenes to be added or updated in IEO geopackage layer.\n')
    else:
        print('\n***** Updating the IEO catalog (geopackage).....\n')
        
        print(f'{len(buckets)} buckets identified with possible scenes to be added or updated to IEO geopackage layer.\n')

        layer = updateIEO(layer, fieldvaluelist, fnames, \
                  ProductIDs, scenedict, bucket = bucket)
    
    # If enabled, download the thumbnails images for any new or modified scenes 
    # if thumbnails:
    #     print('\r***** Downloading thumbnails.....' + (' ' * 50) + '\n')
        
    #     getThumbnails(layer, updatemissing, badgeom)
    
    # Close the USGS/ERS connection
    # print('\r***** Closing the connection to the USGS/ERS {} service.....'.format(catalogID))
    # closeUSGS(baseURL, version, headers)

    # Close the IEO geopackage
    print('\n***** Closing the IEO catalog (geopackage).....')
    data_source = None
    layer = None

    # All done
    print('\n***** Processing completed.')
    
    # Script completed normally - exit
    sys.exit()

# =============================================================================
# Command Line Processing
# =============================================================================
    
# Called from the command line?
if __name__ == '__main__':
    
    # Parse the expected command line arguments
    parser = argparse.ArgumentParser('This script imports Sentinel-2 scene metadata data extent polygons intothe local library.')
    # parser.add_argument('-u','--username', type = str, default = None, help = 'USGS/EROS Registration System (ERS) username.')
    # parser.add_argument('-p', '--password', type = str, default = None, help = 'USGS/EROS Registration System (ERS) password.')
    # parser.add_argument('-c', '--catalogID', type = str, default = 'EE', help = 'USGS/EROS Catalog ID (default = "EE").')
    # parser.add_argument('-v', '--version', type = str, default = "stable", help = 'JSON version, default = stable.')
    parser.add_argument('--startdate', type = str, default = "2015-06-23", help = 'Start date for query in YYYY-MM-DD format. (Default = 2015-06-23, e.g., Sentinel 2A launch date).')
    parser.add_argument('--enddate', type = str, default = None, help = "End date for query in YYYY-MM-DD format. (Default = today's date).")
    # parser.add_argument('-m', '--MBR', type = str, default = None, help = 'Minimum Bounding Rectangle (MBR) coordinates in decimal degrees in the following format (comma delimited, no spaces): lower left latitude, lower left longitude, upper right latitude, upper right longitude. If not supplied, these will be determined from WRS-2 Paths and Rows in updateshp.ini.')
    # parser.add_argument('-b', '--baseURL', type = str, default = 'https://m2m.cr.usgs.gov/api/api/json/', help = 'Base URL to use excluding JSON version (Default = "https://m2m.cr.usgs.gov/api/api/json/").')
    # parser.add_argument('--maxResults', type = int, default = 50000, help = 'Maximum number of results to return (1 - 50000, default = 50000).')
    # parser.add_argument('--thumbnails',  action = 'store_true', help = 'Download thumbnails (default = False).')
    parser.add_argument('--bucket', type = str, default = None, help = 'Specify bucket to scan for scenes. Default = None.')
    parser.add_argument('--rescan', action = 'store_true', help = 'Rescan buckets for Sentinel-2 scenes, ignoring any previously scanned results.')
    parser.add_argument('--verbose', action = 'store_true', help = 'Display more messages during migration.')
    
    args = parser.parse_args()
 
    # Pass the parsed arguments to mainline processing   
    main(args.startdate, args.enddate, args.rescan, args.verbose, args.bucket) #args.username, args.password, args.catalogID, args.version, 
         #args.MBR, args.baseURL, args.maxResults, args.thumbnails, args.savequeries, \
             