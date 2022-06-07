    # =============================================================================
# !/usr/bin/env python3
# 
# Guy Serbin, EOanalytics Ltd.
# Talent Garden Dublin, Claremont Ave. Glasnevin, Dublin 11, Ireland
# email: guyserbin <at> eoanalytics <dot> ie
# 
# version 1.5
# 
# This script will create and update a geopackage layer of all available Landsat TM/ETM+/OLI-TIRS scenes, including available metadata
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
# =============================================================================

# =============================================================================
# Import the needed modules
# =============================================================================

import argparse, datetime, getpass, json, math, os, requests, shutil, sys

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
        import ieo
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

    print('\rCreated thumbnail and/or world files for {} of {} scenes.                                       \n'\
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
    fieldvaluelist = [
        ['LandsatPID', 'LANDSAT_PRODUCT_ID_L2', 'Landsat Product Identifier L2', ogr.OFTString, 40],
        ['LandsatPIDL1', 'LANDSAT_PRODUCT_ID_L1', 'Landsat Product Identifier L1', ogr.OFTString, 40],
        ['sceneID', 'sceneID', 'Landsat Scene Identifier', ogr.OFTString, 21],
        ['SensorID', 'SensorID', 'Sensor Identifier', ogr.OFTString, 0],
        ['SatNumber', 'Satellite', 'Satellite', ogr.OFTString, 0],
        ['acqDate', 'acquisitionDate', 'Date Acquired', ogr.OFTDateTime, 0],
        ['Updated', 'dateUpdated', 'Date Product Generated L2', ogr.OFTDateTime, 0],
        ['path', 'path', 'WRS Path', ogr.OFTInteger, 0],
        ['row', 'row', 'WRS Row', ogr.OFTInteger, 0],
        ['CenterLat', 'sceneCenterLatitude', 'Scene Center Latitude', ogr.OFTReal, 0],
        ['CenterLong', 'sceneCenterLongitude', 'Scene Center Longitude', ogr.OFTReal, 0],
        ['CC', 'cloudCover', 'Cloud Cover Truncated', ogr.OFTInteger, 0],
        ['CCFull', 'cloudCoverFull', 'Scene Cloud Cover L1', ogr.OFTReal, 0],
        ['CCLand', 'CLOUD_COVER_LAND', 'Land Cloud Cover', ogr.OFTReal, 0],
        ['UL_Q_CCA', 'FULL_UL_QUAD_CCA', 'Cloud Cover Quadrant Upper Left', ogr.OFTReal, 0],
        ['UR_Q_CCA', 'FULL_UR_QUAD_CCA', 'Cloud Cover Quadrant Upper Right', ogr.OFTReal, 0],
        ['LL_Q_CCA', 'FULL_LL_QUAD_CCA', 'Cloud Cover Quadrant Lower Left', ogr.OFTReal, 0],
        ['LR_Q_CCA', 'FULL_LR_QUAD_CCA', 'Cloud Cover Quadrant Lower Right', ogr.OFTReal, 0],
        ['DT_L2', 'DATA_TYPE_L2', 'Data Type L2', ogr.OFTString, 0],
        ['DT_L1', 'DATA_TYPE_L1', 'Data Type Level-1', ogr.OFTString, 0],
        ['DT_L0RP', 'DATA_TYPE_L0RP', 'Data Type Level 0Rp', ogr.OFTString, 0],
        ['L1_AVAIL', 'L1_AVAILABLE', 'L1 Available', ogr.OFTString, 0],
        ['IMAGE_QUAL', 'IMAGE_QUALITY', 'Image Quality', ogr.OFTString, 0],
        ['dayOrNight', 'dayOrNight', 'Day/Night Indicator', ogr.OFTString, 0],
        ['sunEl', 'sunElevation_L1', 'Sun Elevation L1', ogr.OFTReal, 0],
        ['sunAz', 'sunAzimuth_L1', 'Sun Azimuth L1', ogr.OFTReal, 0],
        ['sunElL0RA', 'sunElevation_L0RA', 'Sun Elevation L1', ogr.OFTReal, 0],
        ['sunAzL0RA', 'sunAzimuth_L0RA', 'Sun Azimuth L1', ogr.OFTReal, 0],
        ['StartTime', 'sceneStartTime', 'Start Time', ogr.OFTDateTime, 0],
        ['StopTime', 'sceneStopTime', 'Stop Time', ogr.OFTDateTime, 0],
        ['UTM_ZONE', 'UTM_ZONE', 'UTM Zone', ogr.OFTInteger, 0],
        ['DATUM', 'DATUM', 'Datum', ogr.OFTString, 0],
        ['ELEVSOURCE', 'ELEVATION_SOURCE', 'Elevation Source', ogr.OFTString, 0],
        ['ELLIPSOID', 'ELLIPSOID', 'Ellipsoid', ogr.OFTString, 0],
        ['PROJ_L1', 'MAP_PROJECTION_L1', 'Product Map Projection L1', ogr.OFTString, 0],
        ['PROJ_L2', 'MAP_PROJECTION_L2', 'Product Map Projection L2', ogr.OFTString, 0],
        ['PROJ_L0RA', 'MAP_PROJECTION_L0RA', 'Map Projection L0Ra', ogr.OFTString, 0],
        ['ORIENT', 'ORIENTATION', 'Orientation', ogr.OFTString, 0],
        ['EPHEM_TYPE', 'EPHEMERIS_TYPE', 'Ephemeris Type', ogr.OFTString, 0],
        ['CPS_MODEL', 'GROUND_CONTROL_POINTS_MODEL', 'Ground Control Points Model', ogr.OFTInteger, 0],
        ['GCPSVERIFY', 'GROUND_CONTROL_POINTS_VERIFY', 'Ground Control Points Version', ogr.OFTInteger, 0],
        ['RMSE_MODEL', 'GEOMETRIC_RMSE_MODEL', 'Geometric RMSE Model', ogr.OFTReal, 0],
        ['RMSE_X', 'GEOMETRIC_RMSE_MODEL_X', 'Geometric RMSE Model X', ogr.OFTReal, 0],
        ['RMSE_Y', 'GEOMETRIC_RMSE_MODEL_Y', 'Geometric RMSE Model Y', ogr.OFTReal, 0],
        ['RMSEVERIFY', 'GEOMETRIC_RMSE_VERIFY', 'Geometric RMSE Verify', ogr.OFTReal, 0],
        ['FORMAT', 'OUTPUT_FORMAT', 'Output Format', ogr.OFTString, 0],
        ['RESAMP_OPT', 'RESAMPLING_OPTION', 'Resampling Option', ogr.OFTString, 0],
        ['LINES', 'REFLECTIVE_LINES', 'Reflective Lines', ogr.OFTInteger, 0],
        ['SAMPLES', 'REFLECTIVE_SAMPLES', 'Reflective Samples', ogr.OFTInteger, 0],
        ['TH_LINES', 'THERMAL_LINES', 'Thermal Lines', ogr.OFTInteger, 0],
        ['TH_SAMPLES', 'THERMAL_SAMPLES', 'Thermal Samples', ogr.OFTInteger, 0],
        ['PAN_LINES', 'PANCHROMATIC_LINES', 'Panchromatic Lines', ogr.OFTInteger, 0],
        ['PANSAMPLES', 'PANCHROMATIC_SAMPLES', 'Panchromatic Samples', ogr.OFTInteger, 0],
        ['GC_SIZE_R', 'GRID_CELL_SIZE_REFLECTIVE', 'Grid Cell Size Reflective', ogr.OFTInteger, 0],
        ['GC_SIZE_TH', 'GRID_CELL_SIZE_THERMAL', 'Grid Cell Size Thermal', ogr.OFTInteger, 0],
        ['GCSIZE_PAN', 'GRID_CELL_SIZE_PANCHROMATIC', 'Grid Cell Size Panchromatic', ogr.OFTInteger, 0],
        ['PROCSOFTVE', 'PROCESSING_SOFTWARE_VERSION', 'Processing Software Version', ogr.OFTString, 0],
        ['CPF_NAME', 'CPF_NAME', 'Calibration Parameter File', ogr.OFTString, 0],
        ['DATEL1_GEN', 'DATE_L1_GENERATED', 'Date L-1 Generated', ogr.OFTString, 0],
        ['GCP_Ver', 'GROUND_CONTROL_POINTS_VERSION', 'Ground Control Points Version', ogr.OFTInteger, 0],
        ['DatasetID', 'DatasetID', 'Dataset Identifier', ogr.OFTString, 0],
        ['CollectCat', 'COLLECTION_CATEGORY', 'Collection Category', ogr.OFTString, 0],
        ['CollectNum', 'COLLECTION_NUMBER', 'Collection Number', ogr.OFTString, 0],
        ['flightPath', 'flightPath', 'flightPath', ogr.OFTString, 0],
        ['RecStation', 'receivingStation', 'Station Identifier', ogr.OFTString, 0],
        ['imageQual1', 'imageQuality1', 'Image Quality 1', ogr.OFTString, 0],
        ['imageQual2', 'imageQuality2', 'Image Quality 2', ogr.OFTString, 0],
        ['gainBand1', 'gainBand1', 'Gain Band 1', ogr.OFTString, 0],
        ['gainBand2', 'gainBand2', 'Gain Band 2', ogr.OFTString, 0],
        ['gainBand3', 'gainBand3', 'Gain Band 3', ogr.OFTString, 0],
        ['gainBand4', 'gainBand4', 'Gain Band 4', ogr.OFTString, 0],
        ['gainBand5', 'gainBand5', 'Gain Band 5', ogr.OFTString, 0],
        ['gainBand6H', 'gainBand6H', 'Gain Band 6H', ogr.OFTString, 0],
        ['gainBand6L', 'gainBand6L', 'Gain Band 6L', ogr.OFTString, 0],
        ['gainBand7', 'gainBand7', 'Gain Band 7', ogr.OFTString, 0],
        ['gainBand8', 'gainBand8', 'Gain Band 8', ogr.OFTString, 0],
        ['GainChange', 'GainChange', 'Gain Change', ogr.OFTString, 0],
        ['GCBand1', 'gainChangeBand1', 'Gain Change Band 1', ogr.OFTString, 0],
        ['GCBand2', 'gainChangeBand2', 'Gain Change Band 2', ogr.OFTString, 0],
        ['GCBand3', 'gainChangeBand3', 'Gain Change Band 3', ogr.OFTString, 0],
        ['GCBand4', 'gainChangeBand4', 'Gain Change Band 4', ogr.OFTString, 0],
        ['GCBand5', 'gainChangeBand5', 'Gain Change Band 5', ogr.OFTString, 0],
        ['GCBand6H', 'gainChangeBand6H', 'Gain Change Band 6H', ogr.OFTString, 0],
        ['GCBand6L', 'gainChangeBand6L', 'Gain Change Band 6L', ogr.OFTString, 0],
        ['GCBand7', 'gainChangeBand7', 'Gain Change Band 7', ogr.OFTString, 0],
        ['GCBand8', 'gainChangeBand8', 'Gain Change Band 8', ogr.OFTString, 0],
        ['SCAN_GAP_I', 'SCAN_GAP_INTERPOLATION', 'Scan Gap Interpolation', ogr.OFTReal, 0],
        ['ROLL_ANGLE', 'ROLL_ANGLE', 'Roll Angle', ogr.OFTReal, 0],
        ['FULL_PART', 'FULL_PARTIAL_SCENE', 'Full Partial Scene', ogr.OFTString, 0],
        ['NADIR_OFFN', 'NADIR_OFFNADIR', 'Nadir/Off Nadir', ogr.OFTString, 0],
        ['RLUT_FNAME', 'RLUT_FILE_NAME', 'RLUT File Name', ogr.OFTString, 0],
        ['BPF_N_OLI', 'BPF_NAME_OLI', 'Bias Parameter File Name OLI', ogr.OFTString, 0],
        ['BPF_N_TIRS', 'BPF_NAME_TIRS', 'Bias Parameter File Name TIRS', ogr.OFTString, 0],
        ['TIRS_SSM', 'TIRS_SSM_MODEL', 'TIRS SSM Model', ogr.OFTString, 0],
        ['TargetPath',  'Target_WRS_Path', 'Target WRS Path', ogr.OFTInteger, 0],
        ['TargetRow', 'Target_WRS_Row', 'Target WRS Row', ogr.OFTInteger, 0],
        ['DataAnom', 'data_anomaly', 'Data Anomaly', ogr.OFTString, 0],
        ['GapPSource', 'gap_phase_source', 'Gap Phase Source', ogr.OFTString, 0],
        ['GapPStat', 'gap_phase_statistic', 'Gap Phase Statistic', ogr.OFTReal, 0],
        ['L7SLConoff', 'scan_line_corrector', 'Scan Line Corrector', ogr.OFTString, 0],
        ['SensorAnom', 'sensor_anomalies', 'Sensor Anomalies', ogr.OFTString, 0],
        ['SensorMode', 'sensor_mode', 'Sensor Mode', ogr.OFTString, 0],
        ['browse', 'browseAvailable', 'Browse Available', ogr.OFTString, 0],
        ['browseURL', 'browseURL', 'browseUrl', ogr.OFTString, 0],
        ['MetadatUrl', 'metadataUrl', 'metadataUrl', ogr.OFTString, 0],
        ['FGDCMetdat', 'fgdcMetadataUrl', 'fgdcMetadataUrl', ogr.OFTString, 0],
        ['dataAccess', 'dataAccess', 'dataAccessUrl', ogr.OFTString, 0],
        ['orderUrl', 'orderUrl', 'orderUrl', ogr.OFTString, 0],
        ['DownldUrl', 'downloadUrl', 'downloadUrl', ogr.OFTString, 0],
        ['MaskType', 'MaskType', 'MaskType', ogr.OFTString, 0],
        ['Thumbnail_filename', 'Thumbnail_filename', 'Thumbnail_filename', ogr.OFTString, 0],
        ['Surface_reflectance_tiles', 'Surface_reflectance_tiles', 'Surface_reflectance_tiles', ogr.OFTString, 0],
        ['Brightness_temperature_tiles', 'Brightness_temperature_tiles', 'Brightness_temperature_tiles', ogr.OFTString, 0],
        ['Surface_temperature_tiles', 'Surface_temperature_tiles', 'Surface_temperature_tiles', ogr.OFTString, 0],
        ['Fmask_tiles', 'Fmask_tiles', 'Fmask_tiles', ogr.OFTString, 0],
        ['Pixel_QA_tiles', 'Pixel_QA_tiles', 'Pixel_QA_tiles', ogr.OFTString, 0],
        ['Radsat_QA_tiles', 'Radsat_QA_tiles', 'Radsat_QA_tiles', ogr.OFTString, 0],
        ['Aerosol_QA_tiles', 'Aerosol_QA_tiles', 'Aerosol_QA_tiles', ogr.OFTString, 0],
        ['NDVI_tiles', 'NDVI_tiles', 'NDVI_tiles', ogr.OFTString, 0],
        ['EVI_tiles', 'EVI_tiles', 'EVI_tiles', ogr.OFTString, 0],
        ['Tile_filename_base', 'Tile_filename_base', 'Tile_filename_base', ogr.OFTString, 0],
        ['S3_tile_bucket', 'S3_tile_bucket', 'S3_tile_bucket', ogr.OFTString, 0],
        ['S3_ingest_bucket', 'S3_ingest_bucket', 'S3_ingest_bucket', ogr.OFTString, 0],
        ['S3_endpoint_URL', 'S3_endpoint_URL', 'S3_endpoint_URL', ogr.OFTString, 0],
        ['S3_endpoint_path', 'S3_endpoint_path', 'S3_endpoint_path', ogr.OFTString, 0],
        ['Metadata_Ingest_Time', 'Metadata_Ingest_Time', 'Metadata_Ingest_Time', ogr.OFTDateTime, 0],
        ['Raster_Ingest_Time', 'Raster_Ingest_Time', 'Raster_Ingest_Time', ogr.OFTDateTime, 0],
        ]

    # Get list of field names (expected)
    fnames = []
    
    for element in fieldvaluelist:
        fnames.append(element[1])

    # Check if the geopackage contains a layer, and create it, and its fields
    #    (attributes), if it does not
    layers = data_source.GetLayerCount()
    layerNames = []
    if layers > 0:
        for i in range(layers):
            layerNames.append(data_source.GetLayer(i).GetName())
            if verbose_g:
                print(f'Found layer: {data_source.GetLayer(i).GetName()}')
    # No layer found?
    if not ieo.landsatshp in layerNames:
        layer = data_source.CreateLayer(ieo.landsatshp, ieo.prj, ogr.wkbPolygon)
        
        for element in fieldvaluelist:
            field_name = ogr.FieldDefn(element[1], element[3])
            if element[4] > 0:
                field_name.SetWidth(element[4])
            layer.CreateField(field_name)
    
    # One layer found?
    # elif layers == 1:
    #     layer_name = data_source.GetLayer(0).GetName()
        
    #     # Check the name of the layer is that specified in the config (.ini) file
    #     if layer_name != ieo.landsatshp:                
    #         ieo.logerror(ieo.catgpkg, \
    #                      'Geopackage layer name is incorrect (expected={}; actual={}).'.format(ieo.catgpkg, layer_name), \
    #                      errorfile = errorfile)
    #         print('Error: Geopackage layer name is incorrect (expected={}; actual={}). Exiting.'.format(ieo.catgpkg, layer_name))
    #         sys.exit()
    else:   # Layer name is as expected
        layer = data_source.GetLayer(ieo.landsatshp)
 
        # Get list of field names (actual)  
        layerDefinition = layer.GetLayerDefn()
 
        shpfnames = []
                
        for i in range(layerDefinition.GetFieldCount()):
            shpfnames.append(layerDefinition.GetFieldDefn(i).GetName())
            
        # Find missing fields and create them
        for fname in fnames:
            if (not fname in shpfnames) and (not fname.lower() in shpfnames):
                if verbose_g:
                    print(f'Creating missing field: {fname}')
                i = fnames.index(fname)
                field_name = ogr.FieldDefn(fnames[i], fieldvaluelist[i][3])
                if fieldvaluelist[i][4] > 0:
                    field_name.SetWidth(fieldvaluelist[i][4])
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
        print('The IEO Landsat catalog is currently empty.')
    else:
        layer.StartTransaction()
        
        feature = layer.GetNextFeature()
                
        while feature:
            datetuple = None
            
            # Check if bad feature and, if so, delete it without requesting reimport
            try:
                sceneID = feature.GetField("sceneID")
            except:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                if verbose_g:
                    print(exc_type, fname, exc_tb.tb_lineno)
                    print('ERROR: bad feature, deleting.')
                layer.DeleteFeature(feature.GetFID())
                ieo.logerror('{}/{}'.format(ieo.catgpkg, ieo.landsatshp), '{} {} {}'.format(exc_type, fname, exc_tb.tb_lineno), errorfile = errorfile)
                feature = layer.GetNextFeature()
                continue
            
            # Display progress
            fnum += 1
            
            print('\rInspecting feature {:5d} of {}, scene {}.\r'\
                  .format(fnum, featureCount, sceneID), end = '')
            
            # Add scene to list
            scenelist.append(sceneID)
                        
            # Check that feature has invalid Sensor ID (proxy for invalid metadata)
            #    - and, if so, delete it and flag for reimportation
            if not feature.GetField('SensorID') in ['TM', 'ETM', 'OLI', 'TIRS', 'OLI_TIRS']:
                if verbose_g:
                    print('ERROR: missing metadata for SceneID {}. Feature will be deleted from shapefile and reimported.'.format(sceneID))
                ieo.logerror(sceneID, 'Feature missing metadata, deleted, reimportation required.')
                reimport.append(datetime.datetime.strptime(sceneID[9:16], '%Y%j'))
                layer.DeleteFeature(feature.GetFID())
                errors['total'] += 1
                errors['metadata'] += 1
                
            else:    # Sensor ID is valid
                
                # Check that feature has a valid Modification Date field value
                #    - and, if not, flag the feature for updating of that field
                try:
                    mdate = feature.GetField('dateUpdated')
                    if '/' in mdate:
                        datetimestr = '%Y/%m/%d'
                    else:
                        datetimestr = '%Y-%m-%d'
                    if ':' in mdate:
                        datetimestr += ' %H:%M:%S'
                    datetuple = datetime.datetime.strptime(mdate, datetimestr)
                    if not lastupdate or datetuple > lastupdate:
                        lastupdate = datetuple
                        lastmodifiedDate = mdate
                except:
                    if verbose_g:
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                        print(exc_type, fname, exc_tb.tb_lineno)
                        # print('ERROR: modifiedDate information missing for SceneID {}, adding to list.'.format(sceneID))
                    ieo.logerror(sceneID, 'Modification date missing.', errorfile = errorfile)
                    # updatemissing.append(sceneID)
                    errors['total'] += 1
                    errors['date'] += 1
                
                # Check that feature has a valid geometry setting value
                #    - and, if not, flag the feature for updating of that setting
                try:
                    geom = feature.GetGeometryRef()
                    env = geom.GetEnvelope()
                    if env[0] == env[1] or env[2] == env[3]:
                        if verbose_g:
                            print('Bad geometry identified for SceneID {}, adding to the list.'.format(sceneID))
                        ieo.logerror(sceneID, 'Bad/missing geometry.')
                        badgeom.append(sceneID)
                except:
                    if verbose_g:
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                        print(exc_type, fname, exc_tb.tb_lineno)
                        print('Bad geometry identified for SceneID {}, adding to the list.'.format(sceneID), errorfile = errorfile)
                    ieo.logerror(sceneID, 'Bad/missing geometry.')
                    badgeom.append(sceneID)
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
        
        print('The IEO Landsat catalog currently contains {} valid features.'.format(len(scenelist)))
        
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

def updateIEO(layer, fieldvaluelist, fnames, queryfieldnames, \
              sceneIDs, scenedict):

    # This section borrowed from https://pcjericks.github.io/py-gdalogr-cookbook/projection.html
    # Lat/ Lon WGS-84 to local projection transformation
    source = osr.SpatialReference() # Lat/Lon WGS-64
    source.ImportFromEPSG(4326)
    
    target = ieo.prj
    
    transform = osr.CoordinateTransformation(source, target)    
    
    # Initialise the iteration counter for the following loop
    filenum = 1
    
    # Loop through the details of scenes in the scene dictionary
    for sceneID in sceneIDs:
        
        # =============================================================================
        # Processing for addition of a new layer feature (scene) begins here
        # =============================================================================

        # Newly-acquired scene with coordinates?: Add to the catalog's geopackage layer
        if not (scenedict[sceneID]['updategeom'] or scenedict[sceneID]['updatemodifiedDate']) and ('coords' in scenedict[sceneID].keys()):
            print('\rAdding feature {} for scene number {:5d} of {}.\r'.format(sceneID, filenum, len(sceneIDs)), \
                  end='')
           
            # Create the feature
            feature = ogr.Feature(layer.GetLayerDefn())
            
            # Add field attributes
            feature.SetField('sceneID', sceneID)
                       
            # Loop through the atributes for the current scene in the dictionary
            for key in scenedict[sceneID].keys():
                
                # One of the attributes of interest and value available?
                if (scenedict[sceneID][key]) and key in queryfieldnames:
                    
                    try:
                        # Date attribute?: Add as field to feature 
                        if fieldvaluelist[queryfieldnames.index(key)][3] == ogr.OFTDateTime:
                            
                            # In string format?: Convert to datetime format
                            if isinstance(scenedict[sceneID][key], str):
                                timestr = '%Y-%m-%d'
                                if '/' in scenedict[sceneID][key]:
                                    scenedict[sceneID][key] = scenedict[sceneID][key].replace('/', '-')
                                elif scenedict[sceneID][key][4] == ':':
                                    timestr = '%Y:%j:%H:%M:%S.%f'
                                scenedict[sceneID][key] = datetime.datetime.strptime(scenedict[sceneID][key], timestr)
                            
                            feature.SetField(fnames[queryfieldnames.index(key)], \
                                             scenedict[sceneID][key].year, \
                                             scenedict[sceneID][key].month, \
                                             scenedict[sceneID][key].day, \
                                             scenedict[sceneID][key].hour, \
                                             scenedict[sceneID][key].minute, \
                                             scenedict[sceneID][key].second, 100)
                        
                        # Non-date attribute?: Add (string) as field to feature
                        else:
                            feature.SetField(fnames[queryfieldnames.index(key)], \
                                             scenedict[sceneID][key])
                   
                    # Error detected during feature field creation? Log it 
                    except Exception as e:
                        if verbose_g:
                            exc_type, exc_obj, exc_tb = sys.exc_info()
                            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                            print(exc_type, fname, exc_tb.tb_lineno)
                            print('Error with SceneID {}, fieldname = {}, value = {}: {}'\
                                  .format(sceneID, \
                                          fnames[queryfieldnames.index(key)], 
                                          scenedict[sceneID][key], e))
                        ieo.logerror(key, e, errorfile = errorfile)
            
            # Set the geometry details for the new feature
            coords = scenedict[sceneID]['coords']

            # Create ring using the current scene's coordinate data
            ring = ogr.Geometry(ogr.wkbLinearRing)
            
            for coord in coords:
                ring.AddPoint(coord[1], coord[0])
                
            if not coord[0] == coords[0][0] and coord[1] == coords[0][1]:
                ring.AddPoint(coords[0][1], coords[0][0])
            
            # Create polygon using the ring
            poly = ogr.Geometry(ogr.wkbPolygon)
            poly.AddGeometry(ring)
            poly.Transform(transform)   # Convert to local projection
            feature.SetGeometry(poly)
            now = datetime.datetime.now()
            feature.SetField('Metadata_Ingest_Time', now.strftime('%Y-%m-%d %H:%M:%S'))
            # Create the new feature
            layer.CreateFeature(feature)
            
            # Free the new features' resources 
            feature.Destroy()
        
        # =============================================================================
        # Processing for update of an existing layer feature (scene) begins here
        # =============================================================================
        
        else:
            # Reposition at the first feature in the geopackage 
            layer.ResetReading()
            
            # Loop through the features until the one for the current scene is reache
            for feature in layer:
                
                if feature.GetField('sceneID') == sceneID:
                    print('\rFixing feature {} for scene number {:5d} of {}.\r'.format(sceneID, filenum, len(sceneIDs)), \
                          end='')
                    
                    # Geometry update required?
                    if scenedict[sceneID]['updategeom']: 
                        # print('Updating geometry for SceneID {}.'.format(sceneID))
                        
                        # Set the geometry details for the current feature
                        coords = scenedict[sceneID]['coords']
                        
                        # Create ring using the current scene's coordinate data
                        ring = ogr.Geometry(ogr.wkbLinearRing)
                        
                        for coord in coords:
                            ring.AddPoint(coord[0], coord[1])
                            
                        if not coord[0] == coords[0][0] and coord[1] == coords[0][1]:
                            ring.AddPoint(coord[0][0], coord[0][1])
                    
                        # Create polygon using the ring
                        poly = ogr.Geometry(ogr.wkbPolygon)
                        poly.AddGeometry(ring)
                        poly.Transform(transform)   # Convert to local projection
                        feature.SetGeometry(poly)
                        
                    # Modification date update required?   
                    if scenedict[sceneID]['updatemodifiedDate']:
                        # print('Updating modification date for SceneID {}.'.format(sceneID))
                        feature.SetField('dateUpdated', 
                                             scenedict[sceneID]['publishDate'].year, \
                                             scenedict[sceneID]['publishDate'].month, \
                                             scenedict[sceneID]['publishDate'].day, \
                                             scenedict[sceneID]['publishDate'].hour, \
                                             scenedict[sceneID]['publishDate'].minute, \
                                             scenedict[sceneID]['publishDate'].second, 100)
                        
                    # Update the feature
                    layer.SetFeature(feature)
                    
                    # Free the feature's resources 
                    feature.Destroy()
                    
                    # Exit the update feature's for loop
                    break
                    
        # Increment the loop's processsed feature counter
        filenum += 1

    return

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

def valArgs(startdate, enddate, \
            baseURL, maxResults, \
            thumbnails, savequeries, verbose):
       
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
    if urlparse(baseURL).scheme not in ('http', 'https'):
        errMsg = 'Invalid argument value: Base URL is not valid.'
        print(errMsg)
        ieo.logerror(baseURL, errMsg)
        sys.exit()    

    # Validate Naximum Results
    if not isinstance(maxResults, int):
        errMsg = 'Invalid argument value: Maximum Results must be an integer value.'
        print(errMsg)
        ieo.logerror(maxResults, errMsg)
        sys.exit()   
    elif maxResults < 1:
        errMsg = 'Invalid argument value: Maximum Results must be greater than zero.'
        print(errMsg)
        ieo.logerror(baseURL, errMsg)
        sys.exit()
        
    # Validate Thumbnails flag
    if not isinstance(thumbnails, bool):
        errMsg = 'Invalid argument value: Thumbnails setting must be True or False.'
        print(errMsg)
        ieo.logerror(thumbnails, errMsg)
        sys.exit()          

    # Validate Save Queries flag
    if not isinstance(savequeries, bool):
        errMsg = 'Invalid argument value: Save Queries setting must be True or False.'
        print(errMsg)
        ieo.logerror(savequeries, errMsg)
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

def main(username=None, password=None, catalogID='EE', version='stable', \
         startdate='1982-07-16', enddate=None, \
         MBR=None, baseURL='https://m2m.cr.usgs.gov/api/api/json/', \
             maxResults=50000, thumbnails=False, savequeries=False, \
             verbose=False):

    # =============================================================================
    # Declare and initialise the needed global variables
    # =============================================================================
    
    global errorfile, errorsfound, pathrows, verbose_g
    
    errorfile = os.path.join(ieo.logdir, 'Landsat_inventory_download_errors.csv')
    
    verbose_g = verbose

    # =============================================================================
    # Validate selected argument values
    # =============================================================================
    
    print('\n***** Validating the argument values.....')
    
    valArgs(startdate, enddate, \
            baseURL, maxResults, \
            thumbnails, savequeries, verbose)

    # =============================================================================
    # Handle any missing arguments
    # =============================================================================
    
    print('\n***** Checking some key configuration settings.....')
    
    # USGS username or password arguments not provided?: Prompt for them now
    if not (username and password):
        if not username:
            username = input('USGS/ERS username: ')
        if not password:
            password = getpass.getpass('USGS/ERS password: ')

    # End date argument not provided?: Use the current date
    if not enddate:
       enddate = datetime.datetime.today().strftime('%Y-%m-%d')
       
    print('\nChecks completed.')
 
    print('\n***** Getting the Minimum Bounding Rectangle (MBR) for the region of interest.....\n')
 
    # Get WRS-2 Path-Row numbers - using either the ieo.WRS2 geopackage or the updateshp.ini file's path-row range specification
    paths, rows, pathrowstrs = getPathsRows(ieo.config['Landsat']['useWRS2'], \
                                            ieo.config['Landsat']['pathrowvals'])

    # Parse the Minimum Bounding Rectangle argument if provided;
    #    otherwise, construct it
    if MBR:
        MBR = MBR.split(',')
        if len(MBR) != 4:
            ieo.logerror('--MBR', 'Total number of coordinates does not equal four.', errorfile = errorfile)
            print('Error: Improper number of coordinates for --MBR set (must be four). Either remove this option (will use default values) or fix. Exiting.')
            sys.exit()
    else:
        MBR = getMBR(baseURL, version, paths, rows)
    
    print('\nMBR set: {}.'.format(MBR))

    # =============================================================================
    # Mainline processing proper begins here
    # =============================================================================
    
    print('\n***** Opening the IEO Landsat catalog ({}).....\n'.format(ieo.catgpkg))
    
    # Open the IEO geopackage - or create it if it does not exist
    data_source, layer, fieldvaluelist, fnames = openIEO()
    
    print('\n***** Inspecting the current contents of the IEO Landsat catalog.....\n')
    
    # Retrieve the details of the features (scenes) that have alrready been added
    #    to the IEO geopackage (catalog)
    scenelist, updatemissing, badgeom, lastmodifiedDate = \
        readIEO(layer)
    
    print('\n***** Opening a connection to the USGS/ERS {} service.....\n'.format(catalogID))
    
    # Connect to the specified catalog (EarthExplorer) on the specified USGS server
    apiKey = openUSGS(baseURL, version, catalogID, username, password)
    
    # Place the API key in the HTTP headers
    headers = {'X-Auth-Token': apiKey}  
    
    print('\n***** Retrieving the details of the in-scope USGS/ERS Landsat scenes.....\n')
    
    scenedict, queryfieldnames = \
        readUSGS(baseURL, version, headers, \
                    MBR, pathrowstrs, \
                    startdate, enddate, \
                    maxResults, savequeries, \
                    scenelist, updatemissing, badgeom, lastmodifiedDate, \
                    fieldvaluelist)
    
    sceneIDs = scenedict.keys()
    
    if len(sceneIDs) == 0:
        print('\nNo scenes to be added or updated in IEO geopackage layer.\n')
    else:
        print('\n***** Updating the IEO catalog (geopackage).....\n')
        
        print('Total scenes to be added or updated to IEO geopackage layer: {}\n'.format(len(sceneIDs)))

        updateIEO(layer, fieldvaluelist, fnames, queryfieldnames, \
                  sceneIDs, scenedict)
    
    # If enabled, download the thumbnails images for any new or modified scenes 
    if thumbnails:
        print('\r***** Downloading thumbnails.....' + (' ' * 50) + '\n')
        
        getThumbnails(layer, updatemissing, badgeom)
    
    # Close the USGS/ERS connection
    print('\r***** Closing the connection to the USGS/ERS {} service.....'.format(catalogID))
    closeUSGS(baseURL, version, headers)

    # Close the IEO geopackage
    print('\n***** Closing the IEO catalog (geopackage).....')
    data_source = None

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
    parser = argparse.ArgumentParser('This script imports LEDAPS-processed scenes into the local library. It stacks images and converts them to the locally defined projection in IEO, and adds ENVI metadata.')
    parser.add_argument('-u','--username', type = str, default = None, help = 'USGS/EROS Registration System (ERS) username.')
    parser.add_argument('-p', '--password', type = str, default = None, help = 'USGS/EROS Registration System (ERS) password.')
    parser.add_argument('-c', '--catalogID', type = str, default = 'EE', help = 'USGS/EROS Catalog ID (default = "EE").')
    parser.add_argument('-v', '--version', type = str, default = "stable", help = 'JSON version, default = stable.')
    parser.add_argument('--startdate', type = str, default = "1982-07-16", help = 'Start date for query in YYYY-MM-DD format. (Default = 1982-07-16, e.g., Landsat 4 launch date).')
    parser.add_argument('--enddate', type = str, default = None, help = "End date for query in YYYY-MM-DD format. (Default = today's date).")
    parser.add_argument('-m', '--MBR', type = str, default = None, help = 'Minimum Bounding Rectangle (MBR) coordinates in decimal degrees in the following format (comma delimited, no spaces): lower left latitude, lower left longitude, upper right latitude, upper right longitude. If not supplied, these will be determined from WRS-2 Paths and Rows in updateshp.ini.')
    parser.add_argument('-b', '--baseURL', type = str, default = 'https://m2m.cr.usgs.gov/api/api/json/', help = 'Base URL to use excluding JSON version (Default = "https://m2m.cr.usgs.gov/api/api/json/").')
    parser.add_argument('--maxResults', type = int, default = 50000, help = 'Maximum number of results to return (1 - 50000, default = 50000).')
    parser.add_argument('--thumbnails',  action = 'store_true', help = 'Download thumbnails (default = False).')
    parser.add_argument('--savequeries', action = 'store_true', help = 'Save queries.')
    parser.add_argument('--verbose', action = 'store_true', help = 'Display more messages during migration.')
    
    args = parser.parse_args()
 
    # Pass the parsed arguments to mainline processing   
    main(args.username, args.password, args.catalogID, args.version, args.startdate, args.enddate, \
         args.MBR, args.baseURL, args.maxResults, args.thumbnails, args.savequeries, \
             args.verbose)