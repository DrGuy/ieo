# -*- coding: utf-8 -*-
"""
Created on Tue Mar 22 14:41:27 2022

@author: guyse
"""
import os, sys, glob, datetime, argparse
import xml.etree.ElementTree as et
from osgeo import ogr, osr

try: # This is included as the module may not properly install in Anaconda.
    import ieo
except:
    ieodir = os.getenv('IEO_INSTALLDIR')
    if not ieodir:
        ieodir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        # print('Error: IEO failed to load. Please input the location of the directory containing the IEO installation files.')
        # ieodir = input('IEO installation path: ')
    if os.path.isfile(os.path.join(ieodir, 'ieo.py')):
        sys.path.append(ieodir)
        import ieo, S3ObjectStorage
    else:
        print('Error: that is not a valid path for the IEO module. Exiting.')
        sys.exit()

parser = argparse.ArgumentParser('This script imports Sentinel-2 Scihub metadata into PostGIS.')
parser.add_argument('-p', '--password', default = None, type = str, help = 'Password to log into PostGIS server.')
args = parser.parse_args()

source_prj = osr.SpatialReference()
source_prj.ImportFromEPSG(4326)
transform = osr.CoordinateTransformation(source_prj, ieo.prj)
IE_ds = ogr.Open(f'{ieo.ieogpkg} password={args.password}', 0)
IE_layer = IE_ds.GetLayer('Ireland_Island')
IE_feat = IE_layer.GetNextFeature()
IE_geom = IE_feat.GetGeometryRef()

ds = ogr.Open(f'{ieo.catgpkg} password={args.password}', 1)
layer = ds.GetLayer(ieo.Sen2shp)
layerDefn = layer.GetLayerDefn()
fieldlist = []
for i in range(layerDefn.GetFieldCount()):
    fieldName =  layerDefn.GetFieldDefn(i).GetName()
    if not fieldName in fieldlist:
        fieldlist.append(fieldName)

outdir = r'C:\Users\guyse\OneDrive - EOanalytics.ie\EOanalytics\Projects\Github\test\scihub'

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

def createField(layer, fieldlist, fieldname):
    if fieldname == 'ORBIT_NUMBER':
        fieldtype = ogr.OFTInteger
    else:
        fieldtype = ogr.OFTString
    print(f'Creating new field: {fieldname}')
    field_name = ogr.FieldDefn(fieldname, fieldtype)
    layer.CreateField(field_name)
    fieldlist.append(fieldname)
    return layer, fieldlist

scenedict = {}

tagdict = {
    'title': {},
    'link': {'href': [], 'rel': []},
    'id': {},
    'summary': {},
    'ondemand': {},
    'date': {
            'name': {
                    'ingestiondate' : 'GENERATION_TIME',
                    'generationdate' : 'GENERATION_TIME',
                    'beginposition' : 'PRODUCT_START_TIME',
                    'endposition' : 'PRODUCT_STOP_TIME',
                    }
            },
    'double': {
            'name': {
                    'cloudcoverpercentage' : 'Cloud_Coverage_Assessment',
                    'highprobacloudspercentage' : 'HIGH_PROBA_CLOUDS_PERCENTAGE',
                    'mediumprobacloudspercentage' : 'MEDIUM_PROBA_CLOUDS_PERCENTAGE',
                    'notvegetatedpercentage' : 'NOT_VEGETATED_PERCENTAGE',
                    'snowicepercentage' : 'SNOW_ICE_PERCENTAGE',
                    'unclassifiedpercentage' : 'UNCLASSIFIED_PERCENTAGE',
                    'vegetationpercentage' : 'VEGETATION_PERCENTAGE',
                    'waterpercentage' : 'WATER_PERCENTAGE',
                    'illuminationazimuthangle' : 'illuminationazimuthangle',
                    "illuminationzenithangle" : "illuminationzenithangle",
                    
                    }
            },
    'int': {
            'name':
                {
                    
                }
            },
    'str': {
            'name': {
                    'footprint' : 'WKT',
                    "processinglevel" : "PROCESSING_LEVEL",
                    "processingbaseline" : "PROCESSING_BASELINE",
                    "format" : 'PRODUCT_FORMAT',
                    "instrumentshortname" : 'INSTRUMENT_SHORT_NAME',
                    "instrumentname" : 'INSTRUMENT_NAME',
                    "s2datatakeid" : 'S2_DATATAKE_ID',
                    "platformidentifier" : 'PLATFORM_IDENTIFIER',
                    "orbitdirection" : 'SENSING_ORBIT_DIRECTION',
                    "platformserialidentifier" : 'SPACECRAFT_NAME',
                    "producttype" : 'PRODUCT_TYPE',
                    "platformname" : 'PLATFORM_NAME',
                    "size" : 'PRODUCT_SIZE',
                    "level1cpdiidentifier": "level1cpdiidentifier",
                    "filename" : "scihub_filename",
                    "granuleidentifier" : "granuleidentifier",
                    "datastripidentifier" : "datastripidentifier",
                    
                    } 
            }
    }

flist = glob.glob(os.path.join(outdir, 'scihub_query_S2MSI2Ap_*.xml'))
updatedfeats = 0
newfeats = 0
for f in flist:
    scenedict = {}
    print(f'Opening XML file: {f} ({flist.index(f) + 1}/{len(flist)})')
    tree = et.parse(f)
    root = tree.getroot()
    for child in root:
        if child.tag.endswith('{http://www.w3.org/2005/Atom}entry'):
            for s in child:
                if s.tag.endswith('title'):
                    ProductID = s.text
                    print(f'Analyzing metadata for ProductID: {ProductID}')
                    scenedict[ProductID] = {}
                elif s.tag.endswith('link'):
                    if 'rel' in s.attrib.keys():
                        if s.attrib['rel'] == 'icon':
                            scenedict[ProductID]['PREVIEW_IMAGE_URL'] = s.attrib['href']
                        elif s.attrib['rel'] == 'alternative':
                            scenedict[ProductID]['PRODUCT_URI_2A'] = s.attrib['href']
                    else:
                        scenedict[ProductID]['PRODUCT_URI'] = s.attrib['href']
                elif s.tag.endswith('int'):
                    if s.attrib['name'] == 'relativeorbitnumber': 
                        scenedict[ProductID]['SENSING_ORBIT_NUMBER'] = int(s.text)
                    elif s.attrib['name'] == 'orbitnumber': 
                        scenedict[ProductID]['ORBIT_NUMBER'] = int(s.text)
                elif s.tag.endswith('id'):
                    scenedict[ProductID]['SciHub_UUID'] = s.text
                elif s.tag.endswith('ondemand'):
                    scenedict[ProductID]['SciHub_On_Demand'] = s.text
                elif s.tag.endswith('date'):
                    scenedict[ProductID][tagdict['date']['name'][s.attrib['name']]] = s.text
                    if tagdict['date']['name'][s.attrib['name']] == 'beginposition':
                         scenedict[ProductID]['acquisitionDate'] = s.text
                elif s.tag.endswith('double'):
                    scenedict[ProductID][tagdict['double']['name'][s.attrib['name']]] = float(s.text)
                elif s.tag.endswith('str'):
                    if s.attrib['name'] in tagdict['str']['name'].keys():
                        scenedict[ProductID][tagdict['str']['name'][s.attrib['name']]] = s.text
            parts = ProductID.split('_')
            acqdate = datetime.datetime.strptime(parts[2], '%Y%m%dT%H%M%S')
            scenedict[ProductID]['sceneID'] = f'{parts[0]}{parts[5]}{acqdate.strftime("%Y%j")}ESA00' # Creates a fake USGS-like Scene Identifier
            # print(f'{ProductID} geometry: {scenedict[ProductID]["WKT"]}')
            geom = ogr.CreateGeometryFromWkt(scenedict[ProductID]['WKT'])
            
            geom.Transform(transform)
            mgeom = ogr.Geometry(ogr.wkbMultiPolygon)
            mgeom.AddGeometry(geom)
            keylist = set(scenedict[ProductID].keys()).difference(fieldlist)
            if len(keylist) > 0:
                for x in keylist:
                    layer, fieldlist = createField(layer, fieldlist, x)
            p = mgeom.Intersection(IE_geom)
            if p:
                print(f'{ProductID} intersects Ireland. Processing further.')
                updated = False
                layer.StartTransaction()
                layer.SetAttributeFilter(f'"ProductID" = \'{ProductID}\'')
                if layer.GetFeatureCount() > 0:
                    feature = layer.GetNextFeature()
                    print(f'Found feature for {ProductID}.')
                    # update fields with missing data
                    for key in scenedict[ProductID].keys():
                        value = feature.GetField(key)
                        if not value:
                            print(f'{ProductID}: Updating missing field {key}: {scenedict[ProductID][key]}')
                            feature.SetField(key, scenedict[ProductID][key])
                            updated = True
                if updated:
                    layer.SetFeature(feature)
                    feature.Destroy()  
                    updatedfeats += 1
                else:
                    print(f'Creating feature for ProductID: {ProductID}')
                    feature = ogr.Feature(layer.GetLayerDefn())
                    feature.SetField('ProductID', ProductID)
                    for key in scenedict[ProductID].keys():
                        feature.SetField(key, scenedict[ProductID][key])
                    feature.SetGeometry(geom)
                    layer.CreateFeature(feature)
                    print('Feature created.')
                    # Free the new features' resources 
                    feature.Destroy()
                    newfeats += 1
                layer.CommitTransaction()
print(f'Summary: {newfeats} created, {updatedfeats} updated.')                
print('Processing complete.')
                
                
                    
                    
                
                        
