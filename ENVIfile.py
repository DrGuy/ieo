#/usr/bin/python
# By Guy Serbin, EOanalytics Ltd.
# Talent Garden Dublin, Claremont Ave. Glasnevin, Dublin 11, Ireland
# email: guyserbin <at> eoanalytics <dot> ie

# Irish Earth Observation (IEO) Python Module
# version 1.5

import os, sys, shutil, datetime
from osgeo import osr
from pkg_resources import resource_stream, resource_string, resource_filename, Requirement
if sys.version_info[0] == 2:
    import ConfigParser as configparser
else:
    import configparser

# Read in config information
global prjval, projinfo, mapinfostr, gcsstring, prj
config = configparser.ConfigParser()
# ieoconfigdir = os.getenv('IEO_CONFIGDIR')
# if ieoconfigdir:
#     configfile = os.path.join(ieoconfigdir, 'ieo.ini')
# else:
# configfile = 'config/ieo.ini'
# config_location = resource_filename(Requirement.parse('ieo'), configfile)
# config_location = resource_filename(Requirement.parse('ieo'), 'config/ieo.ini')
cwd = os.path.abspath(os.path.dirname(__file__))    
print(cwd)
configfile = os.path.join(cwd, 'config/ieo.ini')
config.read(configfile) # config_path

# Spatial variables
prjvalstr = config['Projection']['proj']
if ':' in prjvalstr:
    i = prjvalstr.find(':') + 1
    prjval = int(prjvalstr[i:])
else:
    prjval = int(prjvalstr) # This assumes that the config value contains on the EPSG value.

prj = osr.SpatialReference()
prj.ImportFromEPSG(prjval) # "EPSG:2157"

# Shamelessly copied from http://pydoc.net/Python/spectral/0.17/spectral.io.envi/
# import numpy as np
# dtype_map = [('1', 'uint8'),                   # unsigned byte
#              ('2', 'int16'),                   # 16-bit int
#              ('3', 'int32'),                   # 32-bit int
#              ('4', 'float32'),                 # 32-bit float
#              ('5', 'float64'),                 # 64-bit float
#              ('6', 'complex64'),               # 2x32-bit complex
#              ('9', 'complex128'),              # 2x64-bit complex
#              ('12', 'uint16'),                 # 16-bit unsigned int
#              ('13', 'uint32'),                 # 32-bit unsigned int
#              ('14', 'int64'),                  # 64-bit int
#              ('15', 'uint64')]                 # 64-bit unsigned int
# envi_to_dtype = dict((k, np.dtype(v).char) for (k, v) in dtype_map)
# dtype_to_envi = dict(tuple(reversed(item)) for item in list(envi_to_dtype.items()))
dtype_to_envi = {
    'uint8': '1',                   # unsigned byte
    'int16': '2',                   # 16-bit int
    'int32': '3',                   # 32-bit int
    'float32': '4',                 # 32-bit float
    'float64': '5',                 # 64-bit float
    'complex64': '6',               # 2x32-bit complex
    'complex128': '9',              # 2x64-bit complex
    'uint16': '12',                 # 16-bit unsigned int
    'uint32': '13',                 # 32-bit unsigned int
    'int64': '14',                  # 64-bit int
    'uint64': '15'                 # 64-bit unsigned int
    }


headerfields = 'acquisition time,band names,bands,bbl,byte order,class lookup,class names,class values,classes,cloud cover,complex function,coordinate system string,data gain values,data ignore value,data offset values,data reflectance gain values,data reflectance offset values,data type,default bands,default stretch,dem band,dem file,description,file type,fwhm,geo points,header offset,interleave,lines,map info,pixel size,product type,projection info,read procedures,reflectance scale factor,rpc info,samples,security tag,sensor type,solar irradiance,spectra names,sun azimuth,sun elevation,wavelength,wavelength units,x start,y start,z plot average,z plot range,z plot titles'.split(',')
headerdict = {'default':dict.fromkeys(headerfields)}
headerdict['default'].update({'parent rasters': []})

headerdict['Fmask'] = headerdict['default'].copy()
headerdict['Fmask'].update({
    'description': 'Landsat Fmask Cloud Mask %s',  # sceneid
    'band names': ['Fmask Cloud Mask %s'], # sceneid
    'classes': 6,
    'class names': ['Clear land', 'Clear water', 'Cloud shadow', 'Snow/ ice', 'Cloud', 'No data'],
    'class lookup': [
    [0,     255,    0],   
    [0,     0,      255],   
    [127,   127,    127],   
    [0,     255,    255],   
    [255,   255,    255],   
    [0,     0,      0]],
    'class values': [0, 1, 2, 3, 4, 255],
    'defaultbasefilename': '%s_cfmask.dat', # sceneid
    'data ignore value': 255})

headerdict['pixel_qa'] = headerdict['default'].copy()
headerdict['pixel_qa'].update({
    'description': 'Landsat Pixel QA Layer %s',  # sceneid
    'band names': ['Pixel QA'], # sceneid
    'defaultbasefilename': '%s_pixel_qa.dat', # sceneid
    'data ignore value': 1})

headerdict['SR_QA_AEROSOL'] = headerdict['default'].copy() # Added in version 1.5 
headerdict['SR_QA_AEROSOL'].update({
    'description': 'Landsat Aerosol QA Layer %s',  # sceneid
    'band names': ['Aerosol QA'], # sceneid
    'defaultbasefilename': '%s_SR_QA_AEROSOL.dat', # sceneid
    'data ignore value': 1})

headerdict['QA_RADSAT'] = headerdict['default'].copy() # Added in version 1.5 
headerdict['QA_RADSAT'].update({
    'description': 'Landsat Radiometric Saturation QA Layer %s',  # sceneid
    'band names': ['RADSAT QA'], # sceneid
    'defaultbasefilename': '%s_QA_RADSAT.dat', # sceneid
    })

headerdict['Landsat ST'] = headerdict['default'].copy() # Added in version 1.5
headerdict['Landsat ST'].update({
    'description': 'Landsat Surface Temperature (%s)',  # sceneid
    'band names': ['Surface Temperature'], # sceneid
    'defaultbasefilename': '%s_ST.dat', # sceneid
    'data ignore value': -9999}) 

headerdict['Landsat Band6'] = headerdict['default'].copy()
headerdict['Landsat Band6'].update({
    'description': 'LEDAPS Brightness Temperature (%s)',  # sceneid
    'band names': ['TIR'], # sceneid
    'wavelength': [11.450000],
    'wavelength units': 'Micrometers',
    'fwhm':[2.100000],
    'defaultbasefilename': '%s_BT.dat', # sceneid
    'data ignore value': -9999}) 

headerdict['Landsat TIR'] = headerdict['default'].copy()
headerdict['Landsat TIR'].update({
    'description': 'Landsat Brightness Temperature (%s)',  # sceneid
    'band names': ['TIR 1', 'TIR 2'], # sceneid
    'wavelength': [10.895000, 12.005000],
    'wavelength units': 'Micrometers',
    'fwhm': [0.590000, 1.010000],
    'defaultbasefilename': '%s_BT.dat', # sceneid
    'data ignore value': -9999}) 

headerdict['Landsat TM'] = headerdict['default'].copy()
headerdict['Landsat TM'].update({
    'description': 'Landsat Surface Reflectance (%s)',  # sceneid
    'band names': ['Blue', 'Green', 'Red', 'NIR', 'SWIR 1', 'SWIR 2'], # sceneid
    'wavelength': [0.485000, 0.560000, 0.662000, 0.830000, 1.648000, 2.215000],
    'wavelength units': 'Micrometers',
    'fwhm': [0.070000, 0.080000, 0.060000, 0.130000, 0.200000, 0.270000],
    'default bands': [6, 4, 1],
    'defaultbasefilename': '%s_ref.dat', # sceneid
    'data ignore value': -9999}) 

headerdict['Landsat ETM+'] = headerdict['default'].copy()
headerdict['Landsat ETM+'].update({
    'description': 'Landsat Surface Reflectance (%s)',  # sceneid
    'band names': ['Blue', 'Green', 'Red', 'NIR', 'SWIR 1', 'SWIR 2'], # sceneid
    'wavelength': [0.483000, 0.560000, 0.662000, 0.835000, 1.648000, 2.206000],
    'wavelength units': 'Micrometers',
    'fwhm': [0.070000, 0.080000, 0.060000, 0.120000, 0.200000, 0.260000],
    'default bands': [6, 4, 1],
    'defaultbasefilename': '%s_ref.dat', # sceneid
    'data ignore value': -9999}) 

headerdict['Landsat OLI'] = headerdict['default'].copy()
headerdict['Landsat OLI'].update({
    'description': 'Landsat Surface Reflectance (%s)',  # sceneid
    'band names': ['Coastal aerosol', 'Blue', 'Green', 'Red', 'NIR', 'SWIR 1', 'SWIR 2'], # sceneid
    'wavelength': [0.443000, 0.482600, 0.561300, 0.654600, 0.864600, 1.609000, 2.201000],
    'wavelength units': 'Micrometers',
    'default bands': [7, 5, 2],
    'fwhm': [0.016000, 0.060100, 0.057400, 0.037500, 0.028200, 0.084700, 0.186700],
    'defaultbasefilename': '%s_ref.dat', # sceneid
    'data ignore value': -9999}) 
    
headerdict['Landsat MSS'] = headerdict['default'].copy()
headerdict['Landsat MSS'].update({
    'description': 'Landsat Surface Reflectance (%s)',  # sceneid
    'band names': ['Green', 'Red', 'Red Edge', 'NIR'], # sceneid
    'wavelength': [0.55, 0.65, 0.75, 0.95],
    'wavelength units': 'Micrometers',
    'fwhm': [0.0500, 0.0500, 0.0500, 0.1500],
    'default bands': [4, 2, 1],
    'defaultbasefilename': '%s_ref.dat' # sceneid
    }) 

headerdict['Sentinel-2'] = headerdict['default'].copy()
headerdict['Sentinel-2'].update({
    'description': 'Sentinel-2 Surface Reflectance (%s)',  # sceneid
    'band names': ['Coastal aerosol', 'Blue', 'Green', 'Red', 'Red Edge 1', 'Red Edge 2', 'Red Edge 3', 'NIR broad', 'NIR narrow', 'NIR water vapor', 'SWIR 1', 'SWIR 2'], # sceneid , 'Cirrus'
    'wavelength': [0.443, 0.49, 0.56, 0.665, 0.705, 0.74, 0.783, 0.842, 0.865, 0.945, 1.61, 2.19], # , 1.375
    'wavelength units': 'Micrometers',
    'fwhm': [0.01, 0.0325, 0.0175, 0.015, 0.0075, 0.0075, 0.01, 0.0575, 0.01, 0.01, 0.045, 0.09], # , 0.015
    'solar irradiance': [129, 128, 128, 108, 74.5, 68, 67, 103, 52.5, 9, 4, 1.5], # , 6
    'default bands': [12, 8, 2],
    'defaultbasefilename': '%s_ref.dat', # sceneid
    'data ignore value': 0 
    }) 

headerdict['Sentinel-2_10m'] = headerdict['default'].copy()
headerdict['Sentinel-2_10m'].update({
    'description': 'Sentinel-2 10m Band Surface Reflectance (%s)',  # sceneid
    'band names': ['Blue', 'Green', 'Red', 'NIR broad'], # sceneid , 'Cirrus'
    'wavelength': [0.49, 0.56, 0.665, 0.842], # , 1.375
    'wavelength units': 'Micrometers',
    'fwhm': [0.0325, 0.0175, 0.015, 0.0575], # , 0.015
    'solar irradiance': [128, 128, 108, 103], # , 6
    'default bands': [4, 3, 2],
    'defaultbasefilename': '%s_ref.dat', # sceneid
    'data ignore value': 0 
    }) 

headerdict['Sentinel-2_20m'] = headerdict['default'].copy()
headerdict['Sentinel-2_20m'].update({
    'description': 'Sentinel-2 20m Band Surface Reflectance (%s)',  # sceneid
    'band names': ['Red Edge 1', 'Red Edge 2', 'Red Edge 3', 'NIR narrow', 'SWIR 1', 'SWIR 2'], # sceneid , 'Cirrus'
    'wavelength': [0.705, 0.74, 0.783, 0.865, 1.61, 2.19], # , 1.375
    'wavelength units': 'Micrometers',
    'fwhm': [0.0075, 0.0075, 0.01, 0.01, 0.045, 0.09], # , 0.015
    'solar irradiance': [74.5, 68, 67, 52.5, 4, 1.5], # , 6
    'default bands': [3, 2, 1],
    'defaultbasefilename': '%s_ref.dat', # sceneid
    'data ignore value': 0 
    }) 

headerdict['Sentinel-2_60m'] = headerdict['default'].copy()
headerdict['Sentinel-2_60m'].update({
    'description': 'Sentinel-2 60m Band Surface Reflectance (%s)',  # sceneid
    'band names': ['Coastal aerosol', 'NIR water vapor'], # sceneid , 'Cirrus'
    'wavelength': [0.443, 0.945], # , 1.375
    'wavelength units': 'Micrometers',
    'fwhm': [0.01, 0.01], # , 0.015
    'solar irradiance': [129, 6], # , 6
    # 'default bands': [12, 8, 2],
    'defaultbasefilename': '%s_ref.dat', # sceneid
    'data ignore value': 0 
    }) 

headerdict['S2OLI'] = headerdict['default'].copy()
headerdict['S2OLI'].update({
    'description': 'Sentinel-2 OLI Equivalent Surface Reflectance (%s)',  # sceneid
    'band names': ['Coastal aerosol', 'Blue', 'Green', 'Red', 'NIR broad', 'SWIR 1', 'SWIR 2'], # sceneid , 'Cirrus'
    'wavelength': [0.443, 0.49, 0.56, 0.665, 0.842, 1.61, 2.19], # , 1.375
    'wavelength units': 'Micrometers',
    'fwhm': [0.01, 0.0325, 0.0175, 0.015, 0.0575, 0.045, 0.09], # , 0.015
    'solar irradiance': [129, 128, 128, 108, 103, 4, 1.5], # , 6
    'default bands': [7, 5, 2],
    'defaultbasefilename': '%s_ref.dat', # sceneid
    'data ignore value': 0 
    }) 

headerdict['S2TM'] = headerdict['default'].copy()
headerdict['S2TM'].update({
    'description': 'Sentinel-2 TM/ETM+ Equivalent Surface Reflectance (%s)',  # sceneid
    'band names': ['Blue', 'Green', 'Red', 'NIR broad', 'SWIR 1', 'SWIR 2'], # sceneid , 'Cirrus'
    'wavelength': [0.49, 0.56, 0.665, 0.842, 1.61, 2.19], # , 1.375
    'wavelength units': 'Micrometers',
    'fwhm': [0.0325, 0.0175, 0.015, 0.0575, 0.045, 0.09], # , 0.015
    'solar irradiance': [128, 128, 108, 103, 4, 1.5], # , 6
    'default bands': [6, 4, 1],
    'defaultbasefilename': '%s_ref.dat', # sceneid
    'data ignore value': 0 
    }) 

headerdict['NDVI'] = headerdict['default'].copy()
headerdict['NDVI'].update({
    'description': 'NDVI (%s)',  # sceneid
    'band names': ['NDVI'], # sceneid
    'defaultbasefilename': '%s_NDVI.dat', # sceneid
    'data ignore value': 0.0
    }) 

headerdict['NDTI'] = headerdict['default'].copy()
headerdict['NDTI'].update({
    'description': 'NDTI (%s)',  # sceneid
    'band names': ['NDTI'], # sceneid
    'defaultbasefilename': '%s_NDTI.dat', # sceneid
    'data ignore value': 0.0
    }) 

headerdict['NBR'] = headerdict['default'].copy()
headerdict['NBR'].update({
    'description': 'NBR (%s)',  # sceneid
    'band names': ['NBR'], # sceneid
    'defaultbasefilename': '%s_NBR.dat', # sceneid
    'data ignore value': 0.0
    }) 

headerdict['EVI'] = headerdict['default'].copy()
headerdict['EVI'].update({
    'description': 'EVI (%s)',  # sceneid
    'band names': ['EVI'], # sceneid
    'defaultbasefilename': '%s_EVI.dat', # sceneid
    'data ignore value': 0.0
    }) 
    
headerdict['Landsat'] = {'LE7': 'Landsat ETM+', 
                         'LT4': 'Landsat TM', 
                         'LT5': 'Landsat TM', 
                         'LM1': 'Landsat MSS', 
                         'LM2': 'Landsat MSS', 
                         'LM3': 'Landsat MSS', 
                         'LM4': 'Landsat MSS', 
                         'LM5': 'Landsat MSS', 
                         'LO8': 'Landsat OLI', 
                         'LT8': 'Landsat TIR', 
                         'LC8': {
                             'ref': 'Landsat OLI', 
                             'BT': 'Landsat TIR'
                             }, 
                         'LO9': 'Landsat OLI', 
                         'LT9': 'Landsat TIR', 
                         'LC9': {
                             'ref': 'Landsat OLI', 
                             'BT': 'Landsat TIR'
                             }
                         }


    
## General functions

def readenvihdr(hdr, *args, **kwargs):
    # started on 16 July 2019
    # this function will read data from an ENVI header into a local headerdict
    # includes code shamelessly borrowed from https://github.com/spectralpython/spectral/blob/master/spectral/io/envi.py
    rastertype = kwargs.get('rastertype', 'default')
    if not os.path.isfile(hdr):
        print('Error: the file {} does not exist.'.format(hdr))
        logerror(hdr, 'Error: HDR file does not exist.')
        return None
    else:
        if not rastertype in headerdict.keys():
            print('Error, rastertype "{}" is not in the recognised rastertypes of headerdict. Using default settings.'.format(rastertype))
            logerror(hdr, 'Error, rastertype "{}" is not in the recognised rastertypes of headerdict. Using default settings.'.format(rastertype))
            rastertype = 'default'
        hdict = headerdict[rastertype].copy()
        with open(hdr, 'r') as lines:
            for line in lines:
                line = line.strip()
                if line.find('=') == -1: continue
                if line[0] == ';': continue
    
                (key, sep, val) = line.partition('=')
                key = key.strip()
#                if not key.islower():
#                    have_nonlowercase_param = True
#                    if not support_nonlowercase_params:
#                        key = key.lower()
                val = val.strip()
                if val and val[0] == '{':
                    str = val.strip()
                    while str[-1] != '}':
                        line = lines.pop(0)
                        if line[0] == ';': continue
    
                        str += '\n' + line.strip()
                    if key == 'description':
                        hdict[key] = str.strip('{}').strip()
                    else:
                        vals = str[1:-1].split(',')
                        for j in range(len(vals)):
                            vals[j] = vals[j].strip()
                        hdict[key] = vals
                else:
                    hdict[key] = val
    return hdict

def isenvifile(f):
    # started on 16 July 2019
    # this function determines if a file is an ENVI file type based upon the existence of a .hdr file                  
    basename = os.path.basename(f)
    if '.' in basename:
        i = f.rfind('.')
        hdr = f.replace(f[i:], '.hdr')
    else:
        hdr = f + '.hdr'
    if os.path.isfile(hdr):
        return hdr
    else:
        return None

class ENVIfile(object):
    
    def __init__(self, data, rastertype, *args, **kwargs):
        subclasses = self._subclass_container()
        self.file = subclasses["file"]
        self.header = subclasses["header"]
        self.colorfile= subclasses["colorfile"]
        del subclasses
        
        # The variable 'data' can either be raster data or a string containing a file path. In the case of a file, it only prepares an ENVI file and possibly a colorfile.
        if sys.version_info.major == 2: # tests for cases where only a .hdr file and possibly a .clr need to be processed.
            headeronly = isinstance(data, basestring)
        else:
            headeronly = isinstance(data, str)
        
        self.outdir = kwargs.get('outdir', None)
        
        if headeronly:
            if not data.endswith('.hdr'):
                self.file.outfilename = data
                outdir, basename = os.path.split(data)
                if not self.outdir:
                    self.outdir = outdir
                if '.' in basename:
                    i = data.rfind('.')
                    data = '%s.hdr'%data[:i]
                else:
                    data = '%s.hdr'
            self.header.hdr = data
        else:
            self.file.data = data 
        
        # Determine proper raster and sensor types
        self.SceneID = kwargs.get('SceneID', None)
        self.ProductID = kwargs.get('ProductID', None)
        if not self.SceneID:
            if self.ProductID:
                self.SceneID = self.ProductID
        self.header.sensortype = kwargs.get('sensortype', None)
        self.header.sensortype = kwargs.get('sensor', None)
        if rastertype in ['ref', 'BT']:
            if self.SceneID[:1] == 'S':
                self.rastertype = 'Sentinel-2'
                self.header.sensortype = self.rastertype
            elif self.SceneID[:3] == 'LC8':
                self.rastertype = headerdict['Landsat']['LC8'][rastertype]
                if rastertype == 'BT':
                    self.header.sensortype = 'Landsat TIR'
                else: 
                    self.header.sensortype = 'Landsat OLI'
            elif self.SceneID[:3] == 'LC9':
                self.rastertype = headerdict['Landsat']['LC9'][rastertype]
                if rastertype == 'BT':
                    self.header.sensortype = 'Landsat TIR'
                else: 
                    self.header.sensortype = 'Landsat OLI'
            elif rastertype == 'BT':
                self.rastertype = 'Landsat Band6'
                if self.SceneID[2:3] == '7':
                    self.header.sensortype = 'Landsat ETM+'
                else:
                    self.header.sensortype = 'Landsat TM'
            elif self.SceneID[:1] == 'L':
                self.rastertype = headerdict['Landsat'][self.SceneID[:3]] 
                self.header.sensortype = headerdict['Landsat'][self.SceneID[:3]] 
        elif rastertype != 'Landsat' and rastertype in headerdict.keys():
            self.rastertype = rastertype
            if rastertype in ['Fmask','NDVI','EVI', 'pixel_qa']:
                if self.SceneID[:1] == 'S':
                    self.header.sensortype = 'Sentinel-2'
                elif self.SceneID[:1] == 'L':
                    if self.SceneID[2:3] in ['8', '9']:
                        self.header.sensortype = 'Landsat OLI'
                    else:
                        self.header.sensortype = headerdict['Landsat'][self.SceneID[:3]] 
        elif not rastertype:
            self.rastertype = 'default'
        else:
            self.rastertype = rastertype
        
        # Various data passed from other functions
        self.header.geoTrans = kwargs.get('geoTrans', None)
        self.header.acqtime = kwargs.get('acqtime', None)
        self.file.outfilename = kwargs.get('outfilename', None)
        self.header.headeroffset = 'header offset = 0\n'
        self.header.byteorder = 'byte order = 0\n'
        self.header.description = kwargs.get('description', None)
        self.header.bandnames = kwargs.get('bandnames', None)
        self.header.classes = kwargs.get('classes', None) 
        self.header.classnames = kwargs.get('classnames', None) # Not the same as classname
        self.header.classvalues = kwargs.get('classvalues', None)
        self.header.classlookup = kwargs.get('classlookup', None)
        self.header.classlookuptable = kwargs.get('classlookuptable', None)
        self.header.dict = kwargs.get('headerdict', None)
        
        
        # tags from other modules, may be incorporated later
        self.tilename = kwargs.get('tilename', None)
        self.header.classname = kwargs.get('classname', None) # Not the same as classnames
        self.year = kwargs.get('year', None)
        self.startyear = kwargs.get('startyear', None)
        self.endyear = kwargs.get('endyear', None)
        self.observationtype = kwargs.get('observationtype',None)
        self.header.dataignorevalue = kwargs.get('dataignorevalue',None)
        self.header.landsat = kwargs.get('landsat', None)
        self.header.defaultbands = kwargs.get('defaultbands', None)
        self.header.wavelength = kwargs.get('wavelength', None)
        self.header.fwhm = kwargs.get('fwhm', None)
        self.header.wavelengthunits = kwargs.get('wavelengthunits', None)
        self.header.solarirradiance = kwargs.get('solarirradiance', None)
        self.header.parentrasters = kwargs.get('parentrasters', None)
        
        self.mask = None # Functionality for this will be added in on a later date
        
        if not headeronly:
            self.header.interleave = 'interleave = bsq\n'
            self.header.gcsstring = 'coordinate system string = {' + prj.ExportToWkt() + '}\n'
            self.header.mapinfo = 'map info = {'
            projname = prj.GetAttrValue('projcs')
            self.header.mapinfo += '{}, 1, 1'.format(projname)
            self.header.mapinfo += ', {}, {}, {}, {}'.format(self.header.geoTrans[0], self.header.geoTrans[3], abs(self.header.geoTrans[1]), abs(self.header.geoTrans[5]))
            if ' UTM ' in projname:
                if projname[-1:] == 'N':
                    UTMnorth = 'North'
                else: 
                    UTMnorth = 'South'
                i = projname.rfind(' ') + 1
                UTMzone = projname[i:-1]
                self.header.mapinfo += ', {}, {}'.format(UTMzone, UTMnorth)
            unitval = prj.GetAttrValue('unit')
            if unitval.lower() == 'metre':
                unitval = 'Meters'
            self.header.mapinfo += ', {}, units={}'.format(prj.GetAttrValue('datum'), unitval)
            self.header.mapinfo += '}\n'
            self.header.projinfo = None
            self.file.datadims(self)
            self.getdictdata()
            self.header.hdr = self.file.outfilename.replace('.dat', '.hdr')
        else:
            self.header.readheader(self)
    
    def checkparentrasters(self, prdata): # this isn't currently implemented
        prtdata = prdata
        if isinstance(prdata, list):
            self.header.parentrasters = 'parent rasters = { '
            for x in prtdata:
                if prtdata.index(x) == 0:
                    self.header.parentrasters += x
                else:
                    self.header.parentrasters += ', {}'.format(x)
            self.header.parentrasters += ' }\n'
        elif prdata.startswith('parent rasters'):
            self.header.parentrasters = prdata
        else:
            self.header.parentrasters = 'parent rasters = {  }\n' # creates empty tag if improperly formatted data are sent
    
    def getdictdata(self):
        # print('rastertype = %s'%self.rastertype)
        # if self.rastertype in ['BT', 'ref'] and (self.SceneID.startswith('L') or self.landsat):
        #     if self.rastertype == 'BT':
        #         if int(self.landsat) == 8:
        #             rtype = 'Landsat TIR'
        #         else:
        #             rtype = 'Landsat Band6'
        #     elif self.SceneID.startswith('LC8') or self.SceneID.startswith('LO8'):
        #         rtype = 'Landsat OLI'
        #     else:
        #         rtype = headerdict['Landsat'][self.SceneID[:3]]
        #     print('headerdict: %s'%rtype)
        #     d = headerdict[rtype].copy()
        # else:
        #     print('headerdict: %s'%self.rastertype)
        ready = False
        if not self.header.dict:
            if self.rastertype in headerdict.keys():
                self.header.dict = headerdict[self.rastertype].copy()
            else:
                self.header.dict = headerdict['default'].copy()
        elif 'ready' in self.header.dict.keys():
            if self.header.dict['ready']:
                ready = self.header.dict['ready']
        
        if not self.header.description:
            if ready:
                self.header.description = 'description = { %s}\n'%self.header.dict['description']
            elif self.header.dict['description']:
                self.header.description = 'description = { %s}\n'%(self.header.dict['description']%(self.SceneID))
            else:
                self.header.description = 'description = { Raster data}\n'
        
        if not self.header.bandnames:
            if self.header.dict['band names']:
                bnames = ''
                for b in self.header.dict['band names']:
                    bnames += ', %s'%b
                self.header.bandnames = 'band names = {%s}\n'% bnames[1:]
            else:
                bnames = ''
                if self.header.bands > 0:
                    for i in range(self.header.bands):
                        bnames += ', Band %d'%(i+1)
                    self.header.bandnames = 'band names = { %s}\n'%bnames[1:]
        
        if not self.file.outfilename:
            if ready:
                self.file.outfilename = os.path.join(self.outdir,self.header.dict['defaultbasefilename'])
            # print(self.header.dict['defaultbasefilename'])
            else:
                self.file.outfilename = os.path.join(self.outdir,self.header.dict['defaultbasefilename']%self.SceneID)
        
        if not self.header.classes:
            if self.header.dict['classes']:
                self.header.classes = 'classes = %d\n'%self.header.dict['classes']
                outstr=''
                for x in self.header.dict['class names']:
                    outstr+=' %s,'%x
                self.header.classnames = 'class names = { %s}\n'%outstr[:-1]
                outstr=''
                for i in self.header.dict['class lookup']:
                    for j in i:
                        outstr+=' %d,'%j
                self.header.classlookup = 'class lookup = { %s}\n'%outstr[:-1]
            else:
                self.header.classes = self.header.classnames = self.header.classlookup = None
        
        if self.header.dict['wavelength']:
            wavelengths = ''
            for b in self.header.dict['wavelength']:
                wavelengths += ', %f'%b
            self.header.wavelength = 'wavelength = {%s}\n'% wavelengths[1:]
        else:
            self.header.wavelength = None
        
        if self.header.dict['fwhm']:
            fwhm = ''
            for b in self.header.dict['fwhm']:
                fwhm += ', %f'%b
            self.header.fwhm = 'fwhm = {%s}\n'% fwhm[1:]
        else:
            self.header.fwhm = None
        
        if self.header.dict['wavelength units']:
            wavelengths = ''
            self.header.wavelengthunits = 'wavelength units= {%s}\n'% self.header.dict['wavelength units']
        else:
            self.header.wavelengthunits = None
        
        if self.header.dict['solar irradiance']:
            solarirradiance = ''
            for b in self.header.dict['solar irradiance']:
                solarirradiance += ', %f'%b
            self.header.solarirradiance = 'solar irradiance = {%s}\n'% solarirradiance[1:]
        else:
            self.header.solarirradiance = None
        
        dataignore = None
        if not self.header.dataignorevalue and self.header.dict['data ignore value']:
            dataignore = self.header.dict['data ignore value']
        if dataignore:
            if self.header.datatypeval >= 4 and self.header.datatypeval <= 9:
                if isinstance(dataignore, str):
                    dataignore = float(dataignore)
                self.header.dataignorevalue = 'data ignore value = %f\n'%dataignore
            else:
                if isinstance(dataignore, str):
                    dataignore = int(dataignore)
                self.header.dataignorevalue = 'data ignore value = %d\n'%dataignore
        
        if self.header.defaultbands and not 'default bands = ' in self.header.defaultbands:
            x = None
            if isinstance(self.header.defaultbands, list):
                x = self.header.defaultbands
            elif self.header.dict['default bands']:
                x = self.header.dict['default bands']
            if x:
                dbands = ''
                for b in self.header.dict['default bands']:
                    dbands += ', %d'%b
                self.header.defaultbands = 'default bands = {%s}\n'% dbands[1:]
        elif self.header.defaultbands and self.header.defaultbands.startswith('default bands = ') and not self.header.defaultbands.endswith('\n'):
            self.header.defaultbands = '%s\n'%self.header.defaultbands
        
        if self.header.acqtime or self.year:
            if self.header.acqtime:
                if not 'acquisition time' in self.header.acqtime:
                    self.header.acquisitiontime = 'acquisition time = %s\n'%self.header.acqtime
                else:
                    self.header.acquisitiontime = self.header.acqtime
            else:
                self.header.acquisitiontime = 'acquisition time = %d-07-01\n'%self.year
        else: 
            self.header.acquisitiontime = None
        if not self.header.parentrasters: 
            if 'parentrasters' in self.header.dict.keys():
                if self.header.dict['parentrasters'].startswith('parent'):
                    self.header.parentrasters = self.header.dict['parentrasters']
                else:
                    self.header.parentrasters = 'parent rasters = { '
                    if isinstance(self.header.dict['parentrasters'], list):
                        for item in self.header.dict['parentrasters']:
                            if self.header.dict['parentrasters'].index(item) > 0:
                                self.header.parentrasters += ', '
                            self.header.parentrasters += item
                        self.header.parentrasters += ' }\n'
                    else:
                        self.header.parentrasters += ('{}'.format(self.header.dict['parentrasters']) + ' }\n')
        
        return 
        
    def Save(self):
        print('Writing raster to disk: %s'%self.file.outfilename)
        if len(self.file.data.shape) == 2:
            bufsize = self.file.data.shape[0] * self.file.data.shape[1] * self.file.data.dtype.itemsize
        else:
            bufsize = self.file.data.shape[1] * self.file.data.shape[2] * self.file.data.dtype.itemsize
        with open(self.file.outfilename, 'wb', bufsize) as fout:
            fout.write(self.file.data.tostring())
        self.WriteHeader()
        print('%s has been written to disk.'%os.path.basename(self.file.outfilename))
        self.file.data = None
    
    def WriteHeader(self):
        # Shamelessly adapted from http://pydoc.net/Python/spectral/0.17/spectral.io.envi/
        self.header.prepheader(self)
        if os.path.exists(self.header.hdr):
            now = datetime.datetime.now()
            bak = '%s.%s.bak'%(self.header.hdr, now.strftime('%Y%m%d_%H%M%S'))
            shutil.move(self.header.hdr,bak)
        with open(self.header.hdr,'w') as output:
            output.write(self.header.headerstr)
        if self.header.classes:
            self.colorfile.writeclr(self)
    
    def _subclass_container(self):
        _parent_class = self # Create access to parent class.
    
        class file:
            def __init__(self):
                self._parent_class = _parent_class # Easy access from self.
                # self.data = None
                # self.outfilename = None
            
            def datadims(self):
                dims = self.file.data.shape
                
                if len(dims) == 3:
                    self.header.bands = 'bands = %d\n'%dims[0]
                    self.header.lines = 'lines = %d\n'%dims[1]
                    self.header.samples = 'samples = %d\n'%dims[2]
                else:
                    self.header.lines = 'lines = %d\n'%dims[0]
                    self.header.samples = 'samples = %d\n'%dims[1]
                    self.header.bands = 'bands = 1\n'
                # print(self.data.dtype)
                self.header.datatypeval = int(dtype_to_envi[str(self.file.data.dtype)])
                self.header.datatype = 'data type = %d\n'%self.header.datatypeval
                return
   
        class header:
            def __init__(self):
                self._parent_class = _parent_class # Easy access from self.
                        
            def readheader(self):
                if not os.path.exists(self.header.hdr):
                    print("Error, header file missing: %s"%self.header.hdr)
                    
                    return
                with open(self.header.hdr,'r') as inhdrlines:
                    openline = False
                    for line in inhdrlines:
                        if not line.startswith('ENVI'):
                            if not openline:
                                i = line.find(' = ')
                                if i > 0:
                                    if line.startswith('interleave'):
                                        self.header.interleave = line
                                    elif line.startswith('lines'):
                                        self.header.lines = line
                                    elif line.startswith('samples'):
                                        self.header.samples = line
                                    elif line.startswith('bands'):
                                        self.header.bands = line
                                    elif line.startswith('data type'):
                                        self.header.datatype = line
                                        i = line.find('=') + 1
                                        self.header.datatypeval = int(line[i:])
                                    elif line.startswith('byte order'):
                                        self.header.byteorder = line
                                    elif line.startswith('header offset'):
                                        self.header.headeroffset = line
                                    elif line.startswith('map info'):
                                        self.header.mapinfo = line
                                    elif line.startswith('projection info'):
                                        self.header.projinfo = line
                                    elif line.startswith('coordinate system string'):
                                        self.header.gcsstring = line
                                    elif line.startswith('acquisition time'):
                                        self.header.acquisitiontime = line
                            
                self.getdictdata()
                
                return 
            
            def prepheader(self):
                self.header.headerstr = 'ENVI\n'
                for x in [self.header.description, self.header.samples, self.header.lines, self.header.bands, self.header.datatype, self.header.interleave]:
                    self.header.headerstr += x
                if self.header.classes:
                    self.header.headerstr += 'file type = ENVI Classification\n'
                elif self.mask:
                    self.header.headerstr += 'file type = ENVI Mask\n'
                else:
                    self.header.headerstr += 'file type = ENVI Standard\n'
                for x in [self.header.headeroffset, self.header.byteorder, self.header.mapinfo, self.header.projinfo, self.header.gcsstring, self.header.bandnames]:
                    if x:
                        self.header.headerstr += x
                if self.header.dataignorevalue:
                    self.header.headerstr += self.header.dataignorevalue
                if self.header.classes:
                    for x in [self.header.classes, self.header.classnames, self.header.classlookup]:
                        self.header.headerstr += x
                if self.header.wavelength:
                    self.header.headerstr += self.header.wavelength
                if self.header.fwhm:
                    self.header.headerstr += self.header.fwhm
                if self.header.wavelengthunits:
                    self.header.headerstr += self.header.wavelengthunits       
                if self.header.solarirradiance:
                    self.header.headerstr += self.header.solarirradiance
                if self.header.defaultbands:
                    self.header.headerstr += self.header.defaultbands
                if self.header.sensortype:
                    self.header.headerstr += 'sensor type = %s\n'%self.header.sensortype
                if self.header.acquisitiontime:
                    self.header.headerstr += self.header.acquisitiontime
                if self.header.parentrasters:
                    if isinstance(self.header.parentrasters, list):
                        if len(self.header.parentrasters) > 0:
                            parentrasterstr = 'parent rasters = { '
                            for item in self.header.parentrasters:
                                if self.header.parentrasters.index(item) > 0:
                                    parentrasterstr += ', '
                                parentrasterstr += item
                                parentrasterstr += ' }\n'
                        else:
                            parentrasterstr = ''
                        self.header.parentrasters = parentrasterstr
                        
                    self.header.headerstr += self.header.parentrasters
                return 
                
                        
        class colorfile:
            def __init__(self):
                self._parent_class = _parent_class # Easy access from self.
                
            def writeclr(self): # creates a colorfile
                if self.file.outfilename:
                    clr = self.file.outfilename.replace('.dat','.clr')
                else:
                    clr = self.header.hdr.replace('.hdr','.clr')
                if not 'class values' in self.header.dict.keys():
                    self.header.dict['class values'] = list(range(len(self.header.dict['class lookup'])))
                elif not self.header.dict['class values']:
                    self.header.dict['class values'] = list(range(len(self.header.dict['class lookup'])))
                with open(clr,'w') as output:
                    for i, rgb in zip(self.header.dict['class values'], self.header.dict['class lookup']):
                        output.write('%d %d %d %d\n'%(i,rgb[0],rgb[1],rgb[2]))
        
        return {"file": file, "header": header, "colorfile": colorfile} 