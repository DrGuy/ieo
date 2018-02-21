#/usr/bin/python
import os, sys, shutil, datetime
from osgeo import osr



## Global variables

# Spatial variables

prj = osr.SpatialReference()
prj.SetProjection("EPSG:2157")

# Directory and file paths


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

headerdict['Landsat Band6'] = headerdict['default'].copy()
headerdict['Landsat Band6'].update({
    'description': 'LEDAPS Brightness Temperature (%s)',  # sceneid
    'band names': ['TIR'], # sceneid
    'wavelength': [11.450000],
    'wavelength units': 'Micrometers',
    'fwhm':[2.100000],
    'defaultbasefilename': '%s_BT.dat' # sceneid
    }) 

headerdict['Landsat TIR'] = headerdict['default'].copy()
headerdict['Landsat TIR'].update({
    'description': 'LEDAPS Brightness Temperature (%s)',  # sceneid
    'band names': ['TIR 1', 'TIR 2'], # sceneid
    'wavelength': [10.895000, 12.005000],
    'wavelength units': 'Micrometers',
    'fwhm': [0.590000, 1.010000],
    'defaultbasefilename': '%s_BT.dat' # sceneid
    }) 

headerdict['Landsat TM'] = headerdict['default'].copy()
headerdict['Landsat TM'].update({
    'description': 'LEDAPS Surface Reflectance (%s)',  # sceneid
    'band names': ['Blue', 'Green', 'Red', 'NIR', 'SWIR 1', 'SWIR 2'], # sceneid
    'wavelength': [0.485000, 0.560000, 0.662000, 0.830000, 1.648000, 2.215000],
    'wavelength units': 'Micrometers',
    'fwhm': [0.070000, 0.080000, 0.060000, 0.130000, 0.200000, 0.270000],
    'default bands': [6, 4, 1],
    'defaultbasefilename': '%s_ref.dat' # sceneid
    }) 

headerdict['Landsat ETM+'] = headerdict['default'].copy()
headerdict['Landsat ETM+'].update({
    'description': 'LEDAPS Surface Reflectance (%s)',  # sceneid
    'band names': ['Blue', 'Green', 'Red', 'NIR', 'SWIR 1', 'SWIR 2'], # sceneid
    'wavelength': [0.483000, 0.560000, 0.662000, 0.835000, 1.648000, 2.206000],
    'wavelength units': 'Micrometers',
    'fwhm': [0.070000, 0.080000, 0.060000, 0.120000, 0.200000, 0.260000],
    'default bands': [6, 4, 1],
    'defaultbasefilename': '%s_ref.dat' # sceneid
    }) 

headerdict['Landsat OLI'] = headerdict['default'].copy()
headerdict['Landsat OLI'].update({
    'description': 'LEDAPS Surface Reflectance (%s)',  # sceneid
    'band names': ['Coastal aerosol', 'Blue', 'Green', 'Red', 'NIR', 'SWIR 1', 'SWIR 2'], # sceneid
    'wavelength': [0.443000, 0.482600, 0.561300, 0.654600, 0.864600, 1.609000, 2.201000],
    'wavelength units': 'Micrometers',
    'default bands': [7, 5, 2],
    'fwhm': [0.016000, 0.060100, 0.057400, 0.037500, 0.028200, 0.084700, 0.186700],
    'defaultbasefilename': '%s_ref.dat' # sceneid
    }) 
    
headerdict['Landsat MSS'] = headerdict['default'].copy()
headerdict['Landsat MSS'].update({
    'description': 'LEDAPS Surface Reflectance (%s)',  # sceneid
    'band names': ['Green', 'Red', 'Red Edge', 'NIR'], # sceneid
    'wavelength': [0.55, 0.65, 0.75, 0.95],
    'wavelength units': 'Micrometers',
    'fwhm': [0.0500, 0.0500, 0.0500, 0.1500],
    'default bands': [4, 2, 1],
    'defaultbasefilename': '%s_ref.dat' # sceneid
    }) 

headerdict['Sentinel-2'] = headerdict['default'].copy()
headerdict['Sentinel-2'].update({
    'description': 'LEDAPS Surface Reflectance (%s)',  # sceneid
    'band names': ['Coastal aerosol', 'Blue', 'Green', 'Red', 'Red Edge 1', 'Red Edge 2', 'Red Edge 3', 'NIR broad', 'NIR narrow', 'NIR water vapor', 'Cirrus', 'SWIR 1', 'SWIR 2'], # sceneid
    'wavelength': [0.443, 0.49, 0.56, 0.665, 0.705, 0.74, 0.783, 0.842, 0.865, 0.945, 1.375, 1.61, 2.19],
    'wavelength units': 'Micrometers',
    'fwhm': [0.01, 0.0325, 0.0175, 0.015, 0.0075, 0.0075, 0.01, 0.0575, 0.01, 0.01, 0.015, 0.045, 0.09],
    'solar irradiance': [129, 128, 128, 108, 74.5, 68, 67, 103, 52.5, 9, 6, 4, 1.5],
    'default bands': [13, 8, 2],
    'defaultbasefilename': '%s_ref.dat' # sceneid
    }) 

headerdict['NDVI'] = headerdict['default'].copy()
headerdict['NDVI'].update({
    'description': 'NDVI (%s)',  # sceneid
    'band names': ['NDVI'], # sceneid
    'defaultbasefilename': '%s_NDVI.dat', # sceneid
    'data ignore value': 0.0
    }) 

headerdict['EVI'] = headerdict['default'].copy()
headerdict['EVI'].update({
    'description': 'EVI (%s)',  # sceneid
    'band names': ['EVI'], # sceneid
    'defaultbasefilename': '%s_EVI.dat', # sceneid
    'data ignore value': 0.0
    }) 
    
headerdict['Landsat'] = {'LE7': 'Landsat ETM+', 'LT4': 'Landsat TM', 'LT5': 'Landsat TM', 'LM1': 'Landsat MSS', 'LM2': 'Landsat MSS', 'LM3': 'Landsat MSS', 'LM4': 'Landsat MSS', 'LM5': 'Landsat MSS', 'LO8': 'Landsat OLI', 'LT8': 'Landsat TIR', 'LC8': {'ref': 'Landsat OLI', 'BT': 'Landsat TIR'}}


    
## General functions

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
            if rastertype in ['Fmask','NDVI','EVI']:
                if self.SceneID[:1] == 'S':
                    self.header.sensortype = 'Sentinel-2'
                elif self.SceneID[:1] == 'L':
                    if self.SceneID[2:3] == '8':
                        self.header.sensortype = 'Landsat OLI'
                    else:
                        self.header.sensortype = headerdict['Landsat'][self.SceneID[:3]] 
        elif not rastertype:
            self.rastertype = 'default'
        else:
            self.rastertype = rastertype
        
        # Various data passed from other functions
        self.header.geoTrans = kwargs.get('geoTrans', None)
        self.header.acqtime = kwargs.get('acqtime',None)
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
            self.header.projinfo = 'projection info = {3, 6378137.0, 6356752.3, 53.500000, -8.000000, 600000.0, 750000.0, 0.999820, D_IRENET95, IRENET95_Irish_Transverse_Mercator, units=Meters}\n'
            self.header.mapinfo = 'map info = {IRENET95_Irish_Transverse_Mercator, 1.0000, 1.0000, %d, %d, 3.0000000000e+001, 3.0000000000e+001, D_IRENET95, units=Meters}\n'%(self.header.geoTrans[0],self.header.geoTrans[3])
            self.header.gcsstring = 'coordinate system string = {PROJCS["IRENET95_Irish_Transverse_Mercator",GEOGCS["GCS_IRENET95",DATUM["D_IRENET95",SPHEROID["GRS_1980",6378137.0,298.257222101]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Transverse_Mercator"],PARAMETER["False_Easting",600000.0],PARAMETER["False_Northing",750000.0],PARAMETER["Central_Meridian",-8.0],PARAMETER["Scale_Factor",0.99982],PARAMETER["Latitude_Of_Origin",53.5],UNIT["Meter",1.0]]}\n'
            self.file.datadims(self)
            self.getdictdata()
            self.header.hdr = self.file.outfilename.replace('.dat','.hdr')
        else:
            self.header.readheader(self)
        
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
                ready = True
        
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
                self.header.dataignorevalue = 'data ignore value = %f\n'%dataignore
            else:
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
                self.header.parentrasters = self.header.dict['parentrasters']
        
        return 
        
    def Save(self):
        print('Writing raster to disk: %s'%self.file.outfilename)
        bufsize = self.file.data.shape[0] * self.file.data.shape[1] * self.file.data.dtype.itemsize
        with open(self.file.outfilename, 'wb', bufsize) as fout:
            fout.write(self.file.data.tostring())
        self.WriteHeader()
        print('%s has been written to disk.'%os.path.basename(self.file.outfilename))
        self.file.data = None
    
    def WriteHeader(self):
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
                self.header.lines = 'lines = %d\n'%dims[0]
                self.header.samples = 'samples = %d\n'%dims[1]
                if len(dims) == 3:
                    self.header.bands = 'bands = %d\n'%dims[2]
                else:
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
                                j = i + 3
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