#!/usr/bin/env python
import os, sys, shutil, glob
from setuptools import setup, find_packages

# configuration data
if sys.version_info[0] == 2:
    import ConfigParser as configparser
else:
    import configparser




def newinidir(dirname):
    # this function creates new directories on disk if they are missing
    if not os.path.isdir(dirname):
        print('Creating: {}'.format(dirname))
        os.mkdir(dirname)

# Access configuration data in installation directory


# create configuration for installation if missing

configdir = os.path.join(os.path.dirname(__file__), 'config')
if not os.path.isdir(configdir):
    os.mkdir(configdir)

sampleconfig = os.path.join(configdir, 'sample_ieo.ini')

# Check to see if ieo.ini exists, and if so, load file data
updatedini = False
ini = os.path.join(configdir, 'ieo.ini')
inilines = []
if os.path.isfile(ini):
    ieo_config = configparser.ConfigParser()
    ieo_config.read(ini) # config_path
    if 'Projection' in ieo_config.keys():
        updatedini = True

# checks to see if sample_ieo.ini exists, and if so, load file data
samplelines = []
sconfig = False
if os.path.isfile(sampleconfig):
    sconfig = True
    sieo_config = configparser.ConfigParser()
    sieo_config.read(ini) # config_path

# check to see if either sample_ieo.ini or ieo.ini are up to date
scb = False # Boolean flag, if set to True, copy sample_ieo.ini to ieo.ini
sc = '' 
if updatedini:
    sc = input('An updated ieo.ini was found. Do you wish to use this file as is (DEFAULT = "N")? (y/N): ')
    if sc.lower() == 'y' or sc.lower == 'yes':
        scb = True
if sconfig and not scb:
    sc = input('Have you edited sample_ieo.ini to suit your archive configuration? (DEFAULT = "N", answering "Y" will save these data to ieo.ini (y/N): ')
    if sc.lower() == 'y' or sc.lower == 'yes':
        ieo_config = sieo_config
        if os.path.isfile(ini):
            import datetime
            now = datetime.datetime.now()
            bak = ini.replace('.ini', '.{}.bak'.format(now.strftime('%Y%m%d-%H%M%S')))
            print('Backing up ieo.ini to: {}'.format(bak))
            shutil.move(ini, bak)
            print('Copying sample_ieo.ini to ieo.ini.')
            shutil.copy(sampleconfig, ini)
        scb = True


lcatdir = os.path.join(os.path.dirname(__file__), 'catalog') 
ldatadir = os.path.join(os.path.dirname(__file__), 'data')
lgdb = os.path.join(ldatadir, 'ieo.gdb')

if not scb: # build a new config file object
    ieo_config = configparser.ConfigParser()
    print('Now creating a new ieo.ini with custom input.')
    
    # DEFAULT section
    ieo_config['DEFAULT'] = {}
    basedir = input('Please input the base directory for all imagery data (Landsat, Sentinel-2, etc.): ')
    landsatdir = input('Please input the base directory for Landsat imagery data (includes Fmask, SR, BT, NDVI, EVI subdirectories, will use {} if not set): '.format(os.path.join(basedir, 'Landsat')))
    if len(basedir) == 0:
        landsatdir = os.path.join(basedir, 'Landsat')
    for y in ['Fmask', 'SR', 'BT', 'NDVI', 'EVI']:
        dirname = os.path.join(landsatdir, y)
        ieo_config['DEFAULT'][y] = dirname 
    y = input('Please input the Landsat data ingest directory (will use %s if not set): '%os.path.join(landsatdir, 'Ingest'))
    if len(y) == 0:
        y = os.path.join(landsatdir, 'Ingest')
    ieo_config['DEFAULT']['ingestdir'] = y
    archdir = input('Please input the post-processing tar.gz archive directory (will use %s if not set): '%os.path.join(basedir, 'archive'))
    if len(archdir) == 0:
        archdir = os.path.join(basedir, 'archive')
    ieo_config['DEFAULT']['archdir'] = archdir
    logdir = input('Please input the log directory (will use %s if not set): '%os.path.join(basedir, 'logs'))
    if len(logdir) == 0:
        logdir = os.path.join(basedir, 'logs')
    ieo_config['DEFAULT']['logdir'] = logdir
    catdir = input('Please input the data catalog directory (will use %s if not set): '%os.path.join(basedir, 'Catalog'))
    if len(catdir) == 0:
        catdir = os.path.join(basedir, 'Catalog')
        ieo_config['DEFAULT']['catdir'] = catdir
    
    ieo_config['DEFAULT']['GDBname'] = 'ieo.gdb'
    
    # right now these are filled wil IEO defaults. I will write a customisable installer for the upcoming sections later
    # VECTOR section
    ieo_config['VECTOR'] = {}
    y = input('Please input the base filename for the Landsat scene catalogue shapefile (will use WRS2_Ireland_scenes.shp if not set): ')
    if len(y) == 0:
        y = 'WRS2_Ireland_scenes.shp'
    ieo_config['VECTOR']['landsatshp'] = y
    y = input('Please input the layer name for the generic Landsat WRS-1 scene polygons (will use Ireland_WRS1_Landsat_1_3_ITM if not set): ')
    if len(y) == 0:
        y = 'Ireland_WRS1_Landsat_1_3_ITM'
    ieo_config['VECTOR']['WRS1'] = 'Ireland_WRS2_Landsat_4_8_ITM'
    y = input('Please input the layer name for the generic Landsat WRS-2 scene polygons (will use Ireland_WRS2_Landsat_4_8_ITM if not set): ')
    if len(y) == 0:
        y = 'Ireland_WRS2_Landsat_4_8_ITM'
    ieo_config['VECTOR']['WRS2'] = y
    y = input('Please input the layer name for the generic Sentinel-2 scene scene polygons (will use Ireland_Sentinel2_tiles_ITM if not set): ')
    if len(y) == 0:
        y = 'Ireland_Sentinel2_tiles_ITM'
    ieo_config['VECTOR']['Sen2tiles'] = y
    y = input('Please input the layer name for the local tile system grid (will use AIRT if not set): ')
    if len(y) == 0:
        y = 'AIRT'
    ieo_config['VECTOR']['nationaltilesystem'] = 'AIRT'
    
    # Projection section
    ieo_config['Projection'] = {}
    y = input('Please input the projection code, including "EPSG:" (will use "EPSG:2157" if not set): ')
    if len(y) == 0:
        y = 'EPSG:2157'
    ieo_config['Projection']['proj'] = y
    y = input('Please input the projection acronym (will use "ITM" if not set): ')
    if len(y) == 0:
        y = 'EPSG:2157'
    ieo_config['Projection']['projacronym'] = y

    # makegrid section
    ieo_config['makegrid'] = {}
    y = input('Please input the minX value for the local tile system grid (will use 418500.0 if not set): ')
    if len(y) == 0:
        y = '418500.0'
    ieo_config['makegrid']['minX'] = y
    y = input('Please input the minY value for the local tile system grid (will use 519000.0 if not set): ')
    if len(y) == 0:
        y = '519000.0'
    ieo_config['makegrid']['minY'] = y
    y = input('Please input the maxX value for the local tile system grid (will use 769500.0 if not set): ')
    if len(y) == 0:
        y = '769500.0'
    ieo_config['makegrid']['maxX'] = y
    y = input('Please input the maxY value for the local tile system grid (will use 969000.0 if not set): ')
    if len(y) == 0:
        y = '969000.0'
    ieo_config['makegrid']['maxY'] = y
    y = input('Please input the number of xtiles for the local tile system grid (will use 12 if not set): ')
    if len(y) == 0:
        y = '12'
    ieo_config['makegrid']['xtiles'] = y
    y = input('Please input the number of ytiles for the local tile system grid (will use 15 if not set): ')
    if len(y) == 0:
        y = '15' 
    ieo_config['makegrid']['ytiles'] = y
    ieo_config.write(ini)
else:
    basedir = os.path.dirname(ieo_config['DEFAULT']['catdir'])
    landsatdir = os.path.join(basedir, 'Landsat')


landsatcatdir = os.path.join(ieo_config['DEFAULT']['catdir'], 'Landsat')
basebasedir = os.path.dirname(basedir)

# Create missing directories on disk 
for d in [basebasedir, basedir, landsatdir, ieo_config['DEFAULT']['catdir'], landsatcatdir]:
    newinidir(d)
for key in ieo_config['DEFAULT'].keys(): # this actually processes some values for the second time, but does nothing if the directories exist
    newinidir(ieo_config['DEFAULT'][key])

# copy data files to Catalogue directory
badlistfile = os.path.join(landsatcatdir, 'badlist.txt')
cpb = False # copy badlist.txt to catdir
if not os.path.isfile(badlistfile):
    cpb = True
else:
    ans = input('Bad date text file {} exists. Overwrite? (y/N): '.format(badlistfile))
    if ans.lower() == 'y' or ans.lower() == 'yes':
        cpb = True
if cpb:
    shutil.copy(os.path.join(lcatdir, 'badlist.txt'), badlistfile) # copy over a file of dates with known geometric errors
cpb = False # copy ieo.gdb to catdir
gdbdir = os.path.join(ieo_config['DEFAULT']['catdir'], 'ieo.gdb')
if not os.path.isdir(gdbdir):
    cpb = True
    os.mkdir(gdbdir)
else:
    ans = input('GDB {} exists. Overwrite? (y/N): '.format(gdbdir))
    if ans.lower() == 'y' or ans.lower() == 'yes':
        cpb = True
if cpb:
    lflist = glob.glob(os.path.join(ldatadir, '*.*'))
    gflist = glob.glob(os.path.join(gdbdir, '*.*'))
    if len(gflist) > 0:
        print('Deleting old GDB files.')
        for f in gflist:
            os.remove(f)
        print('Copying GDB.')
        for f in lflist:
            shutil.copy(f, gdbdir)                
    
setup(
    # Application name:
    name='ieo',

    # Version number:
    version='1.1.0',

    # Application author details:
    author='Guy Serbin',

    license = open('LICENSE').read(),

    description = 'Irish Earth Observation library.',
    long_description = open('README.md').read(),
    # license=open('LICENSE.txt').read(),

    #   description='General image processing routines.',
    # long_description=open('README.md').read(),

    classifiers = [
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.1',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Topic :: Scientific/Engineering :: GIS'
    ],

    # Scripts
    # Moves the script to the user's bin directory so that it can be executed.
    # Usage is 'download_espa_order.py' not 'python download_espa_order.py'
    scripts = ['ieo.py', 'ENVIfile.py'],
#    include_package_data = True,
    
#    packages = find_packages(include = ['config', 'data']),
    packages = ['config', 'data'],
    package_data={'config': ['*',], 'data': ['*',]},
    #package_data = {'config': ['*',],}, #  'data': ['*',], 'data/ieo.gdb': ['*',]
    # Dependent packages (distributions)
    install_requires=[
        'numexpr',
        'numpy',
        'gdal',
        'pillow'
    ],
)
