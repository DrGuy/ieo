#!/usr/bin/env python
import os, sys, shutil, glob
from setuptools import setup, find_packages

def newinidir(dirname):
    # this function creates new directories on disk if they are missing
    if not os.path.isdir(dirname):
        print('Creating: {}'.format(dirname))
        os.mkdir(dirname)

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
    with open(ini, 'r') as lines:
        for line in lines:
            inilines.append(line)
            if '[Projection]' in line:
                updatedini = True

# checks to see if sample_ieo.ini exists, and if so, load file data
samplelines = []
sconfig = False
if os.path.isfile(sampleconfig):
    sconfig = True
    with open(sampleconfig, 'r') as lines:
        for line in lines:
            samplelines.append(line)

# check to see if either sample_ieo.ini or ieo.ini are up to date
scb = False # Boolean flag, if set to True, copy sample_ieo.ini to ieo.ini
sc = '' 
dirdict = {} # dict containing dirname keys and file paths
if updatedini:
    sc = input('An updated ieo.ini was found. Do you wish to use this file as is (DEFAULT = "N")? (y/N): ')
    if sc.lower() == 'y' or sc.lower == 'yes':
        for line in samplelines:
            if 'dir = ' in line:
                i = line.find('=') + 2
                h = line.find(' ')                
                dirdict[line[:h]] = line[i:].rstrip('\n')
                if line[:h] == 'catdir':
                    dirdict['landsatcatdir'] = os.path.join(line[:h], 'Landsat')
                    dirdict['basedir'] = os.path.dirname(line[:h])
                elif line[:h] == 'fmaskdir':
                    dirdict['landsatdir'] = os.path.dirname(line[i:].rstrip('\n'))
        scb = True
if sconfig and not scb:
    sc = input('Have you edited sample_ieo.ini to suit your archive configuration? (DEFAULT = "N", answering "Y" will save these data to ieo.ini (y/N): ')
    if sc.lower() == 'y' or sc.lower == 'yes':
        with open(ini, 'w') as output:
            for line in samplelines:
                if 'dir = ' in line:
                    i = line.find('=') + 2
                    h = line.find(' ')                
                    dirdict[line[:h]] = line[i:].rstrip('\n')
                    if line[:h] == 'catdir':
                        dirdict['landsatcatdir'] = os.path.join(line[:h], 'Landsat')
                        dirdict['basedir'] = os.path.dirname(line[:h])
                    elif line[:h] == 'fmaskdir':
                        dirdict['landsatdir'] = os.path.dirname(line[i:].rstrip('\n'))
                output.write(line)
        scb = True

lcatdir = os.path.join(os.path.dirname(__file__), 'catalog') 
ldatadir = os.path.join(os.path.dirname(__file__), 'data')
lgdb = os.path.join(ldatadir, 'ieo.gdb')

if not scb:
    print('Now creating a new ieo.ini with custom input.')
    with open(ini, 'w') as output:
        # DEFAULT section
        output.write('[DEFAULT]\n')
        w = input('Please input the base directory for all imagery data (Landsat, Sentinel-2, etc.): ')
        dirdict['basedir'] = w
        x = input('Please input the base directory for Landsat imagery data (includes Fmask, SR, BT, NDVI, EVI subdirectories, will use {} if not set): '.format(os.path.join(w, 'Landsat')))
        if len(x) == 0:
            x = os.path.join(w, 'Landsat')
        dirdict['landsatdir'] = x
        for y in ['Fmask', 'SR', 'BT', 'NDVI', 'EVI']:
            dirname = os.path.join(x, y)
            output.write('%sdir = %s\n'%(y.lower(), dirname))
            dirdict['{}dir'.format(y.lower())] = dirname 
        y = input('Please input the data ingest directory (will use %s if not set): '%os.path.join(x, 'ingest'))
        if len(y) == 0:
            y = os.path.join(x, 'ingest')
        output.write('ingestdir = %s\n'%y)
        dirdict['ingestdir'] = y
        archdir = input('Please input the post-processing tar.gz archive directory (will use %s if not set): '%os.path.join(w, 'archive'))
        if len(archdir) == 0:
            archdir = os.path.join(w, 'archive')
        
        output.write('archdir = %s\n'%archdir)
        dirdict['archdir'] = archdir
        logdir = input('Please input the log directory (will use %s if not set): '%os.path.join(w, 'logs'))
        if len(logdir) == 0:
            logdir = os.path.join(w, 'logs')
        dirdict['logdir'] = logdir
        output.write('logdir = %s\n'%logdir)
        catdir = input('Please input the data catalog directory (will use %s if not set): '%os.path.join(w, 'catalog'))
        if len(catdir) == 0:
            catdir = os.path.join(x, 'Catalog')
            dirdict['catdir'] = catdir
            landsatcatdir = os.path.join(catdir, 'Landsat')
            dirdict['landsatcatdir'] = landsatcatdir
            tiledir = os.path.join(x, 'tiles')
        output.write('catdir = %s\n'%catdir)
        output.write('GDBname = ieo.gdb\n')
        
        # right now these are filled wil IEO defaults. I will write a customisable installer for the upcoming sections later
        # VECTOR section
        output.write('\n[VECTOR]\n')
        output.write('\n# Important note: only the shapefile or layer base names, not\n')
        output.write('\n# absolute file paths, are stored here. The GDB is stored separately.\n')
        output.write('landsatshp = WRS2_Ireland_scenes.shp\n')
        output.write('WRS1 = Ireland_WRS1_Landsat_1_3_ITM\n')
        output.write('WRS2 = Ireland_WRS2_Landsat_4_8_ITM\n')
        output.write('Sen2tiles = Ireland_Sentinel2_tiles_ITM\n')
        output.write('nationaltilesystem = AIRT\n')
        
        # Projection section
        output.write('\n[Projection]\n')
        output.write('# projacronym should contain only characters allowed in filenames, and no spaces\n')
        output.write('proj = EPSG:2157\n')
        output.write('projacronym = ITM\n')

        # makegrid section
        output.write('\n[makegrid]\n')
        output.write('minX = 418500.0\n')
        output.write('minY = 519000.0\n')
        output.write('maxX = 769500.0\n')
        output.write('maxY = 969000.0\n')
        output.write('xtiles = 12\n')
        output.write('ytiles = 15\n')

# Create missing directories on disk 
for d in [os.path.dirname(dirdict['basedir']), dirdict['basedir'], dirdict['landsatdir'], dirdict['catdir']]:
    newinidir(d)
for key in dirdict.keys(): # this actually processes some values for the second time, but does nothing if the directories exist
    newinidir(dirdict[key])

# copy data files to Catalogue directory
badlistfile = os.path.join(dirdict['landsatcatdir'], 'badlist.txt')
cpb = False # copy badlist.txt to catdir
if not os.path.isfile(badlistfile):
    cpb = True
else:
    ans = input('Bad date text file {} exists. Overwrite? (y/N): '.format(badlistfile))
    if ans.lowercase() == 'y' or ans.lowercase() == 'yes':
        cpb = True
if cpb:
    shutil.copy(os.path.join(lcatdir, 'badlist.txt'), badlistfile) # copy over a file of dates with known geometric errors
cpb = False # copy ieo.gdb to catdir
gdbdir = os.path.join(dirdict['catdir'], 'ieo.gdb')
if not os.path.isdir(gdbdir):
    cpb = True
    os.mkdir(gdbdir)
else:
    ans = input('GDB {} exists. Overwrite? (y/N): '.format(badlistfile))
    if ans.lowercase() == 'y' or ans.lowercase() == 'yes':
        cpb = True
if cpb:
    lflist = glob.glob(ldatadir, '*.*')
    gflist = glob.glob(gdbdir, '*.*')
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
        'PIL'
    ],
)
