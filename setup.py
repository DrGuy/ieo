#!/usr/bin/env python
import os, sys
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

ini = os.path.join(configdir, 'ieo.ini')
if not os.path.isfile(ini):
    with open(ini, 'w') as output:
        output.write('[DEFAULT]\n')
        w = input('Please input the base directory for all imagery data (Landsat, Sentinel-2, etc.): ')
        newinidir(w)
        x = input('Please input the base directory for Landsat imagery data (includes Fmask, SR, BT, NDVI, EVI subdirectories, will use {} if not set): '.format(os.path.join(w, 'Landsat')))
        if len(x) == 0:
            x = os.path.join(w, 'Landsat')
        newinidir(x)
        for y in ['Fmask', 'SR', 'BT', 'NDVI', 'EVI']:
            dirname = os.path.join(x, y)
            newinidir(dirname)
            output.write('%sdir = %s\n'%(y.lowercase(), dirname))
        y = input('Please input the data catalog directory (will use %s if not set): '%os.path.join(w, 'catalog'))
        if len(y) == 0:
            y = os.path.join(x, 'catalog')
        newinidir(y)
        output.write('catdir = %s\n'%y)
        y = input('Please input the data ingest directory (will use %s if not set): '%os.path.join(x, 'ingest'))
        if len(y) == 0 or not os.path.isdir(y):
            y = os.path.join(x, 'ingest')
        newinidir(y)
        output.write('ingestdir = %s\n'%y)
        y = input('Please input the post-processing tar.gz archive directory (will use %s if not set): '%os.path.join(w, 'archive'))
        if len(y) == 0 or not os.path.isdir(y):
            y = os.path.join(w, 'archive')
        newinidir(y)
        output.write('archdir = %s\n'%y)
        y = input('Please input the log directory (will use %s if not set): '%os.path.join(w, 'logs'))
        if len(y) == 0 or not os.path.isdir(y):
            y = os.path.join(w, 'logs')
        newinidir(y)
        output.write('logdir = %s\n'%y)
    
setup(
    # Application name:
    name='ieo',

    # Version number:
    version='1.0.6',

    # Application author details:
    author='Guy Serbin',

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
        'Topic :: Scientific/Engineering :: GIS'
    ],

    # Scripts
    # Moves the script to the user's bin directory so that it can be executed.
    # Usage is 'download_espa_order.py' not 'python download_espa_order.py'
    scripts = ['ieo.py', 'modistools.py','ENVIfile.py'],
    include_package_data = True,
    
    packages = find_packages(include = ['.', 'config', 'data'], exclude = ['__pycache__', 'build', 'dist', 'ieo.egg-info']),
    package_data = {'config': ['*',], 'data': ['*',], 'data/ieo.gdb': ['*',]},
    # Dependent packages (distributions)
    install_requires=[
        'numexpr',
        'numpy',
        'gdal'
    ],
)
