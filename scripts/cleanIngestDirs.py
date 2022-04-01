# -*- coding: utf-8 -*-
"""
Created on Thu Mar 31 15:26:31 2022

@author: guyse
"""

import os, shutil, glob, sys
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
        import ieo #, S3ObjectStorage
    else:
        print('Error: that is not a valid path for the IEO module. Exiting.')
        sys.exit()

for d in [ieo.ingestdir, ieo.Sen2ingestdir]:
    flist = glob.glob(os.path.join(ieo.Sen2ingestdir, '*'))
    if len(flist) > 0:
        for f in flist:
            if os.path.isdir(f):
                print(f'Deleting path: {f}')
                shutil.rmtree(f)

print('Processing complete.')