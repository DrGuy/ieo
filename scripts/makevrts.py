#!/usr/bin/env python3
# By Guy Serbin, EOanalytics Ltd.
# Talent Garden Dublin, Claremont Ave. Glasnevin, Dublin 11, Ireland
# email: guyserbin <at> eoanalytics <dot> ie

# Irish Earth Observation (IEO) Python Module
# version 1.5

# This script creates VRTs from ingested Landsat data and catalogue files

import os, sys, glob, datetime, argparse#, ieo
from subprocess import Popen
from osgeo import ogr

try: # This is included as the module may not properly install in Anaconda.
    import ieo
except:
    ieodir = os.getenv('IEO_INSTALLDIR')
    if not ieodir:
        print('Error: IEO failed to load. Please input the location of the directory containing the IEO installation files.')
        ieodir = input('IEO installation path: ')
    if os.path.isfile(os.path.join(ieodir, 'ieo.py')):
        sys.path.append(ieodir)
        import ieo
    else:
        print('Error: that is not a valid path for the IEO module. Exiting.')
        sys.exit()

parser = argparse.ArgumentParser('This script creates VRT files for Landsat data.')

parser.add_argument('-i', '--indir', type = str, default = None, help = 'Input directory. If this is set then --nodataval must also be set. Otherwise, default values will be used.')
parser.add_argument('-o', '--outdir', type = str, default = None, help = 'Data output directory.')
parser.add_argument('-y', '--year', type = int, default = None, help = 'Process secenes only for a specific year.')
parser.add_argument('--overwrite', action = "store_true", help = 'Overwrite existing files.')
parser.add_argument('--nodataval', type = int, default = None, help = 'No data value. This must be set if --indir is also set.')
#parser.add_argument('--minrow', type = int, default = 21, help = 'Lowest WRS-2 Row number.')
parser.add_argument('--rowspath', type = int, default = 4, help = 'Max WRS-2 Rows per Path.')
args = parser.parse_args()

# nodatavals = {'SR': '-9999', 'Fmask': '255', 'BT': '-9999', 'NDVI': '0', 'EVI': '0', 'pixel_qa': '1'}
{'SR': '-9999', 'aerosol_qa': '1', 'ST': '-9999', 'NDVI': '0', 'EVI': '0', 'pixel_qa': '1', 'radsat_qa' : '255'}

if args.indir and args.nodataval:
    nodatavals = {os.path.basename(args.indir): args.nodataval}
elif args.indir and not args.nodataval:
    indirs = [args.indir]
    if not os.path.basename(args.indir) in nodatavals.keys():
        args.nodataval = input('Error: --indir set and --nodataval not set. Please input a no data value:')
        nodatavals = {args.indir: args.nodataval}
else:
    indirs = [ieo.srdir, ieo.aerosolqadir, ieo.stdir, ieo.ndvidir, ieo.evidir, ieo.pixelqadir, ieo.radsatqadir]
    nodatavals = {'SR': '-9999', 'aerosol_qa': '1', 'ST': '-9999', 'NDVI': '0', 'EVI': '0', 'pixel_qa': '1', 'radsat_qa' : '255'}

def makefiledict(dirname, year):
    if args.year:
        flist = glob.glob(os.path.join(dirname, 'L*{}*.dat'.format(args.year)))
    else:
        flist = glob.glob(os.path.join(dirname, 'L*.dat'))
    filedict = {}
    if len(flist) >= 2:
        if os.path.basename(flist[0]).find('_') == 3:
            rangerow = [4, 4, 11]
        elif len(os.path.basename(flist[0])) > 40:
            rangerow = [7, 10, 17]
        else:
            rangerow = [6, 9, 16]
        if rangerow[0] != rangerow[1] and len(flist) == 2 and os.path.basename(flist[0])[rangerow[0]:rangerow[1]] == os.path.basename(flist[1])[rangerow[0]:rangerow[1]]:
            filedict = None
            return filedict
        for f in flist:
            basename = os.path.basename(f)
            if not basename[rangerow[1]:rangerow[2]] in filedict.keys():
                filedict[basename[rangerow[1]:rangerow[2]]] = [f]
            elif not f in filedict[basename[rangerow[1]:rangerow[2]]]:
                filedict[basename[rangerow[1]:rangerow[2]]].append(f)
    return filedict

def getpathrows():
    pathrowdict = {'paths': {}, 'rows': []}
    driver = ogr.GetDriverByName("GPKG")
    data_source = driver.Open(ieo.ieogpkg, 0)
    layer = data_source.GetLayer(ieo.WRS2)
    for feature in layer:
        path = feature.GetField('PATH')
        row = feature.GetField('ROW')
        if not path in pathrowdict['paths']:
            pathrowdict['paths'][path] = [row]
        elif not row in pathrowdict['paths'][path]:
            pathrowdict['paths'][path].append(row)
        if not row in pathrowdict['rows']:
            pathrowdict['rows'].append(row)
        
    data_source = None
    for path in pathrowdict['paths'].keys():
        sorted(pathrowdict['paths'][path])
    sorted(pathrowdict['rows'])
    
    return pathrowdict

def makevrtfilename(outdir, filelist):
    numscenes = len(filelist)
    basename = os.path.basename(filelist[0]).replace('.dat', '.vrt')
    if basename.find('_') == 3:
        startrow = 0
        endrow = 0
        outbasename = '{}.vrt'.format(basename[:11])
    elif len(basename) < 40:
        startrow = basename[8:9]
        endrow = os.path.basename(filelist[-1])[8:9]
        outbasename = '{}{}{}{}{}'.format(basename[:6], numscenes, startrow, endrow, basename[9:])
    else:
        startrow = basename[9:10]
        endrow = os.path.basename(filelist[-1])[9:10]
        outbasename = '{}{}{}{}{}'.format(basename[:7], numscenes, startrow, endrow, basename[10:])
    vrtfilename = os.path.join(outdir, outbasename)
    return vrtfilename

def writetocsv(catfile, vrt, filelist, d, pathrowdict):
    if os.path.basename(vrt).find('_') == 3:
        minrow = ''
    else:
        minrow = min(pathrowdict['rows'])
    datetuple = datetime.datetime.strptime(d, '%Y%j')
    scenelist = ['None'] * args.rowspath
    for f in filelist:
        if os.path.basename(vrt).find('_') == 3:
            sceneID = os.path.basename(f)[:11]
            i = 0
            path = 0
        elif len(os.path.basename(f)) < 40:
            sceneID = os.path.basename(f)[:21]
            i = int(sceneID[7:9]) - minrow
            path = sceneID[3:6]
        else:
            sceneID = os.path.basename(os.path.basename(f))[:40]
            i = int(sceneID[8:10]) - minrow
            path = sceneID[4:7]
        scenelist[i] = sceneID
    header = 'Date,Year,DOY,Path'
    for x in pathrowdict['rows']:
        header += ',R{:03d}'.format(x)
        header += ',VRT'
    if not os.path.isfile(catfile): # creates catalog file if missing
        with open(catfile, 'w') as output:
            output.write('{}\n'.format(header))    
    outline = '{},{},{},{}'.format(datetuple.strftime('%Y-%m-%d'), datetuple.strftime('%Y'), datetuple.strftime('%j'), path)
    for s in scenelist:
        outline += ',{}'.format(s)
    with open(catfile, 'a') as output:
        output.write('{}\n'.format(outline))
    
def makevrt(filelist, catfile, vrt, d, pathrowdict):
    dirname, basename = os.path.split(vrt)
    print('Now creating VRT: {}'.format(basename))
    proclist = ['gdalbuildvrt', '-srcnodata', nodatavals[os.path.basename(os.path.dirname(filelist[0]))], vrt]    
#    scenelist.append(vrt)
    for f in filelist:
        if f:
            proclist.append(f)
#    print(proclist)
    p = Popen(proclist)
    print(p.communicate())
    writetocsv(catfile, vrt, filelist, d, pathrowdict)

today = datetime.datetime.today()
catdir = os.path.join(ieo.catdir, 'Landsat')
pathrowdict = getpathrows()

for indir in indirs:
    print('Now processing files in subdir {}, number {} of {}.'.format(os.path.basename(indir), indirs.index(indir) + 1, len(indirs)))
    if args.outdir:
        vrtdir = args.outdir
    else:
        vrtdir = os.path.join(indir, 'vrt')
    print('New VRTs will be written to: {}'.format(vrtdir))
    
    if not os.path.isdir(vrtdir):
        os.mkdir(vrtdir)
    catfile = os.path.join(catdir, '{}_vrt.csv'.format(os.path.basename(indir)))
    print('New VRTs created will be logged in: {}'.format(catfile))
        
    filedict = makefiledict(indir, args.year)
    keylist = sorted(filedict.keys())
    if len(keylist) > 0:
        for key in keylist:
            if len(filedict[key]) > 1:
                filedict[key].sort()
                vrt = makevrtfilename(vrtdir, filedict[key])
                if args.overwrite or not os.path.isfile(vrt):
                    print('Now processing {}, number {} of {}.'.format(os.path.basename(vrt), keylist.index(key) + 1, len(keylist)))
                    makevrt(filedict[key], catfile, vrt, key, pathrowdict)
                else:
                    print('{} exists and no overwrite set, skipping.'.format(os.path.basename(vrt)))
            else:
                print('An insufficient number of scenes for dat {} exist, skipping.'.format(key))
        
print('Processing complete.')