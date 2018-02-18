#/usr/bin/python
import os, datetime, time, shutil, sys, glob, csv
from subprocess import Popen

# This needs to be updated so that it can work with ENVIfile.py

## MODIS functions
    
def extractmodisvis(hdf, outpath):
    paths= [['/ndvi','250m 16 days NDVI','_NDVI.img'],
    ['/evi','250m 16 days EVI','_EVI.img'],
    ['/viq','250m 16 days VI Quality','_VI_Quality.img'],
    ['/pixel_reliability','250m 16 days pixel reliability','_pixelreliability.img']]
    basename=os.path.basename(hdf)
    print('Starting to process HDF data set '+basename)
    for path in paths:
        dirname=outpath+path[0]
        if not os.access(dirname,os.F_OK):
            os.mkdir(dirname)
        
        print('Processing '+basename+' to directory '+dirname)
        outname=dirname+'/'+basename.replace('.hdf',path[2])
        p=Popen(['/opt/LDOPE-1.7/bin/sds2bin','-of='+outname,'-sds="'+path[1]+'"',hdf])
        print(p.communicate())
    print('Processing complete for '+basename)

def extractmodisvisgdal(hdf, outpath):
    paths= [['/ndvi','250m 16 days NDVI','_NDVI.img'],
    ['/evi','250m 16 days EVI','_EVI.img'],
    ['/viq','250m 16 days VI Quality','_VI_Quality.img'],
    ['/pixel_reliability','250m 16 days pixel reliability','_pixelreliability.img']]
    basename=os.path.basename(hdf)
    print('Starting to process HDF data set '+basename)
    for path in paths:
        dirname=outpath+path[0]
        if not os.access(dirname,os.F_OK):
            os.mkdir(dirname)
        
        print('Processing '+basename+' to directory '+dirname)
        outname=dirname+'/'+basename.replace('.hdf',path[2])
        p=Popen(['/opt/LDOPE-1.7/bin/sds2bin','-of='+outname,'-sds="'+path[1]+'"',hdf])
        print(p.communicate())
    print('Processing complete for '+basename)

def convertmodisvihdf2envi(hdffile, outpath): # This converts a LEDAPS-generated HDF to ENVI format via GDAL
    hdf=gdal.Open(hdffile)
    basename=os.path.basename(hdffile)
    dirname=os.path.dirname(os.path.abspath(hdffile))
    outpath=os.path.abspath(outpath)
    dtaglist=[]
    taglist=[]
    if len(dirname)==0:
        dirname=os.curdir()
    if len(outpath)==0:
        outpath=dirname
    print('Exporting '+basename+' to '+outpath)
    i=basename.rfind('/')+1
    j=basename.rfind('.')
    dataset=basename[i:j]
    sdsdict=hdf.GetSubDatasets()
    NDVIdat=outpath+'/ndvi/'+dataset+'_NDVI.dat'
    EVIdat=outpath+'/evi/'+dataset+'_EVI.dat'
    VIQdat=outpath+'/viq/'+dataset+'_VIQ.dat'
    PRdat=outpath+'/pixel_reliability/'+dataset+'_PR.dat'
    proclist=[['gdalTranslate', '-of','ENVI',[x for x in sdsdict if 'NDVI' in x[0]][0][0],NDVIdat],
    ['gdalTranslate', '-of','ENVI',[x for x in sdsdict if 'EVI' in x[0]][0][0],EVIdat],
    ['gdalTranslate', '-of','ENVI',[x for x in sdsdict if 'VI Quality' in x[0]][0][0],VIQdat],
    ['gdalTranslate', '-of','ENVI',[x for x in sdsdict if 'pixel reliability' in x[0]][0][0],PRdat]]
    b1name=proclist[0][3]
    b1=gdal.Open(b1name)
    rmlist=[]
    x=0
    for y in proclist:
        bandname=y[3]
        print('Processing ' +bandname)
        i=bandname.rfind(':')+1
        btag=bandname[i:]
        p=Popen(y)
        print(p.communicate())
    #    print('ENVI file created, updating header.')
        makemodishdrfile(y[4])
    hdf=None
    b1=None
    print('Processing complete.')

def makemodishdrfile(filename): # This modifies GDAL generated an ENVI header into a more useful with improved metadata.
    hdrname=filename.replace('.dat','.hdr')
    basename=os.path.basename(filename)
    x=basename.split('.')
    datestr=x[1]
    year=int(datestr[1:5])
    if year%4==0:
        yearlength=366.
    else:
        yearlength=365.
    day=int(datestr[5:])+8
    wavelength='wavelength = { '+str(float(year)+float(day)/yearlength)+'}\nfwhm = { '+str(8./yearlength)+'}\nwavelength units = Years\n'
    datetuple=time.strptime(datestr[1:],'%Y%j')
    datestring=time.strftime('%Y-%m-%d',datetuple)
    acquisitiontime='acquisition time = %s\n' % datestring
    mapinfo='map info = {Sinusoidal, 1.0000, 1.0000, -1111950.5197, 6671703.1180, 2.316564e+002, 2.316564e+002, D_Unknown, units=Meters}\nprojection info = {16, 6371007.2, 0.000000, 0.0, 0.0, D_Unknown, Sinusoidal, units=Meters}\ncoordinate system string = {PROJCS["Sinusoidal",GEOGCS["GCS_Unknown",DATUM["D_Unknown",SPHEROID["S_Unknown",6371007.181,0.0]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Sinusoidal"],PARAMETER["False_Easting",0.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",0.0],UNIT["Meter",1.0]]}\n'
    i=basename.rfind('_')+1
    j=basename.rfind('.')
    imagetype=basename[i:j]
    description='description = { %s for %s}\n' % (imagetype, datestring)
    bandnames='band names = { %s}\n' % datestring
    if imagetype=='VIQ':
        datatype='data type = 12\n'
    elif imagetype=='PR':
        datatype='data type = 1\n' 
    else:
        datatype='data type = 2\n'
    print('Processing '+hdrname)
    with open(hdrname, 'w') as output:
        output.write('ENVI\n')
        output.write(description)
        output.write('samples = 4800\nlines   = 4800\nbands   = 1\nheader offset = 0\nfile type = ENVI Standard\n')
        output.write(datatype+'interleave = bsq\nbyte order = 0\n')
        output.write(mapinfo+bandnames+wavelength+acquisitiontime)

def modis16bitndvianomaly(filename, outpath, years):
    basename=os.path.basename(filename)
    print('Creating a %d-year anomaly for %s'%(years,basename))
    name=basename[:8]
    year=int(basename[8:12])
    doy=int(basename[12:15])
    j=basename.rfind('.')
    suffix=basename[15:j]
    extension=basename[j+1:]
    params=[os.path.dirname(filename), name, years, year, doy, extension, outpath, suffix]
    if useenvipy:
        try:
            envipy.RunTool('modis16bitndvianomaly', params, Library=save_add+'\\modistools.sav')
        except:
            e = sys.exc_info()[0]
            write_to_page( "<p>Error: %s</p>" % e )
    else:
        print('Error: EnviPy not loaded, returning.')
 
def buildenviheader(hdr, args):
    with open(hdr,'w') as output:
        for arg in args:
            output.write(arg)

def readenviheader(hdr, datatype):
    args=['ENVI\n'] # Output argument list to be written to ENVI header file
    ignore=True
    bandnames=False
    desc=False
    
    basename=os.path.basename(hdr)
    if 'anomaly' in basename:
        j=basename.find('yr')
        datatype+=' %s year anomaly'%basename[16:j]
    if basename[:1]=='M':
        datestr=basename[9:16]
    else:
        datestr=basename[8:15]
    
    datetuple=time.strptime(datestr,'%Y%j')
    datestr=time.strftime('%Y-%m-%d',datetuple)
    args.append('description = { %s for %s}\n'%(datatype,datestr))
    
    bak=hdr.replace('.hdr', '.hdr.bak2')
    if os.access(bak, os.F_OK):
        os.remove(bak)
    os.rename(hdr,bak)
    linenum=1
    with open(bak,'r') as lines:
        for line in lines:
            if line[:7]=='samples':
                ignore=False
            elif line[:8]=='map info':
                args.append(line)
                ignore=True
            elif line[:10]=='projection' or line[:10]=='coordinate':
                args.append(line)
            if not ignore:
                args.append(line)
    args.append('band names = { %s}\n'%datestr)
    args.append('acquisition time = { %s}\n'%datestr)
    return args    