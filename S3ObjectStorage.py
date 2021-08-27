#/usr/bin/python
# By Guy Serbin, EOanalytics Ltd.
# Talent Garden Dublin, Claremont Ave. Glasnevin, Dublin 11, Ireland
# email: guyserbin <at> eoanalytics <dot> ie

# Irish Earth Observation (IEO) Python Module
# S3 Object Storage 
# Version 1.5
# Update history:
    # 21 April 2021 Code is being modified from s3fs to boto3
    # 18 August 2021 Incorporated into Irish Earth Observation (IEO) Python 
        # module v1.5dev


# This module was developed from code initially intended to identify data in 
# Mundi/ Open Telekom Cloud S3 buckets. These functions will be transferred to
# the appropriate submodules, with this one being used solely to interface 
# with S3 object storage

import os, argparse, sys, glob, boto3, datetime
from subprocess import Popen
from pkg_resources import resource_stream, resource_string, resource_filename, Requirement
# from pkg_resources import resource_stream, resource_string, resource_filename, Requirement

# if 'linux' in sys.platform:
#     configdir = '~/.MundiTools'
# else:
#     configdir = r'%\.MundiTools'


if sys.version_info[0] == 2:
    import ConfigParser as configparser
else:
    import configparser

# Access S3 credentials



# Access configuration data 
config = configparser.ConfigParser()
configfile = 'config/ieo.ini'
config_location = resource_filename(Requirement.parse('ieo'), configfile)
config.read(config_location)

parser = argparse.ArgumentParser(description = 'IEO Object Storage interface submodule.')
# parser.add_argument('--indir', type = str, default = None, help = 'Optional input directory in which to search for files. This is ignored if --batch=True.')
# parser.add_argument('--outdir', type = str, default = None, help = 'Optional output directory in which to save products.')
# parser.add_argument('--baseoutdir', type = str, default = config['DEFAULT']['BASEOUTPUTDIR'], help = 'Base output directory in which to save products.')
# parser.add_argument('-o', '--outfile', type = str, default = None, help = 'Output product filename. If --infile is not set, then this flag will be ignored.')
# parser.add_argument('-i', '--infile', type = str, default = None, help = 'Input file name.')
# parser.add_argument('-g', '--graph', type = str, default = r'C:\Users\Guy\.snap\graphs\User Graphs\MCI_Resample_S2_20m.xml', help = 'ESA SNAP XML graph file path.')
# parser.add_argument('-e', '--op', type = str, default = None, help = 'ESA SNAP operator.')
# parser.add_argument('-p', '--properties', type = str, default = r'D:\Imagery\Scripts\Mci.S2.properties', help = 'ESA SNAP GPT command properties text file path.')
# parser.add_argument('-d', '--dimap', type = bool, default = 'store_true', help = 'Input files are BEAM DIMAP.')
# parser.add_argument('--gpt', type = str, default = r'C:\Program Files\snap\bin\gpt', help = 'ESA SNAP XML graph file path.')
# parser.add_argument('--overwrite', type = bool, default = False, help = 'Overwrite any existing files.')
# parser.add_argument('--mgrs', type = str, default = '30TUK', help = 'Sentinel-2 MGRS Tile name.')
# parser.add_argument('--batch', type = bool, default = True, help = 'Process all available scenes for a given satellite/ sensor combination.')
# parser.add_argument('--sentinel', type = str, default = '2', help = 'Sentinel satellite number (default = 2).')
# parser.add_argument('--product', type = str, default = 'l2a', help = 'Sentinel product type (default = l2a, will be different for different sensors).')
# parser.add_argument('--bucket', type = str, default = None, help = 'Only process data from a specific bucket.')
parser.add_argument('--url', type = str, default = config['S3']['endpoint_url'], help = 'Alternative S3 bucket URL. If used, you must also present a different --credentials file from the default.')
parser.add_argument('--credentials', type = str, default = config['S3']['credentials'], help = 'Full path of S3 credentials CSV file to use.')
# parser.add_argument('--warp', type = str, default = None, help = 'Warp products to specific projection EPSG code. Example: for Irish Transverse Mercator (EPSG:2157), use "2157". Not implemented yet.')

args = parser.parse_args()

# S2tiles = config['DEFAULT']['S2tiless2'].split(',')

# suffixdict = {
#     'Mci.S2' : 's2mci',
#     'S2Resampling' : 's2resampled',
#     }

# sensordict = {
#     '1' : {
#         'default' : 'GRD',
#         'accept' : ['SLC', 'GRD', 'RAW', 'OCN'],
#         'startyear' : 2014,
#         'qyear' : 2018,
#         },
#     '2' : {
#         'default' : 'L2A',
#         'accept' : ['L1C', 'L2A'],
#         'startyear' : 2015,
#         'qyear' : 2018,
#         },
#     '3' : {
#         'default' : 'OLCI',
#         'accept' : ['OLCI', 'SRAL'],
#         'startyear' : 2018,
#         'qyear' : None,
#         },
#     '5P' : {
#         'default' : 'L2',
#         'accept' : ['L2'],
#         'startyear' : 2018,
#         'qyear' : None,
#         },
#     }

# if not args.sentinel.upper() in sensordict.keys():
#     print('ERROR: --sentinel value is not supported at this time, or is misspelt.')
#     sys.exit()

# if not args.sensor:
#     args.sensor = sensordict[args.sentinel]['default']
#     print('WARNING: --sensor not set. Using default Sentinel{} value: {}'.format(args.sentinel.upper(), args.sensor))
# elif not args.sensor.upper() in sensordict[args.sentinel]['accept']:
#     print('ERROR: --sensor value is not supported at this time, or is misspelt.')
#     sys.exit()

# if args.outfile:
#     if os.path.isdir(os.path.dirname(args.outfile)):
#         args.outdir = os.path.dirname(args.outfile)

# if args.infile:
#     if os.path.isdir(os.path.dirname(args.infile)):
#         args.indir = os.path.dirname(args.infile)
        
# if not args.batch and (not args.indir or not os.path.isdir(args.indir)):
#     print('ERROR: --batch=False and --infile or --indir settings are invalid. Please fix.')
#     sys.exit()
    
# if not os.path.isdir(args.outdir):
#     try:
#         os.mkdir(args.outdir)
#         print('Created on disk: {}'.format(args.outdir))
#     except:
#         print('ERROR: --outdir {} does not exist and cannot be created. Please fix.'.format(args.outdir))
#         sys.exit()

# ignorelist = []
# if not args.overwrite:
#     flist = glob.glob(os.path.join(args.outdir, '*.dim'))
#     if len(flist) > 0:
#         for f in flist:
#             ignorelist.append(os.path.basename(f)[:60])

# buckets = []
# if args.bucket:
#     buckets = [args.bucket.lower()]
# elif args.batch:
#     if not sensordict[args.sentinel]['qyear']:
#         buckets.append('s{}-{}'.format(args.sentinel, args.sensor).lower())
#     else:
#         now = datetime.datetime.now()
#         for year in range(sensordict[args.sentinel]['startyear'], now.year + 1):
#             if year < sensordict[args.sentinel]['qyear']:
#                 buckets.append('s{}-{}-{}'.format(args.sentinel, args.sensor, year).lower())
#             elif year < now.year:
#                 for q in range(0, 4):
#                     buckets.append('s{}-{}-{}-q{}'.format(args.sentinel, args.sensor, year, q + 1).lower())
#             else:
#                 for q in range (0, (now.month - 1) // 3 + 1):
#                     buckets.append('s{}-{}-{}-q{}'.format(args.sentinel, args.sensor, year, q + 1).lower())
            
# if len(buckets) == 0:
#     print('ERROR: No buckets or data were found to process. Exiting')
#     sys.exit()

def readcredentials():
    credentials = {}
    with open(args.credentials, 'r') as lines:
        for line in lines:
            line = line.rstrip().split(',')
            if line[0] == 'User Name':
                headers = line
            else:
                if len(line) > 0:
                    for i in range(len(line)):
                        credentials[headers[i]] = line[i]
    return credentials

def s3resource(*args, **kwargs):
    s3res = boto3.resource('s3', endpoint_url = args.url)
    return s3res

def s3client(*args, **kwargs):
    s3cli = boto3.client('s3', endpoint_url = args.url)
    return s3cli

# def getlocalbuckets(s3res, *args, **kwargs):
#     localbuckets = []
#     for bucket in s3res.buckets.all():
#         print('Adding {} to local buckets list.'.format(bucket.name))
#         localbuckets.append(bucket.name)
#     return localbuckets

# def findscenes(s3cli, bucket, tile, scenedict):
#     if bucket[-2:-1] == 'q':
#         startmonth = (int(bucket[-1:]) - 1) / 3 + 1
#         endmonth = startmonth + 2
#         year = bucket[-7:-3]
#     else:
#         startmonth = 1
#         endmonth = 12
#         year = bucket[-4:]
#     for m in range(startmonth, endmonth + 1):
#         for d in range(1, 32):
#             prefix = '{}/{}/{}/{}/{:02}/{:02}'.format(tile[:02], tile[2:3], tile[3:], year, m, d)
#             response = s3cli.list_objects_v2(Bucket = bucket, Prefix = prefix)
#             if 'Contents' in response.keys():
#                 if not tile in scenedict.keys():
#                     scenedict[tile] = {}
#                 scene = response['Contents'][0]['Key'].split('/')[6]
#                 datetuple = datetime.datetime.strptime('{}/{}/{}'.format(year, m, d), '%Y%m%d')
#                 sdict = {
#                     'year' : int(year), 
#                     'month' : m,
#                     'day' : d,
#                     'datetuple' : datetuple,
#                     'bucket' : bucket,
#                     'prefix' : '{}/{}'.format(prefix, scene),
#                     }
#                 if not scene in scenedict[tile].keys():
#                     scenedict[tile].append(scene : sdict)
#     return scenedict 
            
def copyfilestobucket(*args, **kwargs):
    # This function will copy local files to a specified S3 bucket
    bucket = kwargs.get('bucket', None) # name of S3 bucket to save files.
    filename = kwargs.get('filename', None) # single file to be coped to S3 bucket
    filelist = kwargs.get('filelist', None) # list of files to be copied to S3 bucket
    copydir = kwargs.get('copydir', None) # directory containing files to be copied to S3 bucket
    inbasedir = kwargs.get('inbasedir', None) # start of local directory path to be stripped from full file path. Only used if "copydir" is is not used.
    targetdir = kwargs.get('targetdir', None) # name of directory in which to copy file or files. If empty, will be taken from the directory of the first file in the list if "copydir" is used, or be determined by processing time
    flist = []
    dirlist = []
    i = 0
    if copydir:
        i = len(copydir)
        if not os.path.isdir(copydir):
            print('ERROR: The path {} does not exist or is not a folder. Exiting.'.format(copydir))
            sys.exit()
        for root, dirs, files in os.walk(copydir):
            for name in files:
                flist.append(os.path.join(root, name))
    elif filelist:
        try:
            if (len(filelist == 0)) or (not isinstance(filelist, 'list')):
                print('ERROR: "filelist" keyword used, but either has zero items or is not a list object. Exiting.')
                sys.exit()
        except Exception as e:
            print('ERROR, an exception has occurred, exiting: {}'.format(e))
            sys.exit()
        flist = filelist
    else:
        if not os.path.isfile(filename):
            print('ERROR: {} does not exist. Exiting.'.format(filename))
            sys.exit()
        else:
            flist.append(filename)
    if not targetdir:
        if copydir:
            if flist[0][i : i + 1] == '/':
                i += 1
            diritems = flist[0][i:].split('/') 
            if len(diritems) == 1:
                now = datetime.datetime.now()
                targetdir = now.strftime('%Y%m%d-%H$M%S')
            else:
                targetdir = diritems[0]
        else:
            now = datetime.datetime.now()
            targetdir = now.strftime('%Y%m%d-%H$M%S')
    if inbasedir and not copydir:
        if os.path.isdir(inbasedir):
            if inbasedir.endswith('/'):
                i = len(inbasedir)
            else:
                i = len(inbasedir) + 1
        else:
            print('ERROR: "inbasedir" {} is not a folder on the local machine. Files will be saved to the base target directory {}.'.format(inbasedir, targetdir))
    numerrors = 0
    for f in flist:
        print('Now copying {} to bucket {}. ({}/{})'.format(f, bucket, flist.index(f + 1), len(flist)))
        if not os.path.isfile(f):
            print('ERROR: {} does not exist on disk, skipping.'. format(f))
            numerrors += 1
        else:
            if i > 0:
                targetfile = "{}/{}".format(targetdir, f[i:])
            else:
                targetfile = "{}/{}".format(targetdir, os.path.basename(f))
            s3cli.upload_file(f, bucket, targetfile)
    print('Upload complete. {}/{} files uploaded, with {} errors.'. format(len(flist) - numerrors, len(flist), numerrors))
            
            
def downloadfile(outdir, bucket, s3_object):
    print('Downloading file {} from bucket {} to: {}'.format(s3_object, bucket, outdir))
    if not os.path.isdir(outdir):
        os.makedirs(outdir)
    outfile = os.path.join(outdir, s3_object)
    s3cli.download_file(bucket, s3_object, outfile)
    
def getbucketobjects(bucket):
    # code borrowed from 
    print('Retrieving objects for S3 bucket: {}'.format(bucket))
    outdict = {}
    contents = s3cli.list_objects_v2(Bucket = bucket)['Contents']
    for s3_key in contents:
        s3_object = s3_key['Key']
        if not s3_object.endswith('/'):
            outdname, outfname = os.path.split(s3_object)
            if not outdname in outdict.keys():
                outdict[outdname] = []
            outdict[outdname].append(outfname)
            
        else:
            outdict[outdname] = []
    return outdict

def downloadscene(scenedict, sceneid, downloaddir):
    # code borrowed from 
    outdir = os.path.join(downloaddir, sceneid)
    print('Downloading scene {} to: {}'.format(sceneid, outdir))
    if not os.path.isdir(outdir):
        os.path.mkdir(outdir)
    bucket = scenedict[sceneid]['bucket']
    prefix = scenedict[sceneid]['prefix']
    i = len(prefix)
    contents = s3cli.list_objects_v2(Bucket = bucket, Prefix = prefix)['Contents']
    for s3_key in contents:
        s3_object = s3_key['Key']
        if not s3_object.endswith('/'):
            outdname, outfname = os.path.split(s3_object[i + 1:])
            outdir = os.path.join(outdir, outdname)
            if not os.path.isdir(outdir):
                os.makedirs(outdir)
            outfile = os.path.join(outdir, outfname)
            s3cli.download_file(bucket, s3_object, outfile)
        else:
            if not os.path.isdir(s3_object):
                os.makedirs(s3_object)
    print('Scene {} has been downloaded.'.format(sceneid))

s3cli = s3client(args.url)
s3res = s3resource(args.url)         

# def openS3(credentials, url):
#     # bucketurl = '{}/{}'.format(url, bucket)
#     print('Accessing server {}.'.format(url))
#     try:    
#         s3 = s3fs.S3FileSystem(
#               anon = False,
#               key = credentials['Access Key Id'], 
#               secret = credentials['Secret Access Key'], 
#               client_kwargs = {'endpoint_url' : url})
#     except:
#         print('ERROR: S3 server is not valid or there is an error with the credentials: {}'.format(url))
#         s3 = None
#     return s3
# if not os.path.isfile(args.graph):
#     print('ERROR: --graph setting is invalid. Please fix.')
#     sys.exit()

# def procscene(infile, outfile):
#     if args.op:
#         proclist = [args.gpt, args.op, '-p', args.properties, '-SsourceProduct="{}"'.format(infile), '-t', '"{}"'.format(outfile)]
#     else:
#         proclist = [args.gpt, args.graph, '-Pfile="{}"'.format(infile), '-Ptarget', '"{}"'.format(outfile)]
#     p = Popen(proclist)
#     print(p.communicate())

# def procscenes(infilelist, outfilelist):
#     if len(infilelist) > 0:
#         for i in range(len(infilelist)):  
#             print('Now processing scene {} ({}/{}).'.format(os.path.basename(os.path.dirname(infilelist[i])), i + 1, len(infilelist)))
#             procscene(infilelist[i], outfilelist[i])
#         print('Processing complete.')
#     else:
#         print('ERROR: No scenes found to process.')
#         sys.exit()


    

# infilelist = []
# outfilelist = []
# if args.infile:
#     if os.path.isfile(args.infile):
#         infilelist.append(args.infile)
#     elif args.indir:
#         if os.path.isfile(os.path.join(args.indir, args.infile)):
#             infilelist.append(os.path.join(args.indir, args.infile))
#         else:
#             print('ERROR: --infile or --indir settings are invalid. Please fix.')
#             sys.exit()
#     else:
#         print('ERROR: --infile or --indir settings are invalid. Please fix.')
#         sys.exit()
        
#     if not args.outfile:
#         outfilelist.append(os.path.join(args.outdir, '{}_s2resampled.dim'.format(os.path.basename(os.path.dirname(args.infile)))))
#     else:
#         if os.path.isdir(os.path.dirname(args.outfile)):
#             outfilelist.append(args.outfile)
#         else:
#             outfilelist.append(os.path.join(args.outdir, args.outfile))
# elif args.indir:
#     for root, dirs, files in os.walk(args.indir):
#         for name in files:
#             if args.dimap:
#                 if name.endswith('.dim'):
#                     print('Adding scene {} to processing list.'.format(os.path.basename(name)))
#                     infilelist.append(os.path.join(root, name))
#                     outfilelist.append(os.path.join(args.outdir, '{}_{}.dim'.format(name[:-4], suffixdict[args.op])))
#             if name == 'MTD_MSIL2A.xml':
#                 print('Adding scene {} to processing list.'.format(os.path.basename(root)))
#                 infilelist.append(os.path.join(root, name))
#                 outfilelist.append(os.path.join(args.outdir, '{}_s2resampled_s2mci.dim'.format(os.path.basename(root))))

# scenedict = {}

# create S3 Client and Resource objects



# def main():
#     # credentials = readcredentials()
#     # s3 = openS3(credentials, args.url)
    
#     for bucket in buckets:
#         try:
#             if args.sentinel == '2':
#                 if args.mgrs:
#                     tiles = [args.mgrs]
#                 else:
#                     tiles = S2tiles
#                 for tile in tiles:
#                     scenedict = findscenes(s3cli, bucket, tile, scenedict)
                
#                 # basesearchdir = '{}/{}/{}/{}/{}'.format(bucket, args.mgrs[:2], args.mgrs[2:3], args.mgrs[3:], bucket[7:11])
#                 # months = s3.ls('{}/{}/{}/{}/{}'.format(bucket, args.mgrs[:2], args.mgrs[2:3], args.mgrs[3:], bucket[7:11]))
#                 # if len(months) == 0:
#                 #     print('ERROR, no useful data found for this period.')
#                 # else:
#                 #     for month in months:
#                 #         if not (month.endswith('quicklook') or month.endswith('thumbnail')):
#                 #             days = s3.ls(month)
#                 #             for day in days:
#                 #                 scenepath = s3.ls(day)[0]
#                 #                 sceneid = os.path.basename(scenepath)[:60]
#                 #                 if not sceneid in ignorelist:
#                 #                     infilelist.append(scenepath)
#         except Exception as e:
#             print('ERROR: {}'.format(e))
#             sys.exit()
        
    

# if __name__ == '__main__':
#     main()
    