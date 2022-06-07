# ieo
Irish Earth Observation (IEO) remote sensing data processing Python module

Image processing library for processing Landsat LEDAPS surface reflectance data to a local projection (the default is Irish Transverse Mercator, but can be modified for any EPSG-coded projection)

This library, at present, is designed to import data from Landsat. The library will convert LEDAPS surface reflectance data for Landsat Thematic Mapper (TM), Enhanced Thematic Mapper+ (ETM+), and Operational Land Imager (OLI) and Thermal Infrared Sensor (TIRS) data from Universal Transverse Mercator (UTM) projections to a local projection, by default Irish Transverse Mercator (ITM). The defaults of the system can easily be modified with the appropriate date sets and EPSG codes. In all cases, outputs are in an ENVI (Harris Geospatial, Boulder, CO, USA) format. Data from the Landsat 1-5 Multispectral Scanner (MSS) instruments are not currently supported.

As of version 1.1.0, this module will now support any local/ regional projection available. The user, however, will have to replace Ireland-specific data sets with those appropriate to their local region. Regional settings can be set by modifying config/sample_ieo.ini to config/ieo.ini.

Currently, only Landsat 4-5 TM, 7 ETM+, 8-9 ETM+, and Sentinel-2 MSI functionality are being actively maintained by the author.

This branch is now closed- please use the "master", "Dev1.6", or any later branch.
