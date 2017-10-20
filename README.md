# ieo
Irish Earth Observation (IEO) remote sensing data processing Python module

Image processing library for Landsat and MODIS to Irish Transverse Mercator Projection

This library, at present, is designed to import data from Landsat and MODIS sensors. The library will convert Landsat Thematic Mapper (TM), Enhanced Thematic Mapper+ (ETM+), and Operational Land Imager (OLI) and Thermal Infrared Sensor (TIRS) data from Universal Transverse Mercator (UTM) zones 29 and 30 North to Irish Transverse Mercator (ITM). It will also convert specific MODIS data products from MODIS Sinusoidal to ITM. In all cases, outputs are in an ENVI (Harris Geospatial, Boulder, CO, USA) format. Data from the Landsat 1-5 Multispectral Scanner (MSS) instruments are not currently supported.

Currently, only Landsat functionality is being actively maintained by the author, with Sentinel-2 support to be added soon.
