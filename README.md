# ieo
Irish Earth Observation (IEO) remote sensing data processing Python module

This is a geospatial perprocessing library to maintain analysis-ready datacubes for remote sensing applications in a local projection. At present, the following data sets are supported:
1. Landsat Collection 2 Level 2 surface reflectance and surface temperature (Landsat 4-5)
2. Sentinel-2 MSI Level 2A surface reflectance.

This library converts data from Universal Transverse Mercator (UTM) projections to a local projection, by default Irish Transverse Mercator (ITM). The defaults of the system can easily be modified with the appropriate date sets and EPSG codes. In all cases, outputs are in an ENVI (Harris Geospatial, Boulder, CO, USA) format, but Cloud-Optimized Geotiff will become default for the 1.6 version release. Data from the Landsat 1-5 Multispectral Scanner (MSS) instruments are not currently supported.

As of version 1.1.0, this module will now support any local/ regional projection available. The user, however, will have to replace Ireland-specific data sets with those appropriate to their local region. Regional settings can be set by modifying config/sample_ieo.ini to config/ieo.ini.

As of version 1.5, this module supports the use of PostGIS, Amazon S3 Object Storage, and Sentinel-2 Level 2A Surface Reflectance data products.

Currently, only Landsat 4-5 TM, 7 ETM+, 8-9 ETM+, and Sentinel-2 MSI functionality are being actively maintained by the author. Sentinel-1 SLC and GRD support will be added in for version 1.6, but will likely require the use of the Sentinel-1 Toolbox.
