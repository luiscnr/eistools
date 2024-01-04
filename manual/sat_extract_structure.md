## ***2.1. Satellite extract files***
Satellite extract files contain a extract (by default 25 x 25 pixels) of satellite data centered in the in situ location.  
These files are generated using the SAT_extract module (section 3). Dimensions, variables and global attributes are summarized in Tables 1-3.

Table 1: Dimensions of satellite extract files

|**Dimension**|**Description**|**Length**
|---|---|---
|*satellite_id*|Satellite measurement|Unlimited (1 for each satellite extract file)
|*satellite_bands*|Satellite bands|Depends on sensor/processor
|*rows*|y spatial coordinate |Default: 25
|*columns*|x spatial coordinate |Default: 25
 
Table 2: Variables included in the satellite extract files.

Variable|Description|Dimensions
|---|---|---
|*satellite_bands*|Band wavelengths (nm)|*satellite_bands*
|*satellite_time*|Overpass time|*satellite_id*
|*satellite_Rrs*|Satellite-derived Rrs|*satellite_id, satellite_bands, rows, columns*
|*satellite_latitude*|Latitude|*satellite_id, rows, columns*
|*satellite_longitude*|Longitude|*satellite_id, rows, columns*
|*satellite_AOT_0865p50*|Aerosol Optical Thickness|*satellite_id, rows, columns*
|*satellite_flag*|Flags Data Set|*satellite_id, rows, columns* 
|*satellite_OAA*|Observation Azimuth Angle|*satellite_id, rows, columns*
|*satellite_OZA*|Observation Zenith Angle|*satellite_id, rows, columns*
|*satellite_SAA*|Sun Azimuth Angle|*satellite_id, rows, columns*
|*satellite_SZA*|Sun Zenith Angle|*satellite_id, rows, columns*