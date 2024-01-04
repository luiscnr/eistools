## ***2.1. Satellite extract files***
Satellite extract files contain a extract (by default 25 x 25 pixels) of satellite data centered in the in situ location.  
These files are generated using the SAT_extract module (section 3). Dimensions, variables and global attributes are summarized in Tables 1-3.

Table 1: Dimensions of satellite extract files

|*Dimension*|*Description*|*Length*|
|satellite_id|Satellite measurement|Unlimited (1 for each satellite extract file)|
|satellite_bands|Satellite bands|Depends on sensor/processor|
|rows|y spatial coordinate |Default: 25|
|columns|x spatial coordinate |Default: 25|
 


