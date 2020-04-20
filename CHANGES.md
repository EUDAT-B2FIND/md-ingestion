## CHANGES 
###Current (2020-03-26)
* updated README.md (#10)
* added old, but working slks-ff-mapfile (#13)
* added slks-dc mapfile and updated harvest_list. Added "Archaeology" to B2FIND disciplines.json. (#12) 

###1.2.0 (2020-02-27)
* Converted B2FIND code from Python 2.7 to Python 3.6 (#9)
* updated harvest_list with ESS, DaRUS and Uni Maribor
* new mapfile for Uni Maribor

###1.1.0 (2020-02-26)
Integration work for (#7)
+ ENVRIplus demonstrator 
  - including mapfiles for ENVRI Communities (such as AnaEE, SeaDataNet, Euro-Argo etc.)
+ oaitestdata 
  - for IVOA (including ivoa_ ivo_vor and ivoa_datacite schema)
  - for DansEASY
  - CLARIN (testing different endpoints)
+ B2SHARE harvesting with different enpoints using different mapfiles (e.g. for CLARIN)
+ Community integration:
  - clarin
  - envidat
  - materialscloud
  - ivoa
  - ucl
+ Community testingestion
  - anaee
  - darus
  - esrf
  - ess
  - hbp
  - slks
  - toar
+ count.py as a mini test
+ hw_harvest_list as old references to harvesting endpoints from Heiner
+ EISCAT json mapfile (#8)

###1.0.0 (2019-11-18)

Initial release for B2FIND software stack, including development during several funded projects.  
