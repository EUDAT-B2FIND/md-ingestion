## CHANGES

### 2.0.1 (2020-09-29)

* slks ingestion on productive B2FIND


### 2.0.0 (2020-09-23)

* Fixed upload (#120).
* geofon (#119).
* Commtesting (#118).
* Slks (#114, #115, #116, #122).
* Seanoe (#113).
* Parse DublinCore bbox (#112).
* New folder 'demo' for EnvriPlus demonstrator (#110).
* Removed legacy code (#106, #107, #109).

### 1.5.0 (2020-09-15)
* Fidgeo (#105)
* fix classify.map_discipline
* from EUDAT-B2FIND/herbadrop (#101)
* Toar (#100)
* Ivoa (#99)
* Dataverseno (#98)
* Darus (#97)
* linkcheck (#95)
* use common discipline function (#94)
* Changes in <contact> for EnviDat (#91)
* changes for relatedIdentifer for IVOA (#92)
* fixed envidat iso test
* fixed datacite contact

### 1.4.0 (2020-09-02)
* Create schema__transition
* changes for toar, ess, radar and b2find export
* Readthedocs (#27)
* Update .travis.yml
* Update annastest (#26)
* Annastest (#25)
* New entry darus ingestion_list. New cronjob darus.
* added mapper tests (#24)
* Add setup py (#22) (#23)

### 1.3.0 (2020-05-11)
* Jsonsraus (#21)
* bugfix geopoint-cloud (#20)
* Darus comm (#19)
* moving seanoe to oaitestdata (#18)
* Pointerforhl (#17)
* Testdata (#16)
* Bugfix mapping (#15)
* Update docs (#14)
* added old, but working slks-ff-mapfile (#13)
* added slks-dc mapfile and updated harvest_list. Added "Archaeology" to B2FIND disciplines.json. (#12)
* updated README.md (#10)

### 1.2.0 (2020-02-27)
* Converted B2FIND code from Python 2.7 to Python 3.6 (#9)
* updated harvest_list with ESS, DaRUS and Uni Maribor
* new mapfile for Uni Maribor

### 1.1.0 (2020-02-26)
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

### 1.0.0 (2019-11-18)

Initial release for B2FIND software stack, including development during several funded projects.  
