Disclaimer
----------
Copyright (c) 2014 Heinrich Widmann (DKRZ)

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

md-ingestion
============

The python script manager.py in this directory provide functionality required for
ingestion (including OAI harvesting, semantic mapping (see repo 'md-mapping') and uploading to CKAN ) of metadata in the B2FIND portal.

Preconditions
=============

The sript manager.py uses the classes and functions from

"""B2FIND.py - classes for JMD management :
  - CKAN_CLIENT  Executes CKAN APIs (interface to CKAN)
  - HARVESTER
  - CONVERTER
  - UPLOADER

Install required modules simplejson, sickle and e.g. by :
  > sudo pip install <module>

or just by installing all modules listed in requirements.txt :
  > less requirements.txt
  sickle
  lxml
  simplejson
  python-Levenshtein
  git+https://github.com/noumar/iso639.git

  > sudo pip isntall -r requiremnets.txt

Or - if this not works for some reason for xml - try
  > sudo apt-get install python-lxml
or on systems with no apt-get but yum available :
  > sudo yum install python-lxml
or (if this is not workabl, e.g. on some SuSE distr.) try
  > sudo zypper install <module>

And set PYTHONPATH to needed modules (must be downloaded if not available) :

   > setenv PYTHONPATH "/home/k204019/Downloads/PicklingTools151Release/Python"

Usage
=====
manager.py [ OPTIONS ]


Description
===========
Management of meta data within EUDAT Joint Metadata Domain (JMD), i.e.
I.  Ingestion of meta data comprising
- 1. Harvesting of XML files from OAI-PMH MD provider(s)
- 2. Converting XML to Jason and semantic mapping of tags to CKAN fields
- 3. Uploading resulting JSON {key:value} dict's as datasets to JMD portal

Options
=======
--version               show program's version number and exit
--help, -h              show this help message and exit
--verbose, -v           increase output verbosity (e.g., -vv is more than -v)
--jobdir=JOBDIR          directory where log, error and html-result files are
                        stored. By default directory is created as
                        startday/starthour/processid .
--mode= h | harvest | c | convert | u | upload | h-c | c-u | h-u | h-d | d | delete
                         This can be used to do a partial workflow. If you use
                        converting without uploading the data will be stored
                        in .json files. Default is "h-u" which means a totally
                        ingestion with (h)arvesting, (c)onverting and
                        (u)ploading to a CKAN database.
--check_mappings=BOOLEAN
                        Check all mappings which are stored in './maptables/'
                        for converting the .xml in .json format and choose the
                        mapping table with the best results.
--community=STRING, -c STRING
                        community where data harvested from and uploaded to
--fromdate=DATE         Filter harvested files by date (Format: YYYY-MM-DD-hh-
                        mm-ss).
--epic_check=FILE       check and generate handles of CKAN datasets in handle
                        server EPIC and with credentials as specified in given
                        credstore file
--ckan_check=BOOLEAN    check existence and checksum against existing datasets
                        in CKAN database
--outdir=PATH, -d PATH  The absolute root dir in which all harvested files
                        will be saved. The converting and the uploading
                        processes work with the files from this dir. (default
                        is '/oaidata')
Multi Mode Options
------------------
Use these options if you want to ingest from a list in a file.

--list=FILE, -l FILE    list of OAI harvest sources (default is
                        ./harvest_list)
--parallel=PARALLEL     [DEPRECATED]

Single Mode Options
-------------------
Use these options if you want to ingest from only ONE source.

--source=PATH, -s PATH  A URL to .xml files which you want to harvest
--verb=STRING           Verbs or requests defined in OAI-PMH, can be
                        ListRecords (default) or ListIdentifers here
--mdsubset=STRING       Subset of harvested meta data
--mdprefix=STRING       Prefix of harvested meta data

Upload Options
--------------
These options will be required to upload an dataset to a CKAN database.

--iphost=IP, -i IP      IP adress of JMD portal (CKAN instance)
--auth=STRING           Authentification for CKAN APIs (API key, iby default
                        taken from file $HOME/.netrc)



Heinrich Widmann, 11.4.2014
John Mrziglod, 20.5.2014
