"""uploading.py - class for B2FIND uploading : 
  - Uploader    uploads mapped MD JSON records into the B2FIND catalogue (CKAN instance)

Copyright (c) 2015 Heinrich Widmann (DKRZ)

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""

# from future
from __future__ import absolute_import
from __future__ import print_function
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

__author__ = "Heinrich Widmann"

# system relevant modules:
import os, glob, sys
import time, datetime, subprocess

# program relevant modules:
import logging
import traceback
import re

PY2 = sys.version_info[0] == 2

# needed for UPLOADER and CKAN class:
import simplejson as json
import hashlib
import collections
from b2handle.handleexceptions import HandleAuthenticationError,HandleNotFoundException,HandleSyntaxError
# needed for CKAN_CLIENT
import socket
if PY2:
    from urllib import quote
    from urllib2 import urlopen, Request
    from urllib2 import HTTPError,URLError
else:
    from urllib import parse
    from urllib.request import urlopen, Request
    from urllib.error import HTTPError,URLError

class CKAN_CLIENT(object):

    """
    ### CKAN_CLIENT - class
    # Provides methods to call a CKAN API request via urllib2
    #
    # Parameters:
    # -----------
    # (URL)     iphost  - URL to CKAN database
    # (string)  auth    - Authentication key for API requests
    #
    # Return Values:
    # --------------
    # 1. CKAN_CLIENT object
    #
    # Public Methods:
    # ---------------
    # .action (action, jsondata) - call the api <action>
    #
    # Usage:
    # ------

    # create CKAN object                       
    CKAN = CKAN_CLIENT(iphost,auth)

    # call action api:
    CKAN.action('package_create',{"name":"testdata", "title":"empty test object"})
    """

    def __init__ (self, ip_host, api_key):
	    self.ip_host = ip_host
	    self.api_key = api_key
	    self.logger = logging.getLogger('root')
	
    def validate_actionname(self,action):
        return True
	
	
    def action(self, action, data={}):
        ## action (action, jsondata) - method
	    # Call the api action <action> with the <jsondata> on the CKAN instance which was defined by iphost
	    # parameter of CKAN_CLIENT.
	    #
	    # Parameters:
	    # -----------
	    # (string)  action  - Action name of the API v3 of CKAN
	    # (dict)    data    - Dictionary with json data
	    #
	    # Return Values:
	    # --------------
	    # (dict)    response dictionary of CKAN
	    
	    if (not self.validate_actionname(action)):
		    logging.critical('Action name '+ str(action) +' is not defined in CKAN_CLIENT!')
	    else:
		    return self.__action_api(action, data)
		
    def __action_api (self, action, data_dict):
        # Make the HTTP request for data set generation.
        response=''
        rvalue = 0
        api_url = "http://{host}/api/rest".format(host=self.ip_host)
        action_url = "{apiurl}/dataset".format(apiurl=api_url)	# default for 'package_create'

        # "package_delete_all", "package_activate_all" and "member_create" are special actions
        # which are not supported by APIv3 of CKAN
        # special cases:
        if (action == "package_activate_all"):
            if data_dict['group']:
	            data = self.action('member_list',{"id" : data_dict['group'], "object_type":"package"})
            else:
	            data = self.action('package_list',{})

            print ('Total number of datasets: ' + str(len(data['result'])))
            for dataset in data['result']:
	            logging.info('\tTry to activate object: ' + str(dataset))
	            self.action('package_update',{"name" : dataset[0], "state":"active"})

            return True
        elif (action == "package_delete_all"):
            if (data_dict['group']):
                data = self.action('member_list',{"id" : data_dict['group'], "object_type":"package"})
            elif (data_dict['list']):
                data['result'] = data_dict['list']
            else:
                data = self.action('package_list',{})
            pcount = 0
            print ('Total number of datasets: ' + str(len(data['result'])))

            for dataset in data['result']:
                pcount += 1
                print('\tTry to delete object (' + str(pcount) + ' of ' + str(len(data['result'])) + '): ' + str(dataset))
                print ('\t', (self.action('package_update',{"name" : dataset[0], "state":"delete"}))['success'])

            return True
        elif (action == "member_create" or action == "organization_member_create"):
            api_url = "http://{host}/api/action".format(host=self.ip_host)
            action_url = "{apiurl}/{action}".format(apiurl=api_url,action=action)

            ds_id = data_dict['id']

            if (data_dict['id'] == None):
                            ds_id = (self.action('package_show',{"id" : data_dict['name']}))['id']

            member_dict = {
	            "id": data_dict['group'],
	            "object": ds_id,
	            "object_type": "package", 
	            "capacity" : "public"
            }

            data_dict	= member_dict
        # normal case:
        else:
            action_url = 'http://{host}/api/3/action/{action}'.format(host=self.ip_host,action=action)

        self.logger.debug(' CKAN request:\n |- Action\t%s\n |- RequestURL\t%s\n |- Data_dict\t%s' % (action,action_url,data_dict))	

        # make json data in conformity with URL standards
        encoding='utf-8'
        ##encoding='ISO-8859-15'
        try:
            if PY2 :
                data_string = quote(json.dumps(data_dict))##.encode("utf-8") ## HEW-D 160810 , encoding="latin-1" ))##HEW-D .decode(encoding)
            else :
                data_string = parse.quote(json.dumps(data_dict)).encode(encoding) ## HEW-D 160810 , encoding="latin-1" ))##HEW-D .decode(encoding)
        except Exception as err :
            logging.critical('%s while building url data' % err)

        try:
            request = Request(action_url,data_string)
            self.logger.debug('request %s' % request)            
            if (self.api_key): request.add_header('Authorization', self.api_key)
            self.logger.debug('api_key %s....' % self.api_key)
            if PY2 :
                response = urlopen(request)
            else :
                response = urlopen(request)
            self.logger.debug('response %s' % response)            
        except HTTPError as e:
            self.logger.warning('%s : The server %s couldn\'t fulfill the action %s.' % (e,self.ip_host,action))
            if ( e.code == 403 ):
                logging.error('Access forbidden, maybe the API key %s is not valid?' % self.api_key)
                exit(e.code)
            elif ( e.code == 409 and action == 'package_create'):
                self.logger.info('\tMaybe the dataset already exists => try to update the package')
                self.action('package_update',data_dict)
            elif ( e.code == 409):
                self.logger.debug('%s : \tMaybe you have a parameter error?')
                return {"success" : False}
            elif ( e.code == 500):
                self.logger.critical('%s : upload data : %s' % (e,data_dict))
                return {"success" : False}
        except URLError as e:
            self.logger.critical('\tURLError %s : %s' % (e,e.reason))
            return {"success" : False}
        except Exception as e:
            self.logger.critical('\t%s' % e)
            return {"success" : False}
        else :
            out = json.loads(response.read())
            self.logger.debug('out %s' % out)
            assert response.code >= 200
            return out


class Uploader(object):
    """
    ### Uploader - class
    # Uploads JSON files to CKAN portal and provides more methods for checking a dataset
    #
    # Parameters:
    # -----------
    # 1. (CKAN_CLIENT object)   CKAN - object of the CKAN_CLIENT class
    # 2. (OUT object)           OUT - object of the OUTPUT class
    #
    # Return Values:
    # --------------
    # 1. UPLOADER object
    #
    # Public Methods:
    # ---------------
    # .check_dataset(dsname,checksum)   - Compare the checksum of the dataset <dsname> with <checksum> 
    # .check_url(url)           - Checks and validates a url via urllib module
    # .delete(dsname,dsstatus)  - Deletes a dataset from a CKAN portal
    # .get_packages(community)  - Gets the details of all packages from a community in CKAN and store those in <UPLOADER.package_list>
    # .upload(dsname, dsstatus,
    #   community, jsondata)    - Uploads a dataset to a CKAN portal
    # .check(jsondata)       - Validates the fields in the <jsondata> by using B2FIND standard
    #
    # Usage:
    # ------

    # create UPLOADER object:
    UP = UPLOADER(CKAN,OUT)

    # VALIDATE JSON DATA
    if (not UP.check(jsondata)):
        print ("Dataset is broken or does not pass the B2FIND standard")

    # CHECK DATASET IN CKAN
    ckanstatus = UP.check_dataset(dsname,checksum)

    # UPLOAD DATASET TO CKAN
    upload = UP.upload(dsname,ckanstatus,community,jsondata)
    if (upload == 1):
        print ('Creation of record succeed'
    elif (upload == 2):
        print ('Update of record succeed'
    else:
        print ('Upload of record failed'
    """
    
    def __init__(self, CKAN, ckan_check, HandleClient,cred, OUT, base_outdir, fromdate, iphost):
        ##HEW-D logging = logging.getLogger()
        self.CKAN = CKAN
        self.ckan_check = ckan_check
        self.HandleClient = HandleClient
        self.cred = cred
        self.OUT = OUT
        self.logger = logging.getLogger('root')        
        self.base_outdir = base_outdir
        self.fromdate = fromdate
        self.iphost = iphost
        self.package_list = dict()

        # Read in B2FIND metadata schema and fields
        schemafile =  '%s/mapfiles/b2find_schema.json' % (os.getcwd())
        with open(schemafile, 'r') as f:
            self.b2findfields=json.loads(f.read())

        self.ckandeffields = ["author","title","notes","tags","url","version"]
        self.b2fckandeffields = ["Creator","Title","Description","Tags","Source","Checksum"]

    def purge_group(self,community):
        ## purge_list (UPLOADER object, community) - method
        # Purges a community (group) from CKAN 
        #
        # Parameters:
        # -----------
        # (string)  community - A B2FIND community / CKAN group
        #
        # Return Values:
        # --------------
        # None   
    
        pstart = time.time()
        self.logger.debug(' Remove all packages from and purge list %s ... ' % community)

        result = (self.CKAN.action('group_purge',{"id":community}))
        print ('result %s' % result)

        ptime = time.time() - pstart
        
        # save details:
        self.OUT.save_stats('#PurgeGroup','','time',ptime)
        
    def get_group_list(self,community):
        ## get_group_list (UPLOADER object, community) - method
        # Gets a full detailed list of all packages from a community (group) in CKAN (parameter <UPLOADER.CKAN>)
        #
        # Parameters:
        # -----------
        # (string)  community - A B2FIND community / CKAN group
        #
        # Return Values:
        # --------------
        # None   
    
        pstart = time.time()
        self.logger.debug(' Get all package names from community %s... ' % community)

        # get the full package list from a community in CKAN:
        query='"groups:'+community+'"'
        print ('query %s' % query)
        community_packages = (self.CKAN.action('package_search',{"q":query}))##['results']##['packages']
        print ('comm_packages %s' % community_packages)

        # create a new dictionary of it:
        package_list = dict() 
        for ds in community_packages:
            print ('ds %s' % ds)
            package_list[ds['name']] = ds['version']

        del community_packages
        self.package_list = package_list
        
        ptime = time.time() - pstart
        
        # save details:
        self.OUT.save_stats('#GetPackages','','time',ptime)
        self.OUT.save_stats('#GetPackages','','count',len(package_list))

    def get_packages(self,community):
        ## get_packages (UPLOADER object, community) - method
        # Gets a full detailed list of all packages from a community in CKAN (parameter <UPLOADER.CKAN>)
        #
        # Parameters:
        # -----------
        # (string)  community - A B2FIND community in CKAN
        #
        # Return Values:
        # --------------
        # None   
    
        pstart = time.time()
        self.logger.debug(' Get all package names from community %s... ' % community)

        # get the full package list from a community in CKAN:
        community_packages = (self.CKAN.action('group_show',{"id":community}))['result']['packages']

        # create a new dictionary of it:
        package_list = dict() 
        for ds in community_packages:
            package_list[ds['name']] = ds['version']

        del community_packages
        self.package_list = package_list
        
        ptime = time.time() - pstart
        
        # save details:
        self.OUT.save_stats('#GetPackages','','time',ptime)
        self.OUT.save_stats('#GetPackages','','count',len(package_list))

    def json2ckan(self, jsondata):
        ## json2ckan(UPLOADER object, json data) - method
        ##  converts flat JSON structure to CKAN JSON record with extra fields
        self.logger.debug(' Default fields:')
        for key in self.ckandeffields :
            if key not in jsondata or jsondata[key]=='':
                self.logger.warning('CKAN default key %s does not exist' % key)
            else:
                if key in  ["author"] :
                    jsondata[key]=';'.join(list(jsondata[key]))
                elif key in ["title","notes"] :
                    jsondata[key]='\n'.join([x for x in jsondata[key] if x is not None])
                self.logger.debug(' | %-15s | %-25s' % (key,jsondata[key]))
                if key in ["title","author","notes"] : ## Specific coding !!??
                    if jsondata['group'] in ['sdl'] :
                        try:
                            self.logger.info('Before encoding :\t%s:%s' % (key,jsondata[key]))
                            jsondata[key]=jsondata[key].encode("iso-8859-1") ## encode to display e.g. 'Umlauts' correctly 
                            self.logger.info('After encoding  :\t%s:%s' % (key,jsondata[key]))
                        except UnicodeEncodeError as e :
                            self.logger.error("%s : ( %s:%s[...] )" % (e,key,jsondata[key]))
                        except Exception as e:
                            self.logger.error('%s : ( %s:%s[...] )' % (e,key,jsondata[key[20]]))
                        finally:
                            pass
                        
        jsondata['extras']=list()
        extrafields=sorted(set(self.b2findfields.keys()) - set(self.b2fckandeffields))
        self.logger.debug(' CKAN extra fields')
        for key in extrafields :
            if key in jsondata :
                if key in ['Contact','Format','Language','Publisher','PublicationYear','Checksum', 'Rights','ResourceType']:
                    value=';'.join(jsondata[key])
                elif key in ['oai_identifier']:
                    if isinstance(jsondata[key],list) or isinstance(jsondata[key],set) : 
                        value=jsondata[key][-1]      
                else:
                    value=jsondata[key]
                jsondata['extras'].append({
                     "key" : key,
                     "value" : value
                })
                del jsondata[key]
                self.logger.debug(' | %-15s | %-25s' % (key,value))
            else:
                self.logger.debug(' | %-15s | %-25s' % (key,'-- No data available'))

        return jsondata

    def check(self, jsondata):
        ## check(UPLOADER object, json data) - method
        # Checks the jsondata and returns the correct ones
        #
        # Parameters:
        # -----------
        # 1. (dict)    jsondata - json dictionary with metadata fields with B2FIND standard
        #
        # Return Values:
        # --------------
        # 1. (dict)   
        # Raise errors:
        # -------------
        #               0 - critical error occured
        #               1 - non-critical error occured
        #               2 - no error occured    
    
        errmsg = ''
        
        ## check ds name (must be lowercase, alphanumeric + ['_-']
        if not re.match("^[a-z0-9_-]*$", jsondata['name']):
            self.logger.error("The dataset name '%s' must be lowercase and alphanumeric + ['_-']" % jsondata['name'])
            jsondata['name']=jsondata['name'].lower()
            self.logger.error(" ... and is converted now to '%s'" % jsondata['name'])
        ## check mandatory fields ...
        mandFields=['title','oai_identifier']
        for field in mandFields :
            if field not in jsondata: ##  or jsondata[field] == ''):
                self.logger.critical("The mandatory field '%s' is missing" % field)
                return None

        identFields=['DOI','PID','url']
        identFlag=False
        for field in identFields :
            if field in jsondata:
                identFlag=True
        if identFlag == False:
            self.logger.critical("At least one identifier from %s is mandatory" % identFields)
            return None
            
        if 'PublicationYear' in jsondata :
            try:
                datetime.datetime.strptime(jsondata['PublicationYear'][0], '%Y')
            except (ValueError,TypeError) as e:
                self.logger.debug("%s : Facet %s must be in format YYYY, given valueis : %s" % (e,'PublicationYear',jsondata['PublicationYear']))
                ##HEW-D raise Exception("Error %s : Key %s value %s has incorrect data format, should be YYYY" % (e,'PublicationYear',jsondata['PublicationYear']))
                # delete this field from the jsondata:
                del jsondata['PublicationYear']
                
        # check Date-Times for consistency with UTC format
        dt_keys=['PublicationTimestamp', 'TemporalCoverage:BeginDate', 'TemporalCoverage:EndDate']
        for key in dt_keys:
            if key in jsondata :
                try:
                    datetime.datetime.strptime(jsondata[key], '%Y-%m-%d'+'T'+'%H:%M:%S'+'Z')
                except ValueError:
                    self.logger.error("Value %s of key %s has incorrect data format, should be YYYY-MM-DDThh:mm:ssZ" % (jsondata[key],key))
                    del jsondata[key] # delete this field from the jsondata
                except TypeError:
                    self.logger.error("Value %s of key %s has incorrect type, must be string YYYY-MM-DDThh:mm:ssZ" % (jsondata[key],key))
                    del jsondata[key] # delete this field from the jsondata

        return jsondata

    def upload(self, request):
        ## upload(UPLOADER object, request) - method
        #  uploads a JSON dataset to a B2FIND instance (CKAN). 

        results = collections.defaultdict(int)

        # set processing parameters
        community=request[0]
        source=request[1]
        mdprefix=request[3]
        mdsubset=request[4]   if len(request)>4 else None
        target_mdschema=request[8]   if len(request)>8 else None

        mdschemasfile='%s/mapfiles/mdschemas.json' % (os.getcwd())
        with open(mdschemasfile, 'r') as f:
            mdschemas=json.loads(f.read())

        # available of sub dirs and extention
        insubdir='/json'
        infformat='json'

        # read target_mdschema (degfault : B2FIND_schema) and set mapfile
        if (target_mdschema and not target_mdschema.startswith('#')):
            print('target_mdschema %s' % target_mdschema)

        # community-mdschema root path
        cmpath='%s/%s-%s/' % (self.base_outdir,community,mdprefix)
        self.logger.info('\t|- Input path:\t%s' % cmpath)
        subdirs=next(os.walk(cmpath))[1] ### [x[0] for x in os.walk(cmpath)]
        fcount=0 # total counter of processed files
        subsettag=re.compile(r'_\d+')
        start = time.time()

        # loop over all available subdirs
        for subdir in sorted(subdirs) :
            if mdsubset and not subdir.startswith(mdsubset) :
                self.logger.warning('\t |- Subdirectory %s does not match %s - no processing required' % (subdir,mdsubset))
                continue
            elif self.fromdate :
                datematch = re.search(r'\d{4}-\d{2}-\d{2}$', subdir[:-2])
                if datematch :
                    subdirdate = datetime.datetime.strptime(datematch.group(), '%Y-%m-%d').date()
                    fromdate = datetime.datetime.strptime(self.fromdate, '%Y-%m-%d').date()
                    if (fromdate > subdirdate) :
                        self.logger.warning('\t |- Subdirectory %s has timestamp older than fromdate %s - no processing required' % (subdir,self.fromdate))
                        continue
                    else :
                        self.logger.warning('\t |- Subdirectory %s with timestamp newer than fromdate %s is processed' % (subdir,self.fromdate))
                else:
                    self.logger.warning('\t |- Subdirectory %s does not contain a timestamp %%Y-%%m-%%d  - no processing required' % subdir)
                    continue    
            else:
                print('\t |- Subdirectory %s is processed' % subdir)
                self.logger.debug('Processing of subdirectory %s' % subdir)

            # check input path
            inpath='%s/%s/%s' % (cmpath,subdir,insubdir)
            if not os.path.exists(inpath):
                self.logger.critical('Can not access directory %s' % inpath)
                return results     

            files = list(filter(lambda x: x.endswith(infformat), os.listdir(inpath)))
            results['tcount'] += len(list(files))
            oldperc=0
            err = None
            self.logger.debug(' |- Processing of %s files in %s' % (infformat.upper(),inpath))
       
            ## start processing loop
            startsubdir = time.time()
            scount = 0
            fcount=0 # counter per sub dir !
            for filename in files:
                ## counter and progress bar
                fcount+=1
                if (fcount<scount): continue
                perc=int(fcount*100/int(len(list(files))))
                bartags=int(perc/5)
                if perc%10 == 0 and perc != oldperc:
                    oldperc=perc
                    print ("\r\t [%-20s] %5d (%3d%%) in %d sec" % ('='*bartags, fcount, perc, time.time()-startsubdir ))
                    sys.stdout.flush()
                self.logger.debug('    | m | %-4d | %-45s |' % (fcount,filename))

                jsondata = dict()
                datasetRecord = dict()

                pathfname= inpath+'/'+filename
                if ( os.path.getsize(pathfname) > 0 ):
                    with open(pathfname, 'r') as f:
                        try:
                            jsondata=json.loads(f.read(),encoding = 'utf-8')
                        except:
                            self.logger.error('    | [ERROR] Cannot load the json file %s' % pathfname)
                            results['ecount'] += 1
                            continue
                else:
                    results['ecount'] += 1
                    continue

                # get dataset id (CKAN name) from filename (a uuid generated identifier):
                ds_id = os.path.splitext(filename)[0]
                self.logger.warning('    | u | %-4d | %-40s |' % (fcount,ds_id))
 
               # add some general CKAN specific fields to dictionary:
                jsondata["name"] = ds_id
                jsondata["state"]='active'
                jsondata["groups"]=[{ "name" : community }]
                if self.iphost.startswith('eudat-b1') :
                    jsondata["owner_org"]="eudat"
                else:
                    jsondata["owner_org"]="eudat-b2find"

                # get OAI identifier from json data extra field 'oai_identifier':
                if 'oai_identifier' not in jsondata :
                    jsondata['oai_identifier'] = [ds_id]

                oai_id = jsondata['oai_identifier'][0]
                self.logger.debug("        |-> identifier: %s\n" % (oai_id))
            
                ### CHECK JSON DATA for upload
                jsondata=self.check(jsondata)
                if jsondata == None :
                    self.logger.critical('File %s failed check and will not been uploaded' % filename)
                    continue

                #  generate get record request for field MetaDataAccess:
                if (mdprefix == 'json'):
                    reqpre = source + '/dataset/'
                    mdaccess = reqpre + oai_id
                else:
                    reqpre = source + '?verb=GetRecord&metadataPrefix=' + mdprefix
                    mdaccess = reqpre + '&identifier=' + oai_id
                    ##HEW-MV2mapping!!! : urlcheck=self.check_url(mdaccess)
                index1 = mdaccess

                # exceptions for some communities:
                if (community == 'clarin' and oai_id.startswith('mi_')):
                    mdaccess = 'http://www.meertens.knaw.nl/oai/oai_server.php?verb=GetRecord&metadataPrefix=cmdi&identifier=http://hdl.handle.net/10744/' + oai_id
                elif (community == 'sdl'):
                    mdaccess =reqpre+'&identifier=oai::record/'+oai_id
                elif (community == 'b2share'):
                    if mdsubset.startswith('trng') :
                        mdaccess ='https://trng-b2share.eudat.eu/api/oai2d?verb=GetRecord&metadataPrefix=marcxml&identifier='+oai_id
                    else:
                        mdaccess ='https://b2share.eudat.eu/api/oai2d?verb=GetRecord&metadataPrefix=marcxml&identifier='+oai_id

                if self.check_url(mdaccess) == False :
                    logging.debug('URL to metadata record %s is broken' % (mdaccess))
                else:
                    jsondata['MetaDataAccess']=mdaccess

                jsondata['group']=community
                ## Prepare jsondata for upload to CKAN (decode UTF-8, build CKAN extra dict's, ...)
                jsondata=self.json2ckan(jsondata)

                # Set the tag ManagerVersion:
                ManagerVersion = '2.3.1' ##HEW-??? Gloaal Variable ManagerVersion
                jsondata['extras'].append({
                     "key" : "ManagerVersion",
                     "value" : '2.3.1' ##HEW-??? Gloaal Variable ManagerVersionManagerVersion
                    })
                datasetRecord["EUDAT/B2FINDVERSION"]=ManagerVersion
                ### datasetRecord["B2FINDHOST"]=self.iphost

                self.logger.debug(' JSON dump\n%s' % json.dumps(jsondata, sort_keys=True))

                # determine checksum of json record and append
                try:
                    encoding='utf-8' ##HEW-D 'ISO-8859-15' / 'latin-1'
                    checksum=hashlib.md5(json.dumps(jsondata, sort_keys=True).encode('latin1')).hexdigest()
                except UnicodeEncodeError as err :
                    self.logger.critical(' %s during md checksum determination' % err)
                    checksum=None
                else:
                    self.logger.debug('Checksum of JSON record %s' % checksum)
                    jsondata['version'] = checksum
                    datasetRecord["CHECKSUM"]=checksum            

                ### check status of dataset (unknown/new/changed/unchanged)
                dsstatus="unknown"

                # check against handle server
                handlestatus="unknown"
                pidRecord=dict()
                b2findds='http://b2find.eudat.eu/dataset/'+ds_id
                ckands='http://'+self.iphost+'/dataset/'+ds_id
                datasetRecord["URL"]=b2findds
                datasetRecord["EUDAT/ROR"]=ckands
                datasetRecord["EUDAT/PPID"]=''
                datasetRecord["EUDAT/REPLICA"]=''
                datasetRecord["EUDAT/METADATATYPE"]=mdschemas[mdprefix]
                datasetRecord["EUDAT/B2FINDSTATUS"]="REGISTERED"
                datasetRecord["EUDAT/B2FINDCOMMUNITY"]=community
                datasetRecord["EUDAT/B2FINDSUBSET"]=mdsubset

                if (self.cred): ##HEW-D??? options.handle_check):
                    pidAttrs=["URL","CHECKSUM","EUDAT/ROR","EUDAT/PPID","EUDAT/REPLICA","EUDAT/METADATATYPE","EUDAT/B2FINDSTATUS","EUDAT/B2FINDVERSION","EUDAT/B2FINDCOMMUNITY","EUDAT/B2FINDSUBSET"]
                    ##HEW-D pidAttrs=["URL","CHECKSUM","JMDVERSION","B2FINDHOST","IS_METADATA","MD_STATUS","MD_SCHEMA","COMMUNITY","SUBSET"]
                    try:
                        pid = self.cred.get_prefix() + '/eudat-jmd_' + ds_id 
                        rec = self.HandleClient.retrieve_handle_record_json(pid)
                    except Exception as err :
                        self.logger.error("%s in self.HandleClient.retrieve_handle_record_json(%s)" % (err,pid))
                    else:
                        self.logger.debug("Retrieved PID %s" % pid )

                    chargs={}
                    if rec : ## Handle exists
                        for pidAttr in pidAttrs :##HEW-D ["CHECKSUM","JMDVERSION","B2FINDHOST"] : 
                            try:
                                pidRecord[pidAttr] = self.HandleClient.get_value_from_handle(pid,pidAttr,rec)
                            except Exception as err:
                                self.logger.critical("%s in self.HandleClient.get_value_from_handle(%s)" % (err,pidAttr) )
                            else:
                                self.logger.debug("Got value %s from attribute %s sucessfully" % (pidRecord[pidAttr],pidAttr))

                            if ( pidRecord[pidAttr] == datasetRecord[pidAttr] ) :
                                chmsg="-- not changed --"
                                if pidAttr == 'CHECKSUM' :
                                    handlestatus="unchanged"
                                self.logger.info(" |%-12s\t|%-30s\t|%-30s|" % (pidAttr,pidRecord[pidAttr],chmsg))
                            else:
                                chmsg=datasetRecord[pidAttr]
                                handlestatus="changed"
                                chargs[pidAttr]=datasetRecord[pidAttr] 
                                self.logger.info(" |%-12s\t|%-30s\t|%-30s|" % (pidAttr,pidRecord[pidAttr],chmsg))
                    else:
                        handlestatus="new"
                    dsstatus=handlestatus

                    if handlestatus == "unchanged" : # no action required :-) !
                        self.logger.warning('No action required :-) - next record')
                        results['ncount']+=1
                        continue
                    elif handlestatus == "changed" : # update dataset !
                        self.logger.warning('Update handle and dataset !')
                    else : # create new handle !
                        self.logger.warning('Create handle and dataset !')
                        chargs=datasetRecord 

                # check against CKAN database
                ckanstatus = 'unknown'                  
                if (self.ckan_check == 'True'):
                    ckanstatus=self.check_dataset(ds_id,checksum)
                    if (dsstatus == 'unknown'):
                        dsstatus = ckanstatus

                upload = 0

                # if the dataset checked as 'new' so it is not in ckan package_list then create it with package_create:
                if (dsstatus == 'new' or dsstatus == 'unknown') :
                    self.logger.debug('\t - Try to create dataset %s' % ds_id)
            
                    res = self.CKAN.action('package_create',jsondata)
                    if (res and res['success']):
                        self.logger.warning("Successful creation of %s dataset %s" % (dsstatus,ds_id)) 
                        results['count']+=1
                        upload = 1
                    else:
                        self.logger.error('\t - Creation failed. Try to update instead.')
                        res = self.CKAN.action('package_update',jsondata)
                        if (res and res['success']):
                            self.logger.warning("Successful update of %s dataset %s" % (dsstatus,ds_id)) 
                            results['count']+=1
                            upload = 1
                        else:
                            self.logger.critical('\t|- Failed dataset update of %s id %s' % (dsstatus,ds_id))
                            results['ecount']+=1
        
                # if the dsstatus is 'changed' then update it with package_update:
                elif (dsstatus == 'changed'):
                    self.logger.debug('\t - Try to update dataset %s' % ds_id)
            
                    res = self.CKAN.action('package_update',jsondata)
                    if (res and res['success']):
                        self.logger.warning("Successful update of %s dataset %s" % (dsstatus,ds_id)) 
                        results['count']+=1
                        upload = 1
                    else:
                        self.logger.warning('\t - Update failed. Try to create instead.')
                        res = self.CKAN.action('package_create',jsondata)
                        if (res and res['success']):
                            self.logger.warning("Successful creation of %s dataset %s" % (dsstatus,ds_id)) 
                            results['count']+=1
                            upload = 1
                        else:
                            self.logger.warning('\t|- Failed dataset creation of %s id %s' % (dsstatus,ds_id))


                # update PID in handle server                           
                if (self.cred):
                    if (handlestatus == "unchanged"):
                        logging.warning("        |-> No action required for %s" % pid)
                    else:
                        if (upload >= 1): # new or changed record
                            if (handlestatus == "new"): # Create new PID
                                logging.warning("        |-> Create a new handle %s with checksum %s" % (pid,checksum))
                                try:
                                    npid = self.HandleClient.register_handle(pid, datasetRecord["URL"], datasetRecord["CHECKSUM"], None, True )
                                except (Exception,HandleAuthenticationError,HandleSyntaxError) as err :
                                    self.logger.warning("Registration failed of handle %s with checksum %s" % (pid,datasetRecord["CHECKSUM"]))
                                    self.logger.critical("%s in HandleClient.register_handle" % err )
                                    sys.exit()
                                else:
                                    self.logger.warning("Successful registration of handle %s with checksum %s" % (pid,datasetRecord["CHECKSUM"]))

                            ## Modify all changed handle attributes
                            if chargs :
                                try:
                                    self.HandleClient.modify_handle_value(pid,**chargs) ## ,URL=dataset_dict["URL"]) 
                                    self.logger.warning("        |-> Update handle %s with changed atrributes %s" % (pid,chargs))

                                except (Exception,HandleAuthenticationError,HandleNotFoundException,HandleSyntaxError) as err :
                                    self.logger.warning("Change failed of handle %s with checksum %s" % (pid,datasetRecord["CHECKSUM"]))

                                    self.logger.critical("%s in HandleClient.modify_handle_value of %s in %s" % (err,chargs,pid))
                                else:
                                    self.logger.warning("Successful change of handle %s with checksum %s" % (pid,datasetRecord["CHECKSUM"]))
                                    self.logger.debug(" Attributes %s of handle %s changed sucessfully" % (chargs,pid))
            
        uploadtime=time.time()-start
        results['time'] = uploadtime
        print ('   \n\t|- %-10s |@ %-10s |\n\t| Provided | Uploaded | No action | Failed |\n\t| %8d | %6d |  %8d | %6d |' % ( 'Finished',time.strftime("%H:%M:%S"),
                    results['tcount'],
                    results['count'],
                    results['ncount'],
                    results['ecount']
               ))

        return results

    def bulk_upload(self, ds, dsstatus, community, jsondata):
        ## bulk_upload (UPLOADER object, dsname, dsstatus, community, jsondata) - method
        # Buld upload using ckanapi an jsonline file or a set of jsonline files
        #   <dsstatus> describes the state of upload ...
        #
        # Parameters:
        # ----------- ????!!!!
        # 1. (string)   dsname - Name of the dataset
        # 2. (string)   dsstatus - Status of the dataset: can be 'new', 'changed', 'unchanged' or 'unknown'.
        #                           See also .check_dataset()
        # 3. (string)   dsname - A B2FIND community in CKAN
        # 4. (dict)     jsondata - Metadata fields of the dataset in JSON format
        # ^^^^^^^^^^^^^ ????!!!! ^^^^^^^^^^^^^^^
        # Return Values:
        # --------------
        # 1. (integer)  upload result:
        #               0 - critical error occured
        #               1 - no error occured, uploaded with 'package_create'
        #               2 - no error occured, uploaded with 'package_update'
    
        # add some CKAN specific fields to dictionary:
        jsondata["name"] = ds
        jsondata["state"]='active'
        jsondata["groups"]=[{ "name" : community }]
        jsondata["owner_org"]="eudat"
   
        # if the dataset checked as 'new' so it is not in ckan package_list then create it with package_create:
        if (dsstatus == 'new' or dsstatus == 'unknown') :
            self.logger.debug('\t - Try to create dataset %s' % ds)
            
            results = self.CKAN.action('package_create',jsondata)
            if (results and results['success']):
                rvalue = 1
            else:
                self.logger.debug('\t - Creation failed. Try to update instead.')
                results = self.CKAN.action('package_update',jsondata)
                if (results and results['success']):
                    rvalue = 2
                else:
                    rvalue = 0
        
        # if the dsstatus is 'changed' then update it with package_update:
        elif (dsstatus == 'changed'):
            self.logger.debug('\t - Try to update dataset %s' % ds)
            
            results = self.CKAN.action('package_update',jsondata)
            if (results and results['success']):
                rvalue = 2
            else:
                self.logger.debug('\t - Update failed. Try to create instead.')
                results = self.CKAN.action('package_create',jsondata)
                if (results and results['success']):
                    rvalue = 1
                else:
                    rvalue = 0
           
        return rvalue


    def delete(self, dsname, dsstatus):
        ## delete (UPLOADER object, dsname, dsstatus) - method
        # Deletes the dataset <dsname> from CKAN portal if its <dsstatus> is not 'new'
        #
        # Parameters:
        # -----------
        # 1. (string)  dsname - Name of the dataset
        # 2. (string)  dsstatus - State of the dataset (can be 'new', 'changed', 'unchanged' or 'unknown')
        #
        # Return Values:
        # --------------
        # 1. (integer)  deletion result:
        #               0 - error occured, dataset was not deleted
        #               1 - no error occured, dataset is deleted
    
        rvalue = 0
        jsondata = {
            "name" : dsname,
            "state" : 'deleted'
        }
   
        jsondatadel = {
            "id" : dsname
        }
   
        # if the dataset exists set it to status deleted in CKAN:
        if (not dsstatus == 'new'):
            self.logger.debug('\t - Try to set dataset %s on state "deleted"' % dsname)
            results = self.CKAN.action('package_update',jsondata)
            if (results and results['success']):
                rvalue = 1
                self.logger.debug('\t - Successful update of state to "deleted" of dataset %s .' % dsname)
            else:
                self.logger.debug('\t - Failed update of state to "deleted" of dataset %s .' % dsname)

            self.logger.debug('\t - Try to delete dataset %s ' % dsname)
##
            results = self.CKAN.action('package_delete',jsondatadel)
##HEW-??            results = self.CKAN.action('dataset_purge',jsondatadel)
            if (results and results['success']):
                rvalue = 1
                self.logger.debug('\t - Successful deletion of dataset %s.' % dsname)
            else:
                self.logger.debug('\t - Failed deletion of dataset %s.' % dsname)
        
        return rvalue
    
    
    def check_dataset(self,dsname,checksum):
        ## check_dataset (UPLOADER object, dsname, checksum) - method
        # Compare the checksum of <dsname> in CKAN portal with the given <checksum>. If they are equal 'unchanged'
        # will be returned. 
        # Otherwise returns 'new', 'changed' or 'unknown' if check failed.
        #
        # Parameters:
        # -----------
        # (string)  dsname - Name of the dataset
        #
        # Return Values:
        # --------------
        # 1. (string)  ckanstatus, can be:
        #               1. 'unknown'
        #               2. 'new'
        #               3. 'unchanged'
        #               4. 'changed'
    
        ckanstatus='unknown'
        if not (dsname in self.package_list):
            ckanstatus="new"
        else:
            if ( checksum == self.package_list[dsname]):
                ckanstatus="unchanged"
            else:
                ckanstatus="changed"
        return ckanstatus
    
    
    def check_url(self,url):
        ## check_url (UPLOADER object, url) - method
        # Checks and validates a url via urllib module
        #
        # Parameters:
        # -----------
        # (url)  url - Url to check
        #
        # Return Values:
        # --------------
        # 1. (boolean)  result
    
        try:
            resp = urlopen(url, timeout=10).getcode()###HEW-!! < 501
        except (HTTPError,ValueError) as err:
            self.logger.error('%s in check_url of %s' % (err,url))
            return False
        except URLError as err: ## HEW : stupid workaraound for SSL: CERTIFICATE_VERIFY_FAILED]
            self.logger.error('%s in check_url of %s' % (err,url))
            if str(err.reason).startswith('[SSL: CERTIFICATE_VERIFY_FAILED]') :
                return Warning
            else :
                return False
        else:
            return True
