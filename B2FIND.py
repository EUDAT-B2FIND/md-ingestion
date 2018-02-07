"""B2FIND.py - classes for B2FIND management : 
  - CKAN_CLIENT  executes CKAN APIs (interface to CKAN)
  - HARVESTER    harvests from a OAI-PMH server
  - MAPPER       converts XML files to JSON files and performs semantic mapping
  - VALIDATER    validates JSON records against the B2FIND MD schema
  - UPLOADER     uploads JSON files to CKAN portal
  - OAICONVERTER converts JSON fields to XML files to provide via OAI-PMH in B2FIND schema
  - OUTPUT       initializes the logger class and provides methods for saving log data and for printing those.    

Install required modules as sickle, lxml, simplejson etc. , e.g. by :
  > sudo pip install <module>
or at once by
  > sudo pip install -r requirements.txt

Copyright (c) 2014 John Mrziglod (DKRZ)
Modified      2015 Heinrich Widmann (DKRZ)

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

# system relevant modules:
import os, glob, sys
import time, datetime, subprocess

# program relevant modules:
import logging
### logger = logging.getLogger('root')
import traceback
import re

__author__ = "Heinrich Widmann"

PY2 = sys.version_info[0] == 2

# needed for HARVESTER class:
from sickle import Sickle
from sickle.oaiexceptions import NoRecordsMatch,CannotDisseminateFormat
from owslib.csw import CatalogueServiceWeb
from SPARQLWrapper import SPARQLWrapper, JSON
from requests.exceptions import ConnectionError
import uuid
import lxml.etree as etree
import xml.etree.ElementTree as ET
from itertools import tee 
import collections
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

# needed for MAPPER :
import codecs
import simplejson as json
import io
from pyparsing import *
import Levenshtein as lvs
import iso639
##import enchant

# needed for UPLOADER and CKAN class:
from collections import OrderedDict
import hashlib
from b2handle.handleexceptions import HandleAuthenticationError,HandleNotFoundException,HandleSyntaxError
from string import Template

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
                logging.error('Access forbidden, maybe the API key is not valid?')
                exit(e.code)
            elif ( e.code == 409 and action == 'package_create'):
                self.logger.info('\tMaybe the dataset already exists => try to update the package')
                self.action('package_update',data_dict)
            elif ( e.code == 409):
                self.logger.debug('\tMaybe you have a parameter error?')
                return {"success" : False}
            elif ( e.code == 500):
                self.logger.critical('\tInternal server error')
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

class HARVESTER(object):
    
    """
    ### HARVESTER - class
    # Provides methods to harvest metadata from external data providers
    #
    # create HARVESTER object                       
    HV = HARVESTER(OUT object, outdir,fromdate)
    """
    
    def __init__ (self, OUT, pstat, base_outdir, fromdate):
        self.logger = logging.getLogger('root')
        self.pstat = pstat
        self.OUT = OUT
        self.base_outdir = base_outdir
        self.fromdate = fromdate
        
    
    def harvest(self, request):
        ## harvest (HARVESTER object, request = [community, source, verb, mdprefix, mdsubset])
        # Harvest all files with <mdprefix> and <mdsubset> from <source> via sickle module and store those to hard drive.
        #
        # Parameters:
        # -----------
        # (list)  request - A list with following items:
        #                    1. community
        #                    2. source (OAI URL)
        #                    3. verb (ListIdentifiers, ListRecords or JSONAPI)
        #                    4. mdprefix (OAI md format as oai_dc, iso etc.)
        #                    5. mdsubset
        #
        # Return Values:
        # --------------
        # 1. (integer)  is -1 if something went wrong    
    
    
        # create a request dictionary:
        req = {
            "community" : request[0],
            "url"   : request[1],
            "lverb" : request[2],
            "mdprefix"  : request[3],
            "mdsubset"  : request[4]   if len(request)>4 else None
        }
   
        # create dictionary with stats:
        resKeys=['count','tcount','ecount','time']
        results = dict.fromkeys(resKeys,0)

        stats = {
            "tottcount" : 0,    # total number of provided datasets
            "totcount"  : 0,    # total number of successful harvested datasets
            "totecount" : 0,    # total number of failed datasets
            "totdcount" : 0,    # total number of all deleted datasets
            "tcount"    : 0,    # number of all provided datasets per subset
            "count"     : 0,    # number of all successful harvested datasets per subset
            "ecount"    : 0,    # number of all failed datasets per subset
            "dcount"    : 0,    # number of all deleted datasets per subset
            
            "timestart" : time.time(),  # start time per subset process
        }
        
        # the gbif api client
        class GBIF_CLIENT(object):
        
            # call action api:
            ## GBIF.action('package_list',{})
        
            def __init__ (self, api_url): ##, api_key):
        	    self.api_url = api_url
                    self.logger = logging.getLogger('root')
     
            def JSONAPI(self, action, offset, chunklen, key):
                ## JSONAPI (action) - method
                return self.__action_api(action, offset, chunklen, key)

            def __action_api (self, action, offset, chunklen, key):
                # Make the HTTP request for get datasets from GBIF portal
                response=''
                rvalue = 0
                ## offset = 0
                limit=chunklen ## None for DataCite-JSON !!!
                api_url = self.api_url
                if key :
                    action_url = "{apiurl}/{action}/{key}".format(apiurl=api_url,action=action,key=str(key))
                elif offset == None :
                    action_url = "{apiurl}/{action}".format(apiurl=api_url,action=action)	
                else :
                    action_url = "{apiurl}/{action}?offset={offset}&limit={limit}".format(apiurl=api_url,action=action,offset=str(offset),limit=str(limit))	

                self.logger.debug('action_url: %s' % action_url)
                try:
                    request = Request(action_url)
                    response = urlopen(request)
                except HTTPError as e:
                   self.logger.error('%s : The server %s couldn\'t fulfill the action %s.' % (e.code,self.api_url,action))
                   if ( e.code == 403 ):
                       self.logger.critical('Access forbidden, maybe the API key is not valid?')
                       exit(e.code)
                   elif ( e.code == 409):
                       self.logger.critical('Maybe you have a parameter error?')
                       return {"success" : False}
                   elif ( e.code == 500):
                       self.logger.critical('Internal server error')
                       exit(e.code)
                except URLError as e:
                   exit('%s' % e.reason)
                else :
                   out = json.loads(response.read())
                   assert response.code >= 200
                   return out        

        requests_log = logging.getLogger("requests")
        requests_log.setLevel(logging.WARNING)
        
        # if the number of files in a subset dir is greater than <count_break>
        # then create a new one with the name <set> + '_' + <count_set>
        count_break = 5000
        count_set = 1
        start=time.time()

        # set subset:
        mdsubset=req["mdsubset"]
        if (not mdsubset):
            subset = 'SET'
        elif mdsubset.endswith('_'): # no OAI subsets, but different OAI-URLs for same community
            subset = mdsubset[:-1]
            mdsubset=None
        elif len(mdsubset) > 2 and mdsubset[-1].isdigit() and  mdsubset[-2] == '_' :
            subset = mdsubset[:-2]
        else:
            subset = mdsubset
            if req["community"] == "b2share" or re.match(r'http(.*?)b2share(.*?)api(.*?)',req["url"]) :
                setMapFile= '%s/mapfiles/b2share_mapset.json' % (os.getcwd())
            elif req["community"] == "dara" and req["url"] == "https://www.da-ra.de/oaip/oai" :
                setMapFile= '%s/mapfiles/dara_mapset.json' % (os.getcwd())
            else:
                setMapFile=None
            if setMapFile :
                with open(setMapFile) as sm :    
                    setMap = json.load(sm)
                    if mdsubset in setMap:
                        mdsubset = setMap[mdsubset]
            
        if (self.fromdate):
            subset = subset + '_f' + self.fromdate

        self.logger.debug(' |- Subset:    \t%s' % subset )

        # make subset dir:
        subsetdir = '/'.join([self.base_outdir,req['community']+'-'+req['mdprefix'],subset+'_'+str(count_set)])

        noffs=0 # set to number of record, where harvesting should start
        stats['tcount']=noffs
        fcount=0
        oldperc=0
        ntotrecs=0
        choffset=0
        chunklen=1000
        pageno=1
        records=list()

        ## JSON-API
        jsonapi_verbs=['dataset','works','records']
        if req["lverb"] in jsonapi_verbs :
            GBIF = GBIF_CLIENT(req['url'])   # create GBIF object   
            harvestreq=getattr(GBIF,'JSONAPI', None)
            outtypedir='hjson'
            outtypeext='json'
            if mdsubset and req["lverb"] == 'works' :
                haction='works?publisher-id='+mdsubset
                dresultkey='data'
            elif req["lverb"] == 'records' :
                haction=req["lverb"]
                if mdsubset :
                    haction+='?q=community:'+mdsubset+'&size='+str(chunklen)+'&page='+str(pageno)
                dresultkey='hits'
            else:
                haction=req["lverb"]
                dresultkey='results'
            try:
                chunk=harvestreq(**{'action':haction,'offset':None,'chunklen':chunklen,'key':None})
                self.logger.debug(" Got first %d records : chunk['data'] %s " % (chunklen,chunk[dresultkey]))
            except (HTTPError,ConnectionError,Exception) as e:
                self.logger.critical("%s :\n\thaction %s\n\tharvest request %s\n" % (e,haction,req))
                return -1

            if req["lverb"] == 'dataset':
                while('endOfRecords' in chunk and not chunk['endOfRecords']):
                    if 'results' in chunk :
                        records.extend(chunk['results'])
                    choffset+=chunklen
                    chunk =harvestreq(**{'action':haction,'offset':choffset,'chunklen':chunklen,'key':None})
                    self.logger.debug(" Got next records [%d,%d] from chunk %s " % (choffset,choffset+chunklen,chunk))
            elif req["lverb"] == 'records':
                records.extend(chunk['hits']['hits'])
                while('hits' in chunk and 'next' in chunk['links']):
                    if 'hits' in chunk :
                        records.extend(chunk['hits']['hits'])
                    pageno+=1
                    chunk =harvestreq(**{'action':haction,'page':pageno,'size':chunklen,'key':None})
                    self.logger.debug(" Got next records [%d,%d] from chunk %s " % (choffset,choffset+chunklen,chunk))
            else:
                if 'data' in chunk :
                    records.extend(chunk['data'])
                    
        # OAI-PMH (verb = ListRecords/Identifier )
        elif req["lverb"].startswith('List'):
            sickle = Sickle(req['url'], max_retries=3, timeout=300)
            outtypedir='xml'
            outtypeext='xml'
            harvestreq=getattr(sickle,req["lverb"], None)
            try:
                records,rc=tee(harvestreq(**{'metadataPrefix':req['mdprefix'],'set':mdsubset,'ignore_deleted':True,'from':self.fromdate}))
            except (HTTPError,ConnectionError) as err:
                self.logger.critical("%s during connecting to %s\n" % (err,req['url']))
                return -1
            except (ImportError,etree.XMLSyntaxError,CannotDisseminateFormat,Exception) as err:
                self.logger.critical("%s during harvest request %s\n" % (err,req))
                return -1

        # CSW2.0
        elif req["lverb"].startswith('csw'):
            outtypedir='xml'
            outtypeext='xml'
            startposition=0
            maxrecords=1000
            try:
                src = CatalogueServiceWeb(req['url'])
                harvestreq=getattr(src,'getrecords2')
                harvestreq(**{'esn':'full','outputschema':'http://www.isotc211.org/2005/gmd','startposition':startposition,'maxrecords':maxrecords})
                records=list(src.records.itervalues())
            except (HTTPError,ConnectionError) as err:
                self.logger.critical("%s during connecting to %s\n" % (err,req['url']))
                return -1
            except (ImportError,CannotDisseminateFormat,Exception) as err:
                self.logger.critical("%s during harvest request %s\n" % (err,req))
                return -1

        # SparQL
        elif req["lverb"].startswith('Sparql'):
            outtypedir='hjson'
            outtypeext='json'
            startposition=0
            maxrecords=1000
            try:
                src = SPARQLWrapper(req['url'])
                harvestreq=getattr(src,'query','format') ##
                statement='''
prefix cpmeta: <http://meta.icos-cp.eu/ontologies/cpmeta/>
prefix prov: <http://www.w3.org/ns/prov#>
select (str(?submTime) as ?time) ?dobj ?spec ?dataLevel ?fileName ?submitterName where{
  ?dobj cpmeta:hasObjectSpec [rdfs:label ?spec ; cpmeta:hasDataLevel ?dataLevel].
  ?dobj cpmeta:hasName ?fileName .
  ?dobj cpmeta:wasSubmittedBy ?submission .
  ?submission prov:endedAtTime ?submTime .
  ?submission prov:wasAssociatedWith [cpmeta:hasName ?submitterName].
}
order by desc(?submTime)
limit 1000
'''                            
                src.setQuery(statement)
                src.setReturnFormat(JSON)
                records = harvestreq().convert()['results']['bindings']
            except (HTTPError,ConnectionError) as err:
                self.logger.critical("%s during connecting to %s\n" % (err,req['url']))
                return -1
            except (ImportError,CannotDisseminateFormat,Exception) as err:
                self.logger.critical("%s during harvest request %s\n" % (err,req))
                return -1
            
        else:
            self.logger.critical(' Not supported harvest type %s' %  req["lverb"])
            sys.exit()

        self.logger.debug(" Harvest method used %s" % harvestreq)
        try:
            if req["lverb"].startswith('List'):
                ntotrecs=len(list(rc))
            else:
                ntotrecs=len(records) 
        except Exception as err:
            self.logger.error('%s Iteratation does not work ?' % (err))
            
        print ("\t|- Retrieved %d records in %d sec - write %s files to disc" % (ntotrecs,time.time()-start,outtypeext.upper()) )
        if ntotrecs == 0 :
            self.logger.warning("\t|- Can not access any records to harvest")
            return -1

        self.logger.debug(' | %-4s | %-25s | %-25s |' % ('#','OAI Identifier','DS Identifier'))
        start2=time.time()

        if (not os.path.isdir(subsetdir+'/'+ outtypedir)):
            os.makedirs(subsetdir+'/' + outtypedir)
        
        delete_ids=list()
        # loop over records
        for record in records :
            ## counter and progress bar
            stats['tcount'] += 1
            fcount+=1
            if fcount <= noffs : continue
            if ntotrecs > 0 :
                perc=int(fcount*100/ntotrecs)
                bartags=int(perc/5)
                if perc%10 == 0 and perc != oldperc :
                    oldperc=perc
                    print ("\r\t[%-20s] %5d (%3d%%) in %d sec" % ('='*bartags, fcount, perc, time.time()-start2 ))
                    sys.stdout.flush()
                    
            # Set oai_id and generate a uniquely identifier for this dataset:
            delete_flag=False
            if req["lverb"] == 'dataset' or req["lverb"] == 'works' or req["lverb"] == 'records' : ## Harvest via JSON-API
                if 'key' in record :
                    oai_id = record['key']
                elif 'id' in record :
                    oai_id = record['id']
            
            elif req["lverb"] == 'csw': ## Harvest via CSW2.0
                if hasattr(record,'identifier') :
                    oai_id = record.identifier
                else:
                    self.logger.critical('Record %s has no attrribute identifier %s' % record) 
            
            elif req["lverb"] == 'ListIdentifiers' : ## OAI-PMH harvesting of XML records
                if (record.deleted):
                    stats['totdcount'] += 1
                    delete_flag=True
                    ##HEW-D continue
                else:
                    oai_id = record.identifier
                    record = sickle.GetRecord(**{'metadataPrefix':req['mdprefix'],'identifier':record.identifier})
            elif req["lverb"] == 'ListRecords' :
                if (record.header.deleted):
                    stats['totdcount'] += 1
                    continue
                else:
                    oai_id = record.header.identifier
            elif req["lverb"].startswith('Sparql'):
                oai_id=record['fileName']['value']
    
            # generate a uniquely identifier and a filename for this dataset:
            uid = str(uuid.uuid5(uuid.NAMESPACE_DNS, oai_id.encode('ascii','replace')))
            outfile = '%s/%s/%s.%s' % (subsetdir,outtypedir,os.path.basename(uid),outtypeext)

            if delete_flag : # record marked as deleted on provider site 
                jsonfile = '%s/%s/%s.%s' % (subsetdir,'json',os.path.basename(uid),'json')
                # remove xml and json file:
                os.remove(xmlfile)
                os.remove(jsonfile)
                delete_ids.append(uid)
            # write record on disc
            try:
                logging.debug('    | h | %-4d | %-45s | %-45s |' % (stats['count']+1,oai_id,uid))
                logging.debug('Try to write the harvested JSON record to %s' % outfile)
                    
                if outtypeext == 'xml':   # get and write the XML content:
                    if hasattr(record,'raw'):
                        metadata = etree.fromstring(record.raw)
                    else:
                        metadata = etree.fromstring(record.xml)
                    if (metadata is not None):
                        try:
                            metadata = etree.tostring(metadata, pretty_print = True)
                        except (Exception,UnicodeEncodeError) as e:
                            self.logger.debug('%s : Metadata: %s ...' % (e,metadata[:20]))
                        if PY2 :
                            try:
                                metadata = metadata.encode('utf-8')
                            except (Exception,UnicodeEncodeError) as e :
                                self.logger.debug('%s : Metadata : %s ...' % (e,metadata[20]))

                        try:
                            f = open(outfile, 'w')
                            f.write(metadata)
                            f.close
                        except IOError :
                            logging.error("[ERROR] Cannot write metadata in xml file '%s': %s\n" % (outfile))
                            stats['ecount'] +=1
                            continue
                        else:
                            logging.debug('Harvested XML file written to %s' % outfile)
                            stats['count'] += 1
                    else:
                        stats['ecount'] += 1
                        self.logger.error('No metadata available for %s' % record)


                elif outtypeext == 'json':   # get the raw json content:
                     if (record is not None):
                         try:
                             with open(outfile, 'w') as f:
                                 json.dump(record,f, sort_keys = True, indent = 4)
                         except IOError:
                             logging.error("[ERROR] Cannot write metadata in out file '%s': %s\n" % (outfile))
                             stats['ecount'] +=1
                             continue
                         else :
                            stats['count'] += 1
                            logging.debug('Harvested JSON file written to %s' % outfile)
                     else:
                        stats['ecount'] += 1
                        logging.warning('    [WARNING] No metadata available for %s' % record['key']) ##HEW-???' % oai_id)


            except TypeError as e:
                    logging.error('    [ERROR] TypeError: %s' % e)
                    stats['ecount']+=1        
                    continue
            except Exception as e:
                    logging.error("    [ERROR] %s and %s" % (e,traceback.format_exc()))
                    ## logging.debug(metadata)
                    stats['ecount']+=1
                    continue

            # Next or last subset?
            if (stats['count'] == count_break) or (fcount == ntotrecs):
                    print('       | %d records written to subset directory %s ' % (stats['count'], subsetdir))

                    # clean up current subset and write ids to remove to delete file
                    for df in os.listdir(subsetdir+'/'+ outtypedir):
                        df=os.path.join(subsetdir+'/'+ outtypedir,df)
                        logging.debug('File to delete : %s' % df)
                        id=os.path.splitext(os.path.basename(df))[0]
                        jf=os.path.join(subsetdir+'/json/',id+'.json')
                        if os.stat(df).st_mtime < start - 1 * 86400:
                            os.remove(df)
                            logging.warning('File %s is deleted' % df)
                            if os.path.exists(jf) : 
                                os.remove(jf)
                                logging.warning('File %s is deleted' % jf)
                            delete_ids.append(id)
                            logging.warning('Append Id %s to list delete_ids' % id)
                            stats['dcount']+=1

                    print('       | %d records deleted from subset directory %s ' % (stats['dcount'], subsetdir))

                    if not fcount == ntotrecs : # next subset neded
                        subsetdir = self.save_subset(req, stats, subset, count_set)
                        if (not os.path.isdir(subsetdir+'/'+ outtypedir)):
                            os.makedirs(subsetdir+'/' + outtypedir)

                        count_set += 1
                                                        
                    # add all subset stats to total stats and reset the temporal subset stats:
                    for key in ['tcount', 'ecount', 'count', 'dcount']:
                        stats['tot'+key] += stats[key]
                        stats[key] = 0
                            
                        # start with a new time:
                        stats['timestart'] = time.time()
                
                    logging.debug('    | %d records written to subset directory %s (if not failed).'% (stats['count'], subsetdir))

        # path to the file with all ids to delete:
        delete_file = '/'.join([self.base_outdir,'delete',req['community']+'-'+req['mdprefix']+'.del'])
        if len(delete_ids) > 0 :
            with open(delete_file, 'a') as file:
                for id in delete_ids :
                    file.write(id+'\n')

        # add all subset stats to total stats and reset the temporal subset stats:
        for key in ['tcount', 'ecount', 'count', 'dcount']:
                stats['tot'+key] += stats[key]
            
        print ('   \t|- %-10s |@ %-10s |\n\t| Provided | Harvested | Failed | Deleted |\n\t| %8d | %9d | %6d | %6d |' % ( 'Finished',time.strftime("%H:%M:%S"),
                    stats['tottcount'],
                    stats['totcount'],
                    stats['totecount'],
                    stats['totdcount']
                ))

        # save the last subset:
        ##HEW-D if (stats['count'] > 0):
        ##HEW-D         lastsubset = self.save_subset(req, stats, subset, count_set)
            
    def save_subset(self, req, stats, subset, count_set):
        # Save stats per subset and add subset item to the convert_list via OUT.print_convert_list()
        #
        # Return Values:
        # --------------
        # 1. (string)   directory to the current subset folder
        # 2. (integer)  counter of the current subset
    
    
        # save stats:
        self.OUT.save_stats(
            "%s-%s" %(req['community'],req['mdprefix']),
            subset+'_'+str(count_set),'h',
            {"tcount":stats['tcount'],
            "count":stats['count'],
            "ecount":stats['ecount'],
            "dcount":stats['dcount'],
            "time":time.time()-stats['timestart']}
        )
        
        # add subset directory to the convert_list:
        subsetpath='/'.join([self.base_outdir,req['community']+'-'+req['mdprefix'],subset+'_'+str(count_set)])
        if (not self.fromdate == None):
            self.OUT.print_convert_list(
                req['community'], req['url'], req['mdprefix'], subsetpath, self.fromdate
                )

        nextsetpath='/'.join([self.base_outdir,req['community']+'-'+req['mdprefix'],subset+'_'+str(count_set+1)])

        return nextsetpath


class MAPPER(object):

    """
    ### MAPPER - class
    # Parameters:
    # -----------
    # 1. (OUT object)   OUT - object of the OUTPUT class
    #
    # Return Values:
    # --------------
    # MAPPER object
    #
    # Public Methods:
    # ---------------
    # map(community, mdprefix, path)  - maps all files in <path> to JSON format by using community and md format specific
    #       mapfiles in md-mapping and stores those files in subdirectory '../json'
    #
    # Usage:
    # ------

    # create MAPPER object:
    MP = MAPPER(OUT)

    path = 'oaidata/enes-iso/subset1'
    community = 'enes'
    mdprefix  = 'iso'

    # map all files of the 'xml' dir in <path> by using mapfile which is defined by <community> and <mdprefix>
    results = MP.map(community,mdprefix,path)
    """

    def __init__ (self, OUT, base_outdir,fromdate):
        ##HEW-D logging = logging.getLogger()
        self.base_outdir = base_outdir
        self.OUT = OUT
        self.fromdate = fromdate
        self.logger = logging.getLogger('root')
        # Read in B2FIND metadata schema and fields
        schemafile =  '%s/mapfiles/b2find_schema.json' % (os.getcwd())
        with open(schemafile, 'r') as f:
            self.b2findfields=json.loads(f.read(), object_pairs_hook=OrderedDict)

        ## settings for pyparsing
        nonBracePrintables = ''
        if PY2:
            unicodePrintables = u''.join(unichr(c) for c in range(65536)
                                        if not unichr(c).isspace())
        else:
            unicodePrintables = u''.join(chr(c) for c in range(65536)
                                        if not chr(c).isspace())
        
        for c in unicodePrintables: ## printables:
            if c not in '(){}[]':
                nonBracePrintables = nonBracePrintables + c

        self.enclosed = Forward()
        value = Combine(OneOrMore(Word(nonBracePrintables) ^ White(' ')))
        nestedParens = nestedExpr('(', ')', content=self.enclosed)
        nestedBrackets = nestedExpr('[', ']', content=self.enclosed)
        nestedCurlies = nestedExpr('{', '}', content=self.enclosed)
        self.enclosed << OneOrMore(value | nestedParens | nestedBrackets | nestedCurlies)

    class cv_disciplines(object):
        """
        This class represents the closed vocabulary used for the mapoping of B2FIND discipline mapping
        Copyright (C) 2014 Heinrich Widmann.

        """
        def __init__(self):
            self.discipl_list = self.get_list()

        @staticmethod
        def get_list():
            import csv
            import os
            discipl_file =  '%s/mapfiles/b2find_disciplines.tab' % (os.getcwd())
            disctab = []
            with open(discipl_file, 'r') as f:
                ## define csv reader object, assuming delimiter is tab
                tsvfile = csv.reader(f, delimiter='\t')

                ## iterate through lines in file
                for line in tsvfile:
                   disctab.append(line)
                   
            return disctab

    class cv_geonames(object):
        """
        This class represents the closed vocabulary used for the mapoping of B2FIND spatial coverage to coordinates
        Copyright (C) 2016 Heinrich Widmann.

        """
        def __init__(self):
            self.geonames_list = self.get_list()

        @staticmethod
        def get_list():
            import csv
            import os
            geonames_file =  '%s/mapfiles/b2find_geonames.tab' % (os.getcwd())
            geonamestab = []
            with open(geonames_file, 'r') as f:
                ## define csv reader object, assuming delimiter is tab
                tsvfile = csv.reader(f, delimiter='\t')

                ## iterate through lines in file
                for line in tsvfile:
                   geonamestab.append(line)
                   
            return geonamestab

    def str_equals(self,str1,str2):
        """
        performs case insensitive string comparison by first stripping trailing spaces 
        """
        return str1.strip().lower() == str2.strip().lower()

    def date2UTC(self,old_date):
        """
        changes date to UTC format
        """
        # UTC format =  YYYY-MM-DDThh:mm:ssZ
        try:
            if type(old_date) is list:
                inlist=old_date
            else:
                inlist=[old_date]
            utc = re.compile(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z')

            utc_day1 = re.compile(r'\d{4}-\d{2}-\d{2}') # day (YYYY-MM-DD)
            utc_day = re.compile(r'\d{8}') # day (YYYYMMDD)
            utc_year = re.compile(r'\d{4}') # year (4-digit number)

            new_date=None
            for val in inlist:
                if utc.search(val):
                    new_date = utc.search(val).group()
                elif utc_day1.search(val):
                    day = utc_day1.search(val).group()
                    new_date = day + 'T11:59:59Z'
                elif utc_day.search(val):
                    rep=re.findall(utc_day, val)[0]
                    new_date = rep[0:4]+'-'+rep[4:6]+'-'+rep[6:8] + 'T11:59:59Z'
                elif utc_year.search(val):
                    year = utc_year.search(val).group()
                    new_date = year + '-07-01T11:59:59Z'
            return new_date
        except Exception :
           logging.error('[ERROR] : %s - in date2UTC replace old date %s by new date %s' % (e,val,new_date))
           return None
        else:
           return new_date

    def replace(self,setname,dataset,facet,old_value,new_value):
        """
        replaces old value - can be a regular expression - with new value for a given facet
        """

        try:
          old_regex = re.compile(old_value)

          for key in dataset:
            if key == facet :
               if re.match(old_regex, dataset[key]):
                  dataset[key] = new_value
                  return dataset
        except Exception :
           logging.error('[ERROR] : %s - in replace of pattern %s in facet %s with new_value %s' % (e,old_value,facet,new_value))
           return dataset
        else:
           return dataset

        return dataset

    def check_url(self,url):
        ## check_url (MAPPER object, url) - method
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
            resp = urlopen(url, timeout=10).getcode()
        except HTTPError as err:
            if (err.code == 422):
                self.logger.error('%s in check_url of %s' % (err.code,url))
                return Warning
            else :
                return False
        except URLError as err: ## HEW : stupid workaraound for SSL: CERTIFICATE_VERIFY_FAILED]
            self.logger.warning('%s in check_url of %s' % (err,url))
            if str(e.reason).startswith('[SSL: CERTIFICATE_VERIFY_FAILED]') :
                return Warning
            else :
                return False
        else:
            return True
 
    def map_url(self, invalue):
        """
        Convert identifiers to data access links, i.e. to 'Source' (ds['url']) or 'PID','DOI' etc. pp
 
        Copyright (C) 2015 by Heinrich Widmann.
        Licensed under AGPLv3.
        """
        try:
            if type(invalue) is not list :
                invalue=invalue.split(";")
            iddict=dict()

            self.logger.debug('invalue %s' % invalue)
            for id in filter(None,invalue) :
                self.logger.debug(' id\t%s' % id)
                if id.startswith('http://data.theeuropeanlibrary'):
                    iddict['url']=id
                elif id.startswith('ivo:'):
                    iddict['IVO']='http://registry.euro-vo.org/result.jsp?searchMethod=GetResource&identifier='+id
                elif id.startswith('10.'): ##HEW-??? or id.startswith('10.5286') or id.startswith('10.1007') :
                    iddict['DOI'] = self.concat('http://dx.doi.org/doi:',id)
                elif 'doi.org/' in id:
                    iddict['DOI'] = 'http://dx.doi.org/'+re.compile(".*doi.org/(.*)\s?.*").match(id).groups()[0].strip(']')
                elif 'doi:' in id: ## and 'DOI' not in iddict :
                    iddict['DOI'] = 'http://dx.doi.org/doi:'+re.compile(".*doi:(.*)\s?.*").match(id).groups()[0].strip(']')
                elif 'hdl.handle.net' in id:
                    reurl = re.search("(?P<url>https?://[^\s<>]+)", id)
                    if reurl :
                        iddict['PID'] = reurl.group("url")
                elif 'hdl:' in id:
                    iddict['PID'] = id.replace('hdl:','http://hdl.handle.net/')
                elif 'http:' in id or 'https:' in id:
                    reurl = re.search("(?P<url>https?://[^\s<>]+)", id)
                    if reurl :
                        iddict['url'] = reurl.group("url")##[0]
            
        except Exception as e :
            self.logger.critical('%s - in map_identifiers %s can not converted !' % (e,invalue))
            return {}
        else:
            if self.OUT.verbose > 2 :
                for id in iddict :
                    self.logger.debug('iddict\t(%s,%s)' % (id,iddict[id]))
                    if self.check_url(iddict[id]):
                        self.logger.debug('Identifier %s checked successfully' % iddict[id])
                    else:
                        self.logger.crtitical('Identifier %s failed in url checker' % iddict[id])

        return iddict

    def map_lang(self, invalue):
        """
        Convert languages and language codes into ISO names
 
        Copyright (C) 2014 Mikael Karlsson.
        Adapted for B2FIND 2014 Heinrich Widmann
        Licensed under AGPLv3.
        """
        def mlang(language):
            if '_' in language:
                language = language.split('_')[0]
            if ':' in language:
                language = language.split(':')[1]
            if len(language) == 2:
                try: return iso639.languages.get(alpha2=language.lower())
                except KeyError: pass
            elif len(language) == 3:
                try: return iso639.languages.get(alpha3=language.lower())
                except KeyError: pass
                except AttributeError: pass
                try: return iso639.languages.get(terminology=language.lower())
                except KeyError: pass
                try: return iso639.languages.get(bibliographic=language.lower())
                except KeyError: pass
            else:
                try: return iso639.languages.get(name=language.title())
                except KeyError: pass
                for l in re.split('[,.;: ]+', language):
                    try: return iso639.languages.get(name=l.title())
                    except KeyError: pass

        newvalue=list()
        if type(invalue) == list :
            for lang in invalue:
                mcountry = mlang(lang)
                if mcountry:
                    newvalue.append(mcountry.name)
        else:
            mcountry = mlang(invalue)
            if mcountry:
                newvalue.append(mcountry.name)

        return newvalue
 
    def map_geonames(self,invalue):
        """
        Map geonames to coordinates
 
        Copyright (C) 2014 Heinrich Widmann
        Licensed under AGPLv3.
        """
        from geopy.geocoders import Nominatim
        from geopy.exc import GeocoderQuotaExceeded
        geolocator = Nominatim()
        try:
          location = geolocator.geocode(invalue.split(';')[0])
          if not location :
              return None ### (None,None)
          if location.raw['importance'] < 0.9 :
              return None
        except GeocoderQuotaExceeded:
           logging.error('%s can not converted !' % (invalue.split(';')[0]))
           sleep(5)
           return None
        except Exception :
           logging.error('[ERROR] : %s - in map_geonames %s can not converted !' % (e,invalue.split(';')[0]))
           return None ### (None,None)
        else:
          return location ### (location.latitude, location.longitude)

    def map_temporal(self,invalue):
        """
        Map date-times to B2FIND start and end time
 
        Copyright (C) 2015 Heinrich Widmann
        Licensed under AGPLv3.
        """
        desc=''
        try:
          logging.info('Invalue\t%s' % invalue)
          if type(invalue) is not list:
            invalue=invalue.split(';')
          if type(invalue[0]) is dict :
            invalue=invalue[0]
            if '@type' in invalue :
              if invalue['@type'] == 'single':
                 if "date" in invalue :       
                   desc+=' %s : %s' % (invalue["@type"],invalue["date"])
                   return (desc,self.date2UTC(invalue["date"]),self.date2UTC(invalue["date"]))
                 else :
                   desc+='%s' % invalue["@type"]
                   return (desc,None,None)
              elif invalue['@type'] == 'verbatim':
                  if 'period' in invalue :
                      desc+=' %s : %s' % (invalue["type"],invalue["period"])
                  else:
                      desc+='%s' % invalue["type"]
                  return (desc,None,None)
              elif invalue['@type'] == 'range':
                  if 'start' in invalue and 'end' in invalue :
                      desc+=' %s : ( %s - %s )' % (invalue['@type'],invalue["start"],invalue["end"])
                      return (desc,self.date2UTC(invalue["start"]),self.date2UTC(invalue["end"]))
                  else:
                      desc+='%s' % invalue["@type"]
                      return (desc,None,None)
              elif 'start' in invalue and 'end' in invalue :
                  desc+=' %s : ( %s - %s )' % ('range',invalue["start"],invalue["end"])
                  return (desc,self.date2UTC(invalue["start"]),self.date2UTC(invalue["end"]))
              else:
                  return (desc,None,None)
          else:
            outlist=list()
            if len(invalue) == 1 :
                try:
                    desc+=' point in time : %s' % self.date2UTC(invalue[0]) 
                    return (desc,self.date2UTC(invalue[0]),self.date2UTC(invalue[0]))
                except ValueError:
                    return (desc,None,None)
##                else:
##                    desc+=': ( %s - %s ) ' % (self.date2UTC(invalue[0]),self.date2UTC(invalue[0])) 
##                    return (desc,self.date2UTC(invalue[0]),self.date2UTC(invalue[0]))
            elif len(invalue) == 2 :
                try:
                    desc+=' period : ( %s - %s ) ' % (self.date2UTC(invalue[0]),self.date2UTC(invalue[1])) 
                    return (desc,self.date2UTC(invalue[0]),self.date2UTC(invalue[1]))
                except ValueError:
                    return (desc,None,None)
            else:
                return (desc,None,None)
        except Exception :
            logging.debug('[ERROR] : %s - in map_temporal %s can not converted !' % (e,invalue))
            return (None,None,None)
        else:
            return (desc,None,None)

    def is_float_try(self,str):
            try:
                float(str)
                return True
            except ValueError:
                return False

    def flatten(self,l):
        for el in l:
            if isinstance(el, collections.Iterable) and not isinstance(el, basestring):
                for sub in flatten(el):
                    yield sub
            else:
                yield el

    def map_spatial(self,invalue,geotab):
        """
        Map coordinates to spatial
 
        Copyright (C) 2014 Heinrich Widmann
        Licensed under AGPLv3.
        """
        desc=''
        pattern = re.compile(r";|\s+")
        try:
          logging.debug('   | Invalue:\t%s' % invalue)
          if isinstance(invalue,list) :
              if len(invalue) == 1:
                  valarr=[invalue[0]] ##HEW-D .split()
              else:
                  valarr=self.flatten(invalue)
          else:
              valarr=invalue.split() ##HEW??? [invalue]
          coordarr=list()
          nc=0
          for val in valarr:
              if type(val) is dict : ## special dict case
                  coordict=dict()
                  if "description" in val :
                      desc=val["description"]
                  if "boundingBox" in val :
                      coordict=val["boundingBox"]
                      desc+=' : [ %s , %s , %s, %s ]' % (coordict["minLatitude"],coordict["maxLongitude"],coordict["maxLatitude"],coordict["minLongitude"])
                      return (desc,coordict["minLatitude"],coordict["maxLongitude"],coordict["maxLatitude"],coordict["minLongitude"])
                  else :
                      return (desc,None,None,None,None)
              else:
                  logging.debug('value %s' % val)
                  if self.is_float_try(val) is True :
                      coordarr.append(val)
                      nc+=1
                  else:
                      ec=0
                      for gentry in geotab :
                          if val == gentry[0]:
                              desc+=' '+val
                              coordarr.append(gentry[1])
                              coordarr.append(gentry[2])
                              break
                      else:
##                          ec+=1
##                          if ec<10 :
##                              geoname=self.map_geonames(val)
##                              time.sleep(0.1)
##                          else:
##                              continue
##                          if geoname == None :
##                              continue
##                          importance=geoname.raw['importance']
##                          if importance < 0.7 : # wg. Claudia :-(
##                              continue
##                          nc=2
##                          coordarr.append(geoname.latitude)
##                          coordarr.append(geoname.longitude)
                          desc+=' '+val
          if nc==2 :
              return (desc,coordarr[0],coordarr[1],coordarr[0],coordarr[1])
          elif nc==4 :
              return (desc,coordarr[0],coordarr[1],coordarr[2],coordarr[3])
          elif desc :
              return (desc,None,None,None,None) 
          else :
              return (None,None,None,None,None) 

          if len(coordarr)==2 :
              desc+=' boundingBox : [ %s , %s , %s, %s ]' % (coordarr[0],coordarr[1],coordarr[0],coordarr[1])
              return(desc,coordarr[0],coordarr[1],coordarr[0],coordarr[1])
          elif len(coordarr)==4 :
              desc+=' boundingBox : [ %s , %s , %s, %s ]' % (coordarr[0],coordarr[1],coordarr[2],coordarr[3])
              return(desc,coordarr[0],coordarr[1],coordarr[2],coordarr[3])
        except Exception as e :
           logging.error('%s - in map_spatial invalue %s can not converted !' % (e,invalue))
           return (None,None,None,None,None) 

    def map_checksum(self,invalue):
        """
        Filter out md checksum from value list
 
        Copyright (C) 2016 Heinrich Widmann
        Licensed under AGPLv3.
        """
        if type(invalue) is not list :
            inlist=re.split(r'[;&\s]\s*',invalue)
            inlist.append(invalue)
        else:
            inlist=invalue

        for inval in inlist: 
            if re.match("[a-fA-F0-9]{32}",inval) : ## checks for MD5 checksums !!! 
                return inval  

        return None

    def map_discipl(self,invalue,disctab):
        """
        Convert disciplines along B2FIND disciplinary list
 
        Copyright (C) 2014 Heinrich Widmann
        Licensed under AGPLv3.
        """
        
        retval=list()
        if type(invalue) is not list :
            inlist=re.split(r'[;&\s]\s*',invalue)
            inlist.append(invalue)
        else:
            seplist=[re.split(r"[;&]",i) for i in invalue]
            swlist=[re.findall(r"[\w']+",i) for i in invalue]
            inlist=swlist+seplist
            inlist=[item for sublist in inlist for item in sublist] ##???
        for indisc in inlist :
           ##indisc=indisc.encode('ascii','ignore').capitalize()
           if PY2:
               indisc=indisc.encode('utf8').replace('\n',' ').replace('\r',' ').strip().title()
           else:
               indisc=indisc.replace('\n',' ').replace('\r',' ').strip().title()
           maxr=0.0
           maxdisc=''
           for line in disctab :
             try:
               disc=line[2].strip()
               r=lvs.ratio(indisc,disc)
             except Exception :
                 logging.error('[ERROR] %s in map_discipl : %s can not compared to %s !' % (e,indisc,disc))
                 continue
             if r > maxr  :
                 maxdisc=disc
                 maxr=r
                 ##HEW-T                 print ('--- %s \n|%s|%s| %f | %f' % (line,indisc,disc,r,maxr))
           if maxr == 1 and indisc == maxdisc :
               self.logger.info('  | Perfect match of %s : nothing to do' % indisc)
               retval.append(indisc.strip())
           elif maxr > 0.90 :
               self.logger.info('   | Similarity ratio %f is > 0.90 : replace value >>%s<< with best match --> %s' % (maxr,indisc,maxdisc))
               ##return maxdisc
               retval.append(indisc.strip())
           else:
               self.logger.debug('   | Similarity ratio %f is < 0.90 compare value >>%s<< and discipline >>%s<<' % (maxr,indisc,maxdisc))
               continue

        if len(retval) > 0:
            retval=list(OrderedDict.fromkeys(retval)) ## this elemenates real duplicates
            return ';'.join(retval)
        else:
            return 'Not stated' 
   
    def cut(self,invalue,pattern,nfield=None):
        """
        Invalue is expected as list. Loop over invalue and for each elem : 
           - If pattern is None truncate characters specified by nfield (e.g. ':4' first 4 char, '-2:' last 2 char, ...)
        else if pattern is in invalue, split according to pattern and return field nfield (if 0 return the first found pattern),
        else return invalue.

        Copyright (C) 2015 Heinrich Widmann.
        Licensed under AGPLv3.
        """

        outvalue=list()
        if not isinstance(invalue,list): invalue = invalue.split()
        for elem in invalue:
            logging.debug('elem:%s\tpattern:%s\tnfield:%s' % (elem,pattern,nfield))
            try:
                if pattern is None :
                    if nfield :
                        outvalue.append(elem[nfield])
                    else:
                        outvalue.append(elem)
                else:
                    rep=''
                    cpat=re.compile(pattern)
                    if nfield == 0 :
                        rep=re.findall(cpat,elem)[0]
                    elif len(re.split(cpat,elem)) > nfield-1 :
                        rep=re.split(cpat,elem)[nfield-1]
                    logging.debug('rep\t%s' % rep)
                    if rep :
                        outvalue.append(rep)
                    else:
                        outvalue.append(elem)
            except Exception :
                logging.error("[ERROR] %s in cut() with invalue %s" % (e,invalue))

        return outvalue

    def list2dictlist(self,invalue,valuearrsep):
        """
        transfer list of strings/dicts to list of dict's { "name" : "substr1" } and
          - eliminate duplicates, numbers and 1-character- strings, ...      
        """

        dictlist=[]
        valarr=[]
        rm_chars = '(){}<>;|`\'\"\\#' ## remove chars not allowed in CKAN tags
        repl_chars = ':,=/?' ## replace chars not allowed in CKAN tags
        # read in list of stopwords
        swfile='%s/mapfiles/stopwords.txt' % os.getcwd()
        with open(swfile) as sw:
            stopwords = sw.read().splitlines()
        if isinstance(invalue,dict):
            invalue=invalue.values()
        elif not isinstance(invalue,list):
            invalue=invalue.split(';')
            invalue=list(OrderedDict.fromkeys(invalue)) ## this eliminates real duplicates
        for lentry in invalue :
            try:
                if type(lentry) is dict :
                    if "value" in lentry:
                        valarr.append(lentry["value"])
                    else:
                        valarr=lentry.values()
                else:
                    valarr=re.split(r"[\n&,;+]+",lentry)
                self.logger.debug('valarr %s' % valarr)
                for entry in valarr:
                    if len(entry.split()) > 8 :
                        logging.debug('String has too many words : %s' % entry)
                        continue
                    entry="". join(c for c in entry if c not in rm_chars and not c.isdigit())
                    for c in repl_chars :
                        if c in entry:
                            entry = entry.replace(c,' ')
                    entry=entry.strip()
                    if isinstance(entry,int) or len(entry) < 2 : continue
                    entrywords = entry.split()
                    resultwords  = [word for word in entrywords if word.lower() not in stopwords]
                    entry=' '.join(resultwords).encode('ascii','ignore').strip()
                    dictlist.append({ "name": entry })
            except AttributeError :
                logging.error('[ERROR] %s in list2dictlist of lentry %s , entry %s' % (err,lentry,entry))
                continue
            except Exception :
                logging.error('[ERROR] %s in list2dictlist of lentry %s, entry %s ' % (e,lentry,entry))
                continue
        return dictlist[:12]

    def uniq(self,input):

        ## eleminates duplicates and removes words in blacklist from list

        blacklist=["Unspecified"]
        for string in blacklist :
            if string in input : input.remove(string)
        uniqset = set(input)
        return list(uniqset)

    def concat(self,str1,str2):
        """
        concatenete given strings

        Copyright (C) 2015 Heinrich Widmann.
        Licensed under AGPLv3.
        """

        return str1+str2

    def utc2seconds(self,dt):
        """
        converts datetime to seconds since year 0

        Copyright (C) 2015 Heinrich Widmann.
        Licensed under AGPLv3.
        """
        year1epochsec=62135600400
        utc1900=datetime.datetime.strptime("1900-01-01T11:59:59Z", "%Y-%m-%dT%H:%M:%SZ")
        utc=self.date2UTC(dt)
        try:
           utctime = datetime.datetime.strptime(utc, "%Y-%m-%dT%H:%M:%SZ")
           diff = utc1900 - utctime
           diffsec= int(diff.days) * 24 * 60 *60
           if diff > datetime.timedelta(0): ## date is before 1900
              sec=int(time.mktime((utc1900).timetuple()))-diffsec+year1epochsec
           else:
              sec=int(time.mktime(utctime.timetuple()))+year1epochsec
        except Exception as err :
           logging.error('[ERROR] : %s - in utc2seconds date-time %s can not converted !' % (err,utc))
           return None

        return sec

    def splitstring2dictlist(self,dataset,facetName,valuearrsep,entrysep):
        """
        split string in list of string and transfer to list of dict's [ { "name1" : "substr1" }, ... ]      
        """

        # read in list of stopwords
        swfile='%s/stopwords' % os.getcwd()
        with open(swfile) as sw:
            stopwords = sw.read().splitlines()
        na_arr=['not applicable','Unspecified']
        for facet in dataset:
          if facet == facetName and len(dataset[facet]) == 1 :
            valarr=dataset[facet][0]['name'].split(valuearrsep)
            valarr=list(OrderedDict.fromkeys(valarr)) ## this elimintas real duplicates
            dicttagslist=[]
            for entry in valarr:
               if entry in na_arr : continue
               entrywords = entry.split()
               resultwords  = [word for word in entrywords if word.lower() not in stopwords]
               print ('resultwords %s' % resultwords)
               entrydict={ "name": ' '.join(resultwords).replace('/','-') }  
               dicttagslist.append(entrydict)
       
            dataset[facet]=dicttagslist
        return dataset       


    def changeDateFormat(self,dataset,facetName,old_format,new_format):
        """
        changes date format from old format to a new format
        current assumption is that the old format is anything (indicated in the 
        config file by * ) and the new format is UTC
        """
        for facet in dataset:
            if self.str_equals(facet,facetName) and old_format == '*':
                if self.str_equals(new_format,'UTC'):
                    old_date = dataset[facet]
                    new_date = date2UTC(old_date)
                    dataset[facet] = new_date
                    return dataset
        return dataset

    def normalize(self,x):
        """normalize the path expression; outside jsonpath to allow testing"""
        subx = []
    
        # replace index/filter expressions with placeholders
        # Python anonymous functions (lambdas) are cryptic, hard to debug
        def f1(m):
            n = len(subx)   # before append
            g1 = m.group(1)
            subx.append(g1)
            ret = "[#%d]" % n
            return ret
        x = re.sub(r"[\['](\??\(.*?\))[\]']", f1, x)
    
        # added the negative lookbehind -krhodes
        x = re.sub(r"'?(?<!@)\.'?|\['?", ";", x)
    
        x = re.sub(r";;;|;;", ";..;", x)
    
        x = re.sub(r";$|'?\]|'$", "", x)
    
        # put expressions back
        def f2(m):
            g1 = m.group(1)
            return subx[int(g1)]
    
        x = re.sub(r"#([0-9]+)", f2, x)
    
        return x
    



    def jsonpath(self,obj, expr, result_type='VALUE', debug=0, use_eval=True):
       """traverse JSON object using jsonpath expr, returning values or paths"""

       def s(x,y):
           """concatenate path elements"""
           return str(x) + ';' + str(y)
   
       def isint(x):
           """check if argument represents a decimal integer"""
           return x.isdigit()
   
       def as_path(path):
           """convert internal path representation to
              "full bracket notation" for PATH output"""
           p = '$'
           for piece in path.split(';')[1:]:
               # make a guess on how to index
               # XXX need to apply \ quoting on '!!
               if isint(piece):
                   p += "[%s]" % piece
               else:
                   p += "['%s']" % piece
           return p
   
       def store(path, object):
           if result_type == 'VALUE':
               result.append(object)
           elif result_type == 'IPATH': # Index format path (Python ext)
               # return list of list of indices -- can be used w/o "eval" or split
               result.append(path.split(';')[1:])
           else: # PATH
               result.append(as_path(path))
           return path
   
       def trace(expr, obj, path):
            if debug: print ("trace", expr, "/", path)
            if expr:
                x = expr.split(';')
                loc = x[0]
                x = ';'.join(x[1:])
                if debug: print ("\t", loc, type(obj))
                if loc == "*":
                    def f03(key, loc, expr, obj, path):
                        if debug > 1: print ("\tf03", key, loc, expr, path)
                        trace(s(key, expr), obj, path)
                    walk(loc, x, obj, path, f03)
                elif loc == "..":
                    trace(x, obj, path)
                    def f04(key, loc, expr, obj, path):
                        if debug > 1: print ("\tf04", key, loc, expr, path)
                        if isinstance(obj, dict):
                            if key in obj:
                                trace(s('..', expr), obj[key], s(path, key))
                        else:
                            if key < len(obj):
                                trace(s('..', expr), obj[key], s(path, key))
                    walk(loc, x, obj, path, f04)
                elif loc == "!":
                    # Perl jsonpath extension: return keys
                    def f06(key, loc, expr, obj, path):
                        if isinstance(obj, dict):
                            trace(expr, key, path)
                    walk(loc, x, obj, path, f06)
                elif isinstance(obj, dict) and loc in obj:
                    trace(x, obj[loc], s(path, loc))
                elif isinstance(obj, list) and isint(loc):
                    iloc = int(loc)
                    if len(obj) >= iloc:
                        trace(x, obj[iloc], s(path, loc))
                else:
                    # [(index_expression)]
                    if loc.startswith("(") and loc.endswith(")"):
                        if debug > 1: print ("index", loc)
                        e = evalx(loc, obj)
                        trace(s(e,x), obj, path)
                        return
    
                    # ?(filter_expression)
                    if loc.startswith("?(") and loc.endswith(")"):
                        if debug > 1: print ("filter", loc)
                        def f05(key, loc, expr, obj, path):
                            if debug > 1: print ("f05", key, loc, expr, path)
                            if isinstance(obj, dict):
                                eval_result = evalx(loc, obj[key])
                            else:
                                eval_result = evalx(loc, obj[int(key)])
                            if eval_result:
                                trace(s(key, expr), obj, path)
    
                        loc = loc[2:-1]
                        walk(loc, x, obj, path, f05)
                        return
    
                    m = re.match(r'(-?[0-9]*):(-?[0-9]*):?(-?[0-9]*)$', loc)
                    if m:
                        if isinstance(obj, (dict, list)):
                            def max(x,y):
                                if x > y:
                                    return x
                                return y
    
                            def min(x,y):
                                if x < y:
                                    return x
                                return y
    
                            objlen = len(obj)
                            s0 = m.group(1)
                            s1 = m.group(2)
                            s2 = m.group(3)
    
                            # XXX int("badstr") raises exception
                            start = int(s0) if s0 else 0
                            end = int(s1) if s1 else objlen
                            step = int(s2) if s2 else 1
    
                            if start < 0:
                                start = max(0, start+objlen)
                            else:
                                start = min(objlen, start)
                            if end < 0:
                                end = max(0, end+objlen)
                            else:
                                end = min(objlen, end)
    
                            for i in xrange(start, end, step):
                                trace(s(i, x), obj, path)
                        return
    
                    # after (expr) & ?(expr)
                    if loc.find(",") >= 0:
                        # [index,index....]
                        for piece in re.split(r"'?,'?", loc):
                            if debug > 1: print ("piece", piece)
                            trace(s(piece, x), obj, path)
            else:
                store(path, obj)
    
       def walk(loc, expr, obj, path, funct):
            if isinstance(obj, list):
                for i in xrange(0, len(obj)):
                    funct(i, loc, expr, obj, path)
            elif isinstance(obj, dict):
                for key in obj:
                    funct(key, loc, expr, obj, path)
    
       def evalx(loc, obj):
            """eval expression"""
    
            if debug: print ("evalx", loc)
    
            # a nod to JavaScript. doesn't work for @.name.name.length
            # Write len(@.name.name) instead!!!
            loc = loc.replace("@.length", "len(__obj)")
    
            loc = loc.replace("&&", " and ").replace("||", " or ")
    
            # replace !@.name with 'name' not in obj
            # XXX handle !@.name.name.name....
            def notvar(m):
                return "'%s' not in __obj" % m.group(1)
            loc = re.sub("!@\.([a-zA-Z@_]+)", notvar, loc)
    
            # replace @.name.... with __obj['name']....
            # handle @.name[.name...].length
            def varmatch(m):
                def brackets(elts):
                    ret = "__obj"
                    for e in elts:
                        if isint(e):
                            ret += "[%s]" % e # ain't necessarily so
                        else:
                            ret += "['%s']" % e # XXX beware quotes!!!!
                    return ret
                g1 = m.group(1)
                elts = g1.split('.')
                if elts[-1] == "length":
                    return "len(%s)" % brackets(elts[1:-1])
                return brackets(elts[1:])
    
            loc = re.sub(r'(?<!\\)(@\.[a-zA-Z@_.]+)', varmatch, loc)
    
            # removed = -> == translation
            # causes problems if a string contains =
    
            # replace @  w/ "__obj", but \@ means a literal @
            loc = re.sub(r'(?<!\\)@', "__obj", loc).replace(r'\@', '@')
            if not use_eval:
                if debug: print ("eval disabled")
                raise Exception("eval disabled")
            if debug: print ("eval", loc)
            try:
                # eval w/ caller globals, w/ local "__obj"!
                v = eval(loc, caller_globals, {'__obj': obj})
            except Exception :
                if debug: print (e)
                return False
    
            if debug: print ("->", v)
            return v
    
       # body of jsonpath()

       # Get caller globals so eval can pick up user functions!!!
       caller_globals = sys._getframe(1).f_globals
       result = []
       if expr and obj:
           cleaned_expr = self.normalize(expr)
           if cleaned_expr.startswith("$;"):
               cleaned_expr = cleaned_expr[2:]
    
           trace(cleaned_expr, obj, '$')

           if len(result) > 0:
               return result
       return False

    def add_unique_to_dict_list(self,dict_list, key, value):
        for d in dict_list:
            if d["key"] ==  key:
                return d["value"]

        dict_list.append({"key": key, "value": value})
        return value
    

    def jsonmdmapper(self,dataset,jrules):
        """
        changes JSON dataset field values according to mapfile
        """  
        format = 'VALUE'
        newds=dict()
      
        for rule in jrules:
           if rule.startswith('#'):
             continue
           field=rule.strip('\n').split(' ')[0]
           jpath=rule.strip('\n').split(' ')[1]

           try:
              if not jpath.startswith('$') :
                value=[jpath]
              else:
                result=self.jsonpath(dataset, jpath, format)
                if isinstance(result, (list, tuple)): ## and (len(result)>0):
                    if isinstance(result[0], (list, tuple)):
                        value=result[0]
                    else:
                        value=result
                else:
                    continue

              # add value to JSON key
              if field in newds:
                  newds[field].extend(value)
              else:
                  newds[field]=value

##HEW-T              if field == 'SpatialCoverage' :
##HEW-T                  print('SpatialCoverage newds %s' % newds[field])

           except Exception as e:
                logging.debug(' %s:[ERROR] %s : processing rule %s : %s : %s' % (self.jsonmdmapper.__name__,e,field,jpath,value))
                continue
        return newds
      
    def postprocess(self,dataset,specrules):
        """
        changes dataset field values according to configuration
        """  
    
        for rule in specrules:
          try: 
            # jump over rule lines starting with #
            if rule.startswith('#'):
                continue
            # specrules can be checked for correctness
            assert(rule.count(',,') == 5),"a double comma should be used to separate items in rule"
            
            rule = rule.rstrip('\n').split(',,') # splits  each line of config file
            ## print ('rule %s' % rule
            groupName = rule[0]
            setName = rule[1]
            facetName = rule[2]
            old_value = rule[3]
            new_value = rule[4]
            action = rule[5]
                        
            oai_set=dataset['oai_set']

            ## call action
            if action == "replace":
                dataset = self.replace(setName,dataset,facetName,old_value,new_value)
##            elif action == "truncate":
##                dataset = self.truncate(dataset,facetName,old_value,new_value)
            elif action == "changeDateFormat":
                dataset = self.changeDateFormat(dataset,facetName,old_value,new_value)
            elif action == 'splitstring2dictlist':
                dataset = self.splitstring2dictlist(dataset,facetName,old_value,new_value)
            elif action == "another_action":
                pass
            else:
                pass
          except Exception as e:
            self.logger.error(" [ERROR] %s : perform %s for facet %s with invalue %s and new_value %s" % (e,action,facetName,old_value,new_value))
            continue

        return dataset
    
    def evalxpath(self, obj, expr, ns):
        # returns list of selected entries from xml obj using xpath expr
        flist=re.split(r'[\(\),]',expr.strip()) ### r'[(]',expr.strip())
        retlist=list()
        for func in flist:
            func=func.strip()
            if func.startswith('//'):
                fxpath= '.'+re.sub(r'/text()','',func)
                self.logger.debug('xpath %s' % fxpath)
                try:
                    for elem in obj.findall(fxpath,ns):
                        self.logger.debug(' //elem %s' % elem)
                        if elem.text :
                            self.logger.debug(' |- elem.text %s' % elem.text)
                            retlist.append(elem.text)
                except Exception as e:
                    self.logger.error('%s : during xpath extraction of %s' % (e,fxpath))
                    return []
            elif func == '/':
                try:
                    for elem in obj.findall('.//',ns):
                        self.logger.debug(' /elem %s' % elem)
                        if elem.text :
                            self.logger.debug(' |- elem.text %s' % elem.text)
                            retlist.append(elem.text)
                except Exception as e:
                    self.logger.error('%s : during xpath extraction of %s' % (e,'./'))
                    return []

        return retlist

    def xpathmdmapper(self,xmldata,xrules,namespaces):
        # returns list or string, selected from xmldata by xpath rules (and namespaces)
        self.logger.info(' XPATH rules %s' % xrules)
        self.logger.info(' | %-10s | %-10s | %-20s | \n' % ('Field','XPATH','Value'))
        jsondata=dict()

        for line in xrules:
          self.logger.info(' Next line of xpath rules : %-20s' % (line))
          try:
            retval=list()
            m = re.match(r'(\s+)<field name="(.*?)">', line)
            if m:
                field=m.group(2)
                if field in ['Discipline','oai_set','Source']: ## set default for mandatory fields !!
                    retval=['Not stated']
                self.logger.info(' Field:xpathrule : %-10s:%-20s\n' % (field,line))
            else:
                xpath=''
                m2 = re.compile('(\s+)(<xpath>)(.*?)(</xpath>)').search(line)
                m3 = re.compile('(\s+)(<string>)(.*?)(</string>)').search(line)
                if m3:
                    xpath=m3.group(3)
                    self.logger.info(' xpath %-10s' % xpath)
                    retval=xpath
                elif m2:
                    xpath=m2.group(3)
                    self.logger.info(' xpath %-10s' % xpath)
                    retval=self.evalxpath(xmldata, xpath, namespaces)
                else:
                    self.logger.info(' Found no xpath expression')
                    continue

                if retval and len(retval) > 0 :
                    jsondata[field]=retval ### .extend(retval)
                    self.logger.info(' | %-10s | %10s | %20s | \n' % (field,xpath,retval[:20]))
                elif field in ['Discipline','oai_set']:
                    jsondata[field]=['Not stated']
          except Exception as e:
              logging.error('    | [ERROR] : %s in xpathmdmapper processing\n\tfield\t%s\n\txpath\t%s\n\tretvalue\t%s' % (e,field,xpath,retval))
              continue

        return jsondata

    def map(self,request): ### community,mdprefix,path,target_mdschema):
        ## map(MAPPER object, community, mdprefix, path) - method
        # Maps XML files formated in source specific MD schema/format (=mdprefix)
        #   to JSON files formatted in target schema (by default B2FIND schema) 
        # For each file two steps are performed
        #  1. select entries by Python XPATH converter according 
        #      the mapfile [<community>-]<mdprefix>.xml . 
        #  2. perform generic and semantic mapping 
        #        versus iso standards and closed vovabularies ...
        #
        # Parameters:
        # -----------
        # 1. (list)     request -  specifies the processing parameters as <communtiy>, <mdprefix> etc. 
        # 2. (string, optinal)   target_mdschema - specifies the schema the inpted records are be mapped to
        #
        # Return Values:
        # --------------
        # 1. (dict)     results statistics
    
        resKeys=['count','tcount','ecount','time']
        results = dict.fromkeys(resKeys,0)
        
        # set processing parameters
        community=request[0]
        mdprefix=request[3]
        mdsubset=request[4]   if len(request)>4 else None
        target_mdschema=request[8]   if len(request)>8 else None

        # settings according to md format (xml or json processing)
        if mdprefix == 'json' :
            mapext='conf'
            insubdir='/hjson'
            infformat='json'
        else:
            mapext='xml'
            insubdir='/xml'
            infformat='xml'

        # read target_mdschema (degfault : B2FIND_schema) and set mapfile
        if (target_mdschema and not target_mdschema.startswith('#')):
            mapfile='%s/mapfiles/%s-%s.%s' % (os.getcwd(),community,target_mdschema,mapext)
        else:
            mapfile='%s/mapfiles/%s-%s.%s' % (os.getcwd(),community,mdprefix,mapext)

        if not os.path.isfile(mapfile):
            self.logger.critical(' Can not access community specific mapfile %s ' % mapfile )
            mapfile='%s/mapfiles/%s.%s' % (os.getcwd(),mdprefix,mapext)
            if not os.path.isfile(mapfile):
                self.logger.critical(' ... nor md schema specific mapfile %s ' % mapfile )
                return results
        print('\t|- Mapfile\t%s' % os.path.basename(mapfile))
        mf = codecs.open(mapfile, "r", "utf-8")
        maprules = mf.readlines()
        maprules = list(filter(lambda x:len(x) != 0,maprules)) # removes empty lines

        # check namespaces
        namespaces=dict()
        for line in maprules:
            ns = re.match(r'(\s+)(<namespace ns=")(\w+)"(\s+)uri="(.*)"/>', line)
            if ns:
                namespaces[ns.group(3)]=ns.group(5)
                continue
        self.logger.debug('  |- Namespaces\t%s' % json.dumps(namespaces,sort_keys=True, indent=4))

        # instance of B2FIND discipline table
        disctab = self.cv_disciplines()
        # instance of B2FIND discipline table
        geotab = self.cv_geonames()
        # instance of British English dictionary
        ##HEW-T dictEn = enchant.Dict("en_GB")
        # loop over all files (harvested records) in input path ( path/xml or path/hjson) 
        ##HEW-D  results['tcount'] = len(filter(lambda x: x.endswith('.json'), os.listdir(path+'/hjson')))

        # community-mdschema root path
        cmpath='%s/%s-%s/' % (self.base_outdir,community,mdprefix)
        self.logger.info('\t|- Input path:\t%s' % cmpath)
        subdirs=next(os.walk(cmpath))[1] ### [x[0] for x in os.walk(cmpath)]
        totcount=0 # total counter of processed files
        subsettag=re.compile(r'_\d+')
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

            # make output directory for mapped json's
            if (target_mdschema and not target_mdschema.startswith('#')):
                outpath='%s-%s/%s/%s/' % (cmpath,target_mdschema,subdir,'json')
            else:
                outpath='%s/%s/%s/' % (cmpath,subdir,'json')
            if (not os.path.isdir(outpath)): os.makedirs(outpath)
            self.logger.debug('Ouput path is %s' % outpath)

            files = list(filter(lambda x: x.endswith(infformat), os.listdir(inpath)))
            results['tcount'] += len(list(files))
            oldperc=0
            err = None
            self.logger.debug(' |- Processing of %s files in %s' % (infformat.upper(),inpath))
       
            ## start processing loop
            start = time.time()
            fcount=0 # counter per sub dir !
            for filename in files:
                ## counter and progress bar
                fcount+=1
                perc=int(fcount*100/int(len(list(files)))) ## int(results['tcount'])
                bartags=int(perc/5)
                if perc%10 == 0 and perc != oldperc:
                    oldperc=perc
                    print ("\r\t [%-20s] %5d (%3d%%) in %d sec" % ('='*bartags, fcount, perc, time.time()-start ))
                    sys.stdout.flush()
                self.logger.debug('    | m | %-4d | %-45s |' % (fcount,filename))

                jsondata = dict()
                infilepath=inpath+'/'+filename      
                if ( os.path.getsize(infilepath) > 0 ):
                    ## load and parse raw xml rsp. json
                    with open(infilepath, 'r') as f:
                        try:
                            if  mdprefix == 'json':
                                jsondata=json.loads(f.read())
                            else:
                                xmldata= ET.parse(infilepath)
                        except Exception as e:
                            self.logger.error('    | [ERROR] %s : Cannot load or parse %s-file %s' % (e,infformat,infilepath))
                            results['ecount'] += 1
                            continue
                        else:
                            self.logger.debug(' |- Read file %s ' % infilepath)
                        
                    ## XPATH rsp. JPATH converter
                    if  mdprefix == 'json':
                        try:
                            self.logger.debug(' |- %s    INFO %s to JSON FileProcessor - Processing: %s/%s' % (time.strftime("%H:%M:%S"),infformat,inpath,filename))
                            jsondata=self.jsonmdmapper(jsondata,maprules)
                        except Exception as e:
                            self.logger.error('    | [ERROR] %s : during %s 2 json processing' % (infformat) )
                            results['ecount'] += 1
                            continue
                    else:
                        try:
                            # Run Python XPATH converter
                            self.logger.warning('    | xpathmapper | %-4d | %-45s |' % (fcount,os.path.basename(filename)))
                            jsondata=self.xpathmdmapper(xmldata,maprules,namespaces)
                            ##HEW-T print ('jsondata %s' % jsondata)
                        except Exception as e:
                            self.logger.error('    | [ERROR] %s : during XPATH processing' % e )
                            results['ecount'] += 1
                            continue

                    iddict=dict()
                    blist=list()
                    spvalue=None
                    stime=None
                    etime=None
                    publdate=None
                    # loop over target schema (B2FIND)
                    self.logger.info('Mapping of ...')
                    ##HEW-T print ('self.b2findfields %s' % self.b2findfields.values())
                    if 'url' not in jsondata:
                        self.logger.error('|- No identifier for id %s' % filename)

                    for facetdict in self.b2findfields.values() :
                        facet=facetdict["ckanName"]
                        ##HEW-T  print ('facet %s ' % facet)
                        if facet in jsondata:
                            self.logger.info('|- ... facet:value %s:%s' % (facet,jsondata[facet]))
                            try:
                                if facet == 'author':
                                    jsondata[facet] = self.uniq(self.cut(jsondata[facet],'\(\d\d\d\d\)',1))
                                elif facet == 'tags':
                                    jsondata[facet] = self.list2dictlist(jsondata[facet]," ")
                                elif facet == 'url':
                                    iddict = self.map_url(jsondata[facet])

                                    if 'DOI' in iddict :
                                        if not 'DOI' in jsondata :
                                            jsondata['DOI']=iddict['DOI']
                                    if 'PID' in iddict :
                                        if not ('DOI' in jsondata and jsondata['DOI']==iddict['PID']):
                                            jsondata['PID']=iddict['PID']
                                    if 'url' in iddict:
                                        ##HEW-D if not ('DOI' in jsondata and jsondata['DOI']==iddict['url']) and not ('PID' in jsondata and jsondata['PID']==iddict['url']  and iddict['url'].startswith('html')) :
                                        jsondata['url']=iddict['url']
                                    else:
                                        jsondata['url']=''

                                elif facet == 'Checksum':
                                    jsondata[facet] = self.map_checksum(jsondata[facet])
                                elif facet == 'Discipline':
                                    jsondata[facet] = self.map_discipl(jsondata[facet],disctab.discipl_list)
                                elif facet == 'Publisher':
                                    blist = self.cut(jsondata[facet],'=',2)
                                    jsondata[facet] = self.uniq(blist)
                                elif facet == 'Contact':
                                    if all(x is None for x in jsondata[facet]):
                                        jsondata[facet] = ['Not stated']
                                    else:
                                        blist = self.cut(jsondata[facet],'=',2)
                                        jsondata[facet] = self.uniq(blist)
                                elif facet == 'SpatialCoverage':
                                    spdesc,slat,wlon,nlat,elon = self.map_spatial(jsondata[facet],geotab.geonames_list)
                                    if wlon and slat and elon and nlat :
                                        spvalue="{\"type\":\"Polygon\",\"coordinates\":[[[%s,%s],[%s,%s],[%s,%s],[%s,%s],[%s,%s]]]}" % (wlon,slat,wlon,nlat,elon,nlat,elon,slat,wlon,slat)
                                    if spdesc :
                                        jsondata[facet] = spdesc
                                elif facet == 'TemporalCoverage':
                                    tempdesc,stime,etime=self.map_temporal(jsondata[facet])
                                    if tempdesc:
                                        jsondata[facet] = tempdesc
                                elif facet == 'Language': 
                                    jsondata[facet] = self.map_lang(jsondata[facet])
                                elif facet == 'Format': 
                                    jsondata[facet] = self.uniq(jsondata[facet])
                                elif facet == 'PublicationYear':
                                    publdate=self.date2UTC(jsondata[facet])
                                    if publdate:
                                        jsondata[facet] = self.cut([publdate],'\d\d\d\d',0)
                                elif facet == 'fulltext':
                                    encoding='utf-8'
                                    jsondata[facet] = ' '.join([x.strip() for x in filter(None,jsondata[facet])]).encode(encoding)[:32000]
                                elif facet == 'oai_set':
                                    if jsondata[facet]==['Not stated'] :
                                        jsondata[facet]=mdsubset
                            except Exception as err:
                                self.logger.error('%s during mapping of field\t%s' % (err,facet))
                                self.logger.debug('\t\tvalue%s' % (jsondata[facet]))
                                continue
                        else: # B2FIND facet not in jsondata
                            if facet == 'title':
                                if 'notes' in jsondata :
                                    jsondata[facet] = jsondata['notes'][:20]
                                else:
                                    jsondata[facet] = 'Not stated'

                    if spvalue :
                        jsondata["spatial"]=spvalue
                    if stime and etime :
                        jsondata["TemporalCoverage:BeginDate"] = stime
                        jsondata["TempCoverageBegin"] = self.utc2seconds(stime) 
                        jsondata["TemporalCoverage:EndDate"] = etime 
                        jsondata["TempCoverageEnd"] = self.utc2seconds(etime)
                    if publdate :
                        jsondata["PublicationTimestamp"] = publdate

                    ## write to JSON file
                    jsonfilename=os.path.splitext(filename)[0]+'.json'
                
                    with io.open(outpath+'/'+jsonfilename, 'w') as json_file:
                        try:
                            self.logger.debug('decode json data')
                            if PY2 :
                                data = json.dumps(jsondata,sort_keys = True, indent = 4).decode('utf-8') ## needed, else : Cannot write json file ... : must be unicode, not str
                            else :
                                data = json.dumps(jsondata,sort_keys = True, indent = 4) ## no decoding for PY3 !!

                        except Exception as err:
                            self.logger.error('%s : Cannot decode jsondata %s' % (err,jsondata))
                        try:
                            self.logger.debug('Write to json file %s/%s' % (outpath,jsonfilename))
                            json_file.write(data)
                        except TypeError as err:
                            self.logger.error(' %s : Cannot write data in json file %s ' % (jsonfilename,err))
                        except Exception as err:
                            self.logger.error(' %s : Cannot write json file %s' % (err,outpath+'/'+filename))
                            ##HEW-D err+='Cannot write json file %s' % jsonfilename
                            results['ecount'] += 1
                            continue
                        else:
                            self.logger.debug(' Succesfully written to json file %s' % outpath+'/'+filename)
                            ##HEW-D err+='Cannot write json file %s' % jsonfilename
                            results['count'] += 1
                            continue
                else:
                    self.logger.error('Can not access content of %s' % infilepath)
                    results['ecount'] += 1
                    continue

        out=' %s to json stdout\nsome stuff\nlast line ..' % infformat
        ##HEW-D if (err is not None ): self.logger.error('[ERROR] ' + err)

        totcount+=results['count'] # total # of sucessfully processed files
        print ('   \t|- %-10s |@ %-10s |\n\t| Provided | Mapped | Failed |\n\t| %8d | %6d | %6d |' % ( 'Finished',time.strftime("%H:%M:%S"),
                    results['tcount'],
                    totcount,
                    results['ecount']
                ))

        # search in output for result statistics
        last_line = out.split('\n')[-2]
        if ('INFO  Main - ' in last_line):
            string = last_line.split('INFO  Main ')[1]
            [results['count'], results['ecount']] = re.findall(r"\d{1,}", string)
            results['count'] = int(results['count']); results['ecount'] = int(results['ecount'])
    
        return results

    def is_valid_value(self,facet,valuelist):
        """
        checks if value is the consitent for the given facet
        """
        vall=list()
        if not isinstance(valuelist,list) : valuelist=[valuelist]

        for value in valuelist:
            errlist=''
            if facet in ['title','notes','author','Publisher']:
                cvalue=value
                try:
                    if PY2 :
                        if isinstance(value, unicode) :
                            ## value=value.decode('utf-8')
                            cvalue=value.encode("iso-8859-1")
                    else :
                        if isinstance(value, str) :
                            cvalue=value.encode("iso-8859-1")
                except (Exception,UnicodeEncodeError) as e :
                    self.logger.error("%s : { %s:%s }" % (e,facet,value))
                else:
                    vall.append(cvalue)
                finally:
                    pass
            elif self.str_equals(facet,'Discipline'):
                if self.map_discipl(value,self.cv_disciplines().discipl_list) is None :
                    errlist+=' | %10s | %20s |' % (facet, value)
                else :
                    vall.append(value)
            elif self.str_equals(facet,'PublicationYear'):
                try:
                    datetime.datetime.strptime(value, '%Y')
                except ValueError:
                    errlist+=' | %10s | %20s |' % (facet, value)
                else:
                    vall.append(value)
            elif self.str_equals(facet,'PublicationTimestamp'):
                try:
                    datetime.datetime.strptime(value, '%Y-%m-%d'+'T'+'%H:%M:%S'+'Z')
                except ValueError:
                    errlist+=' | %10s | %20s |' % (facet, value)
                else:
                    vall.append(value)
            elif self.str_equals(facet,'Language'):
                if self.map_lang(value) is None:
                    errlist+=' | %10s | %20s |' % (facet, value)
                else:
                    vall.append(value)
            elif self.str_equals(facet,'tags'):
                if isinstance(value,dict) and value["name"]:
                    vall.append(value["name"])
                else:
                    errlist+=' | %10s | %20s |' % (facet, value)
            else:
                vall.append(value)
            # to be continued for every other facet

            ##if errlist != '':
            ##    print (' Following key-value errors fails validation:\n' + errlist 
            return vall
                
    def validate(self,request,target_mdschema):
        ## validate(MAPPER object, community, mdprefix, path) - method
        # validates the (mapped) JSON files in directory <path> against the B2FIND md schema
        # Parameters:
        # -----------
        # 1. (string)   community - B2FIND community the md are harvested from
        # 2. (string)   mdprefix -  metadata format of original harvested source (not needed her)
        # 3. (string)   path - path to subset directory 
        #      (without (!) 'json' subdirectory)
        #
        # Return Values:
        # --------------
        # 1. (dict)     statistic of validation 
    
        import collections

        resKeys=['count','tcount','ecount','time']
        results = dict.fromkeys(resKeys,0)
        
        # set processing parameters
        community=request[0]
        mdprefix=request[3]
        mdsubset=request[4]   if len(request)>4 else None

        # set extension of mapfile according to md format (xml or json processing)
        if mdprefix == 'json' :
            mapext='conf' ##!!!HEW --> json
        else:
            mapext='xml'
        mapfile='%s/mapfiles/%s-%s.%s' % (os.getcwd(),community,mdprefix,mapext)
        if not os.path.isfile(mapfile):
           mapfile='%s/mapfiles/%s.%s' % (os.getcwd(),mdprefix,mapext)
           if not os.path.isfile(mapfile):
              self.logger.error('Mapfile %s does not exist !' % mapfile)
              return results
        mf=open(mapfile) 

        # community-mdschema root path
        cmpath='%s/%s-%s/' % (self.base_outdir,community,mdprefix)
        self.logger.info('\t|- Input path:\t%s' % cmpath)
        subdirs=next(os.walk(cmpath))[1] ### [x[0] for x in os.walk(cmpath)]
        # loop over all available subdirs
        fcount=0
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
            inpath='%s/%s/%s' % (cmpath,subdir,'json')
            if not os.path.exists(inpath):
                self.logger.critical('Can not access directory %s' % inpath)
                continue     
            elif not os.path.exists(inpath) or not os.listdir(inpath):
                self.logger.critical('The directory %s does not exist or no json files to validate are found!' % (inpath))
                continue

            # find all .json files in inpath/json:
            files = list(filter(lambda x: x.endswith('.json'), os.listdir(inpath)))
            results['tcount'] = len(files)

            # sum of all .json files of all sub dirs
            results['count'] += results['tcount'] 
        
            self.logger.info(' %s Validation of %d files in %s/json' % (time.strftime("%H:%M:%S"),results['tcount'],inpath))
            if results['tcount'] == 0 :
                self.logger.error(' ERROR : Found no files to validate !')
                return results
            self.logger.info('    |   | %-4s | %-45s |\n   |%s|' % ('#','infile',"-" * 53))

            totstats=dict()
            for facetdict in self.b2findfields.values() :
                facet=facetdict["ckanName"]
                if facet.startswith('#') or facetdict["display"] == "hidden" :
                    continue
                totstats[facet]={
                    'xpath':'',
                    'mapped':0,
                    'valid':0,
                    'vstat':[]
                    }          

                mf.seek(0, 0)
                for line in mf:
                    if '<field name="'+facet+'">' in line:
                        totstats[facet]['xpath']=re.sub(r"<xpath>(.*?)</xpath>", r"\1", next(mf))
                        break

            fcount = 0
            oldperc = 0
            start = time.time()
            for filename in files: ## loop over datasets
                fcount+=1
                perc=int(fcount*100/int(len(files)))
                bartags=int(perc/10)
                if perc%10 == 0 and perc != oldperc :
                    oldperc=perc
                    print ("\r\t [%-20s] %d / %d%% in %d sec" % ('='*bartags, fcount, perc, time.time()-start ))
                    sys.stdout.flush()

                jsondata = dict()
                self.logger.info('    | v | %-4d | %-s/%s |' % (fcount,os.path.basename(inpath),filename))

                if ( os.path.getsize(inpath+'/'+filename) > 0 ):
                    with open(inpath+'/'+filename, 'r') as f:
                        try:
                            jsondata=json.loads(f.read())
                        except:
                            self.logger.error('    | [ERROR] Cannot load the json file %s' % inpath+'/'+filename)
                            results['ecount'] += 1
                            continue
                else:
                    results['ecount'] += 1
                    continue
            
                try:
                    valuearr=list()
                    for facetdict in self.b2findfields.values() : ## loop over facets
                        facet=facetdict["ckanName"]
                        if facet.startswith('#') or facetdict["display"] == "hidden" :
                            continue
                        value = None
                        if facet in jsondata:
                            value = jsondata[facet]
                            self.logger.warning('facet:value : %s:%s' % (facet,value))
                        if value:
                            totstats[facet]['mapped']+=1
                            pvalue=self.is_valid_value(facet,value)
                            self.logger.debug(' key %s\n\t|- value %s\n\t|-  type %s\n\t|-  pvalue %s' % (facet,value[:30],type(value),pvalue[:30]))
                            if pvalue and len(pvalue) > 0:
                                totstats[facet]['valid']+=1  
                                if type(pvalue) is list :
                                    totstats[facet]['vstat'].extend(pvalue)
                                else:
                                    totstats[facet]['vstat'].append(pvalue)
                            else:
                                totstats[facet]['vstat']=[]  
                        else:
                            if facet == 'title':
                                self.logger.debug('    | [ERROR] Facet %s is mandatory, but value is empty' % facet)
                except IOError :
                    self.logger.error(" %s in validation of facet '%s' and value '%s' \n" % (e,facet, value))
                    exit()

        outfile='%s/%s' % (cmpath,'validation.stat')
        printstats='\n Statistics of\n\tcommunity\t%s\n\tsubset\t\t%s\n\t# of records\t%d\n  see as well %s\n\n' % (community,subdir,fcount,outfile)  
        printstats+=" |-> {:<16} <-- {:<20} \n  |-- {:<12} | {:<9} | \n".format('Facet name','XPATH','Mapped','Validated')
        printstats+="  |-- {:>5} | {:>4} | {:>5} | {:>4} |\n".format('#','%','#','%')
        printstats+="      |- Value statistics:\n      |- {:<5} : {:<30} |\n".format('#','Value')
        printstats+=" ----------------------------------------------------------\n"

        for key,facetdict in self.b2findfields.items() : ###.values() :
            facet=facetdict["ckanName"]
            if facet.startswith('#') or facetdict["display"] == "hidden" :
                continue

            if float(fcount) > 0 :
                printstats+="\n |-> {:<16} <-- {:<20}\n  |-- {:>5} | {:>4.0f} | {:>5} | {:>4.0f}\n".format(key,totstats[facet]['xpath'],totstats[facet]['mapped'],totstats[facet]['mapped']*100/float(fcount),totstats[facet]['valid'],totstats[facet]['valid']*100/float(fcount))
                try:
                    counter=collections.Counter(totstats[facet]['vstat'])
                    if totstats[facet]['vstat']:
                        for tuple in counter.most_common(10):
                            ucvalue=tuple[0]##HEW-D .encode('utf8')
                            if len(ucvalue) > 80 :
                                restchar=len(ucvalue)-80
                                contt=' [...(%d chars follow)...]' % restchar 
                            else: 
                                contt=''
                            ##HEW-D?? printstats+="      |- {:<5d} : {:<30}{:<5} |\n".format(tuple[1],unicode(tuple[0])[:80],contt) ##HEW-D??? .encode("utf-8")[:80],contt)
                            printstats+="      |- {:<5d} : {:<30s}{:<5s} |\n".format(tuple[1],ucvalue[:80],contt) ##HEW-D??? .encode("utf-8")[:80],contt)
                except TypeError as e:
                    self.logger.error('%s : facet %s' % (e,facet))
                    continue
                except Exception as e:
                    self.logger.error('%s : facet %s' % (e,facet))
                    continue

        if self.OUT.verbose > 2:
            print (printstats)

        f = open(outfile, 'w')
        f.write(printstats)
        f.write("\n")
        f.close

        self.logger.debug('%s     INFO  B2FIND : %d records validated; %d records caused error(s).' % (time.strftime("%H:%M:%S"),fcount,results['ecount']))


        print ('   \t|- %-10s |@ %-10s |\n\t| Provided | Validated | Failed |\n\t| %8d | %9d | %6d |' % ( 'Finished',time.strftime("%H:%M:%S"),
                    results['tcount'],
                    fcount,
                    results['ecount']
                ))

        return results

    def json2xml(self,json_obj, line_padding="", mdftag="", mapdict="b2findfields"):

        result_list = list()
        json_obj_type = type(json_obj)


        if json_obj_type is list:
            for sub_elem in json_obj:
                result_list.append(json2xml(sub_elem, line_padding, mdftag, mapdict))

            return "\n".join(result_list)

        if json_obj_type is dict:
            for tag_name in json_obj:
                sub_obj = json_obj[tag_name]
                if tag_name in mapdict : 
                    tag_name=mapdict[tag_name]
                    if not isinstance(tag_name,list) : tag_name=[tag_name]
                    for key in tag_name:
                        result_list.append("%s<%s%s>" % (line_padding, mdftag, key))
                        if type(sub_obj) is list:
                            for nv in sub_obj:
                                if tag_name == 'tags' or tag_name == 'KEY_CONNECT.GENERAL_KEY':
                                    result_list.append("%s%s" % (line_padding, nv["name"].strip()))
                                else:
                                    result_list.append("%s%s" % (line_padding, nv.strip()))
                        else:
                            result_list.append(self.json2xml(sub_obj, "\t" + line_padding, mdftag, mapdict))

                        result_list.append("%s</%s%s>" % (line_padding, mdftag, key))



                else:
                        self.logger.debug ('[WARNING] : Field %s can not mapped to B2FIND schema' % tag_name)
                        continue
            
            return "\n".join(result_list)

        return "%s%s" % (line_padding, json_obj)

    def oaiconvert(self,request): ##HEW-D community,mdprefix,path,target_mdschema):
        ## oaiconvert(MAPPER object, request) - method
        # Converts B2FIND JSON files to XML files formatted in target format, e.g. 'CERA' (exp_) and ds2_ files
    
        results = {
            'count':0,
            'tcount':0,
            'ecount':0,
            'time':0
        }
        
        # set processing parameters
        community=request[0]
        mdprefix=request[3]
        mdsubset=request[4]   if len(request)>4 else None
        target_mdschema=request[5]   if len(request)>5 else None
        # set subset:
        if (not mdsubset):
            subset = 'SET_1' ## or 2,...
        elif mdsubset.endswith('_'): # no OAI subsets, but store in sub dirs
            subset = mdsubset+'1' ## or 2,...
        elif mdsubset[-1].isdigit() and  mdsubset[-2] == '_' :
            subset = mdsubset
        else:
            subset = mdsubset+'_1'
        self.logger.info(' |- Subset:    \t%s' % subset )

        # check for target_mdschema and set subset and path
        if (target_mdschema):
            # data subset dir :
            outpath = '/'.join([self.base_outdir,community+'-'+mdprefix+'-'+target_mdschema,subset,'xml'])
            self.logger.info('\t|- Data out path:\t%s' % outpath)
        else:
            self.logger.critical('For OAI converter processing target metaschema must be given!')
            sys.exit()

        inpath = '/'.join([self.base_outdir,community+'-'+mdprefix,subset])
        # check data in and out path
        if not os.path.exists(inpath+'/json') or not os.listdir(inpath + '/json'):
            logging.error('[ERROR] Can not access input data path %s' % (inpath+'/json') )
            return results
        elif not os.path.exists(outpath) :
            logging.warning('[ERROR] Create not existing output data path %s' % (outpath) )
            os.makedirs(outpath)
    
        # run oai-converting
        # find all .json files in inpath/json:
        files = filter(lambda x: x.endswith('.json'), os.listdir(inpath+'/json'))
        
        results['tcount'] = len(files)

        ##oaiset=path.split(target_mdschema)[0].split('_')[0].strip('/')
        ##oaiset=os.path.basename(path)
        ## outpath=path.split(community)[0]+'/b2find-oai_b2find/'+community+'/'+mdprefix +'/'+path.split(mdprefix)[1].split('_')[0]+'/xml'
        ##HEW-D outpath=path.split(community)[0]+'b2find-oai_b2find/'+community+'/'+mdprefix +'/xml'

        logging.debug(' %s     INFO  OAI-Converter of files in %s' % (time.strftime("%H:%M:%S"),inpath))
        logging.debug('    |   | %-4s | %-40s | %-40s |\n   |%s|' % ('#','infile','outfile',"-" * 53))

        fcount = 0
        oldperc = 0
        start = time.time()

        # Read in B2FIND metadata schema and fields
        schemafile =  '%s/mapfiles/b2find_schema.json' % (os.getcwd())
        with open(schemafile, 'r') as f:
            b2findfields=json.loads(f.read())

        for filename in files:
            ## counter and progress bar
            fcount+=1
            perc=int(fcount*100/int(len(files)))
            bartags=perc/10
            if perc%10 == 0 and perc != oldperc :
                oldperc=perc
                print ("\r\t[%-20s] %d / %d%% in %d sec" % ('='*bartags, fcount, perc, time.time()-start ))
                sys.stdout.flush()

            createdate = str(datetime.datetime.utcnow())
            jsondata = dict()
            logging.debug(' |- %s     INFO  JSON2XML - Processing: %s/%s' % (time.strftime("%H:%M:%S"),os.path.basename(inpath),filename))

            if ( os.path.getsize(inpath+'/json/'+filename) > 0 ):
                with open(inpath+'/json/'+filename, 'r') as f:
                    try:
                        jsondata=json.loads(f.read())
                    except:
                        logging.error('    | [ERROR] Can not access json file %s' % inpath+'/json/'+filename)
                        results['ecount'] += 1
                        continue
            else:
                results['ecount'] += 1
                continue
            
            ### oai-convert !!
            if target_mdschema == 'cera':
                ##HEW-T print('JJJJJJJJ %s' % jsondata)
                if 'oai_identifier' in jsondata :
                    identifier=jsondata['oai_identifier'][0]
                else:
                    identifier=os.path.splitext(filename)[0]
                convertfile='%s/mapfiles/%s%s.%s' % (os.getcwd(),'json2',target_mdschema,'json')
                with open(convertfile, 'r') as f:
                    try:
                        mapdict=json.loads(f.read())
                    except:
                        logging.error('    | [ERROR] Cannot load the convert file %s' % convertfile)
                        sys.exit()

                    for filetype in ['ds2','exp']:
                        outfile=outpath+'/'+filetype+'_'+community+'_'+identifier+'.xml'             
	                ### load xml template
                        templatefile='%s/mapfiles/%s_%s_%s.%s' % (os.getcwd(),target_mdschema,filetype,'template','xml')
                        with open(templatefile, 'r') as f:
                            try:
                                dsdata= f.read() ##HEW-D ET.parse(templatefile).getroot()
                            except Exception :
                                logging.error('    | Cannot load tempalte file %s' % (templatefile))

                        data=dict()
                        jsondata['community']=community
                        ##HEW-D dsdata = Template(dsdata)
                        for facetdict in b2findfields.values() :
                            facet=facetdict["ckanName"]
                            ##HEW-T  print ('facet %s ' % facet)
                            if facet in jsondata:
                                if isinstance(jsondata[facet],list) and len(jsondata[facet])>0 :
                                    if facet == 'tags':
                                        data[facet]=''
                                        for tagndict in jsondata[facet]:
                                            data[facet]+=tagndict['name']
                                    else:
                                        data[facet]=' '.join(jsondata[facet]).strip('\n ')
                                else :
                                    data[facet]=jsondata[facet]
                                    ## outdata = dsdata.substitute(key=data[key])
                                    ##HEW-T print('KKKK key %s\t data %s' % (key,data[key]))
                            else:
                                data[facet]=''

                        data['identifier']=identifier
                        try:
                            outdata=dsdata%data
                        except KeyError as err :
                            logging.error("[ERROR] %s\n" % err )
                            pass

                        outfile=outpath+'/'+filetype+'_'+identifier+'.xml'
                        try :
                            f = open(outfile, 'w')
                            f.write(outdata.encode('utf-8'))
                            f.write("\n")
                            f.close
                        except IOError :
                            logging.error("[ERROR] Cannot write data in xml file '%s': %s\n" % (outfile))
                            return(False, outfile , outpath, fcount)
	
            else:
                identifier=jsondata["oai_identifier"]
                outfile=outpath+'/'+filetype+'/'+community+'_'+identifier+'.xml'
                mapdict=self.b2findfields ##HEW-D ??? ckanfields ???
                header="""<record xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
   <header>
     <identifier>"""+identifier+"""</identifier>
     <datestamp>"""+createdate+"""</datestamp>
     <setSpec>"""+oaiset+"""</setSpec>
   </header>
   <metadata>
     <oai_b2find:b2find xmlns:b2find="http://purl.org/b2find/elements/1.1/" xmlns:oai_b2find="http://www.openarchives.org/OAI/2.0/oai_b2find/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://b2find.eudat.eu/schema http://b2find.eudat.eu/schema/oai_b2find.xsd">
"""
                footer="""
     </oai_b2find:b2find>
   </metadata>
</record>"""
                xmlprefix='b2find'
                xmldata=header+self.json2xml(jsondata,'\t',xmlprefix,mapdict)+footer
                try:
                    f = open(outfile, 'w')
                    f.write(xmldata.encode('utf-8'))
                    f.write("\n")
                    f.close
                except IOError :
                    logging.error("[ERROR] Cannot write data in xml file '%s': %s\n" % (outfile))
                    return(False, outfile , outpath, fcount)

            logging.debug('    | o | %-4d | %-45s | %-45s |' % (fcount,os.path.basename(filename),os.path.basename(outfile)))
            

        logging.info('%s     INFO  B2FIND : %d records converted; %d records caused error(s).' % (time.strftime("%H:%M:%S"),fcount,results['ecount']))

        # count ... all .xml files in path/b2find
        results['count'] = len(filter(lambda x: x.endswith('.xml'), os.listdir(outpath)))
        print ('   \t|- %-10s |@ %-10s |\n\t| Provided | Converted | Failed |\n\t| %8d | %6d | %6d |' % ( 'Finished',time.strftime("%H:%M:%S"),
                    results['tcount'],
                    fcount,
                    results['ecount']
                ))
    
        return results    

class UPLOADER(object):

    """
    ### UPLOADER - class
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
                if key in ['Contact','Format','Language','Publisher','PublicationYear','Checksum','Rights']:
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
            self.logger.critical("The dataset name '%s' must be lowercase and alphanumeric + ['_-']" % jsondata['name'])
            jsondata['name']=jsondata['name'].lower()
            self.logger.critical(" ... and is converted now to '%s'" % jsondata['name'])
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

        mdschemas={
            "ddi" : "ddi:codebook:2_5 http://www.ddialliance.org/Specification/DDI-Codebook/2.5/XMLSchema/codebook.xsd",
            "oai_ddi" : "http://www.icpsr.umich.edu/DDI/Version1-2-2.xsd",
            "marcxml" : "http://www.loc.gov/MARC21/slim http://www.loc.gov/standards",
            "iso" : "http://www.isotc211.org/2005/gmd/metadataEntity.xsd",        
            "iso19139" : "http://www.isotc211.org/2005/gmd/gmd.xsd",        
            "inspire" : "http://inspire.ec.europa.eu/theme/ef",        
            "oai_dc" : "http://www.openarchives.org/OAI/2.0/oai_dc.xsd",
            "oai_datacite" : "http://schema.datacite.org/oai/oai-1.0/",
            "oai_qdc" : "http://pandata.org/pmh/oai_qdc.xsd",
            "cmdi" : "http://catalog.clarin.eu/ds/ComponentRegistry/rest/registry/profiles/clarin.eu:cr1:p_1369752611610/xsd",
            "json" : "http://json-schema.org/latest/json-schema-core.html",
            "fgdc" : "No specification for fgdc available",
            "fbb" : "http://www.kulturarv.dk/fbb http://www.kulturarv.dk/fbb/fbb.xsd",
            "hdcp2" : "No specification for hdcp2 settings",
            "eposap" : "http://www.epos-ip.org/terms.html"
        }

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
                jsondata["owner_org"]="eudat"

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
                    logging.error('URL %s is broken' % (mdaccess))
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
                        self.logger.debug('\t - Creation failed. Try to update instead.')
                        res = self.CKAN.action('package_update',jsondata)
                        if (res and res['success']):
                            self.logger.warning("Successful update of %s dataset %s" % (dsstatus,ds_id)) 
                            results['count']+=1
                            upload = 1
                        else:
                            self.logger.warning('\t|- Failed dataset update of %s id %s' % (dsstatus,ds_id))
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
##            results = self.CKAN.action('package_delete',jsondatadel)
            results = self.CKAN.action('dataset_purge',jsondatadel)
            if (results and results['success']):
                rvalue = 1
                self.logger.debug('\t - Succesful deletion of dataset %s.' % dsname)
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
##        except socket.timeout as e:
#            return False    #catched
#        except IOError as err:
#            return False
        else:
            # 200 !?
            return True

class OUTPUT(object):

    """
    ### OUTPUT - class
    # Initializes the logger class, saves statistics per every subset and creates HTML output file
    #
    # Parameters:
    # -----------
    # 1. (dict)     pstat - States of all processes
    # 2. (string)   now - Current date and time
    # 3. (string)   jid - Job id of the programm in the system
    # 4. (OptionParser object)  options - Object with all command line options
    #
    # Return Values:
    # --------------
    # 1. OUTPUT object
    #
    # Public Methods:
    # ---------------
    # .get_stats() 
    # .save_stats(request, subset, mode, stats) - Saves the statistics of a process per subset
    # .start_logger()  - Initializes logger class of the program
    #
    #
    # Usage:
    # ------

    ## EXAMPLE ##
    """

    def __init__(self, pstat, now, jid, options):

        self.options = options
        self.pstat = pstat
        self.start_time = now
        self.jid = jid
        ##HEW-?? self.logger = logger
        
        # create jobdir if it is necessary:
        if (options.jobdir):
            self.jobdir = options.jobdir
        else:
            self.jobdir='log/%s/%s/%s_%s_%s' % (
                now.split(' ')[0],
                now.split(' ')[1].split(':')[0],
                jid, options.mode, options.list
            )
            
        if not os.path.exists(self.jobdir):
            os.makedirs(self.jobdir)
            
        self.convert_list = None
        self.verbose = options.verbose
        
        # Generate special request fields in statistic dictionary:
        self.stats = {
            '#GetPackages' : {
                'time':0,
                'count':0,
            },
            '#Start' : {
                'StartTime':0,
                'TotalTime':0,
                '#critical':False,
                '#error':False,
            }
        }
        self.stats_counter = 0
        
        # choose the debug level:
        self.log_level = {
            'log':logging.INFO,
            'err':logging.ERROR,
            'err':logging.DEBUG,
            'std':logging.INFO,
        }
        
        if self.verbose == 1:
            self.log_level = {
                'log':logging.DEBUG,
                'err':logging.ERROR,
                'std':logging.INFO,
            }
        elif self.verbose == 2:
            self.log_level = {
                'log':logging.DEBUG,
                'err':logging.ERROR,
                'std':logging.DEBUG,
            }
            
        # create the logger and start it:
        ##HEW-CHG!!! self.start_logger()
        self.logger = logging.getLogger('root')        
        self.table_code = ''
        self.details_code = ''

    def setup_custom_logger(self,name,verbose):
            log_format='%(levelname)s :  %(message)s'
            log_level=logging.CRITICAL
            log_format='[ %(levelname)s <%(module)s:%(funcName)s> @\t%(lineno)4s ] %(message)s'
            if verbose == 1 : log_level=logging.ERROR
            elif  verbose == 2 : log_level=logging.WARNING
            elif verbose == 3 : log_level=logging.INFO
            elif verbose > 3 : log_level=logging.DEBUG

            formatter = logging.Formatter(fmt=log_format)

            handler = logging.StreamHandler()
            handler.setFormatter(formatter)

            self.logger.setLevel(log_level)
            self.logger.addHandler(handler)
            return self.logger
    
    def start_logger(self):
        ## start_logger (OUTPUT object) - method
        # Initializes logger class of the program
        #
        # Parameters:
        # -----------
        # None
        #
        # Return Values:
        # --------------
        # None
    
        logger = logging.getLogger('root')
        logger.setLevel(logging.DEBUG)
        
        # create file handler which logs even debug messages
        lh = logging.FileHandler(self.jobdir + '/myapp.log', 'w')
        lh.setLevel(self.log_level['log'])
        
        # create file handler which logs only error messages
        eh = logging.FileHandler(self.jobdir + '/myapp.err', 'w')
        eh.setLevel(self.log_level['err'])
        
        # create console handler with a higher log level
        ch = logging.StreamHandler()
        ch.setLevel(self.log_level['std'])
        
        # create formatter and add it to the handlers
        formatter_l = logging.Formatter("%(message)s")
        formatter_h = logging.Formatter("%(message)s\t[%(module)s, %(funcName)s, NO: %(lineno)s]\n")
        
        lh.setFormatter(formatter_l)
        ch.setFormatter(formatter_l)
        eh.setFormatter(formatter_h)
        
        # add the handlers to the logger
        logger.addHandler(lh)
        logger.addHandler(ch)
        logger.addHandler(eh)
        
        logging = logger

    def save_stats(self,request,subset,mode,stats):
        ## save_stats (OUT object, request, subset, mode, stats) - method
        # Saves the statistics of a process (harvesting, converting, oai-converting, mapping or uploading) per subset in <OUTPUT.stats>. 
        # <OUTPUT.stats> is a big dictionary with all results statistics of the harvesting, converting, oai-converting and uploading routines.
        # Requests which start with a '#' are special requests like '#Start' or '#GetPackages' and will be ignored in the most actions.
        #
        # Special Requests:
        #   #Start    - contents statistics from the start periode and common details of the manager.py
        #       Subsets:
        #           TotalTime   - total time of all processes (without HTML file generation) since start
        #           StartTime   - start time of the manager
        #
        # Parameters:
        # -----------
        # (string)  request - normal request named by <community>-<mdprefix> or a special request which begins with a '#'
        # (string)  subset - ...
        # (string)  mode - process mode (can be 'h', 'c', 'm','v', 'u' or 'o')
        # (dict)    stats - a dictionary with results stats
        #
        # Return Values:
        # --------------
        # None
        
        
        # create a statistic dictionary for this request:
        if(not request in self.stats):
            # create request dictionary:
            self.stats[request] = dict()
        
        # special requests have only dictionaries with two-level-depth:
        if (request.startswith('#')):
            # special request e.g. '#Start':
            self.stats[request][mode] += stats
        
        # normal requests have dictionaries with three-level-depth:
        else:
        
            # create an empty template dictionary if the <subset> have not exists yet:
            if(not subset in self.stats[request]):
                self.stats_counter += 1
                template = {
                    'h':{
                        'count':0,
                        'dcount':0,
                        'ecount':0,
                        'tcount':0,
                        'time':0,
                        'avg':0,
                    },
                    'c':{
                        'count':0,
                        'ecount':0,
                        'tcount':0,
                        'time':0,
                        'avg':0
                    },
                    'm':{
                        'count':0,
                        'ecount':0,
                        'tcount':0,
                        'time':0,
                        'avg':0
                    },
                    'v':{
                        'count':0,
                        'ecount':0,
                        'tcount':0,
                        'time':0,
                        'avg':0
                    },
                    'o':{
                        'count':0,
                        'ecount':0,
                        'tcount':0,
                        'time':0,
                        'avg':0
                    },
                    'u':{
                        'count':0,
                        'ecount':0,
                        'tcount':0,
                        'ncount':0,
                        'time':0,
                        'avg':0
                    },
                    'd':{
                        'count':0,
                        'dcount':0,
                        'ecount':0,
                        'tcount':0,
                        'time':0,
                        'avg':0
                    },
                    '#critical':False,
                    '#error':False,
                    '#id':self.stats_counter,
                }
                self.stats[request].update({subset : template})
            
            # update the values in the old dictionary with the newest <stats>
            for k in stats:
                self.stats[request][subset][mode][k] += stats[k]
            
            # calculate the average time per record:
            if (self.stats[request][subset][mode]['count'] != 0): 
                self.stats[request][subset][mode]['avg'] = \
                    self.stats[request][subset][mode]['time'] / self.stats[request][subset][mode]['count']
            else: 
                self.stats[request][subset][mode]['avg'] = 0


        # If a normal or the #Start request is saved then write the log files separately for each <subset>
        if (not request.startswith('#') or request == '#Start'):

            # shutdown the logger:                                                                  
            logging.shutdown()
            
            # make the new log dir if it is necessary:
            logdir= self.jobdir
            if (not os.path.exists(logdir)):
               os.makedirs(logdir)     

            # generate new log and error filename:
            logfile, errfile = '',''
            if (request == '#Start'):
                logfile='%s/start.logging.txt' % (logdir)
                errfile='%s/start.err.txt' % (logdir)
            else:
                logfile='%s/%s_%s.logging.txt' % (logdir,mode,self.get_stats(request,subset,'#id'))
                errfile='%s/%s_%s.err.txt' % (logdir,mode,self.get_stats(request,subset,'#id'))

            # move log files:
            try:
                if (os.path.exists(logdir+'/myapp.log')):
                    os.rename(logdir+'/myapp.log',logfile )
                if (os.path.exists(logdir+'/myapp.err')):                                                        
                    os.rename(logdir+'/myapp.err',errfile )
            except OSError :
                print("[ERROR] Cannot move log and error files to %s and %s: %s\n" % (logfile,errfile))
            else:
                # set ERROR or CRITICAL flag in stats dictionary if an error log exists:
                if (os.path.exists(errfile)):
                    # open error file:
                    errors=open(errfile,'r').read() or 'No error occured'
                    
                    if (request != '#Start'):
                        # color this line red if an critical or a non-critical error occured:                       
                        if (errors.find('CRITICAL:') != -1):
                            self.stats[request][subset]['#critical'] = True
                        elif (errors.find('ERROR:') != -1):
                            self.stats[request][subset]['#error'] = True


    
    def get_stats(self,request,subset='',mode='',key=''):
        ## get_stats (OUTPUT object, request, subset, mode, key) - method
        # Returns the statistic dictionary which are identified by <request>, <subset>, <mode> and <key> and 
        # saved by .save_stats() before
        #
        # Parameters:
        # -----------
        # (string)  request - param_des
        # (string)  subset - param_des
        # (string)  mode - param_des
        # (string)  key - param_des
        #
        # Return Values:
        # --------------
        # Statistic values
        if (not subset) : subset=''
        

        if ('#' in ''.join([request,subset,mode])):
            
            if (request == '#AllRequests'):
                # returns all requests except all which start with an '#'
                return filter(lambda x: not x.startswith('#'), self.stats.keys())
            elif (subset == '#AllSubsets'):
                # returns all subsets except all which start with an '#'
                return filter(lambda x: x and not x.startswith('#'), self.stats[request].keys())
                
    
            if(request == '#total'):
                # returns the sum of the keys in the modes in all subsets and all requests
                
                total = 0
                
                for r in self.stats:
                    if (not r.startswith('#')):
                        for s in self.stats[r]:
                            total += self.stats[r][s][mode][key]
                    
                return total
            elif(subset == '#total' and mode != '#total'):
                # returns the sum of the keys in the modes of all subsets in the request
                
                total = 0
                
                for s in self.stats[request]:
                    total += self.stats[request][s][mode][key]
                    
                return total
            elif(mode == '#total'):
                total = 0
                
                for s in self.stats[request]:
                    for m in self.stats[request][s]:
                        if (not m.startswith('#')):
                                total += self.stats[request][s][m][key]
                    
                return total
            elif('#' in mode and subset ):
                return self.stats[request][subset][mode]
                
            elif(subset):
                return self.stats[request][subset]

            elif(request):
                return self.stats[request]
        
        return self.stats[request][subset][mode][key]
            
            
            
    
    def HTML_print_begin(self):
        ## HTML_print_begin (OUTPUT object) - method
        # Writes header and layout stylesheet at the begin of HTML overview file and save it to '<OUTPUT.jobdir>/overview.html'
        #
        # Parameters:
        # -----------
        # None
        #
        # Return Values:
        # --------------
        # None
    
    
        pstat = self.pstat
        options = self.options
    
        # open results.html
        reshtml = open(self.jobdir+'/overview.html', 'w')
        
        # write header with css stylesheets at the begin of the HTML file:
        head='''<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01//EN\"
        \"http://www.w3.org/TR/html4/strict.dtd\">
    <html>
	    <head><style type=\"text/css\">
		    body {}
		    .table-error { background-color:#FFC0C0; }
		    .table-critical { background-color:#FF6060; }
		    .table-disabled { color:#cecece; }
		    .details-head {}
		    .details-body-log { background-color:#DFE8FF; padding:4px; margin:0px; }
		    .details-body-error { background-color:#FFC0C0; padding:4px; margin:0px; }
	    </style></head>
	    <body>
    '''
        reshtml.write(head) 
        
        # write head of the body:
        reshtml.write("\t\t<h1>Results of B2FIND ingestion workflow</h1>\n")
        reshtml.write(
            '\t\t<b>Date:</b> %s UTC, <b>Process ID:</b> %s, <b>Epic check:</b> %s<br />\n\t\t<ol>\n' 
                % (self.start_time, self.jid, options.handle_check)
        )
        
        i=1
        for proc in pstat['status']:
            if (pstat['status'][proc] == 'tbd' and proc != 'a') :
                reshtml.write('\t\t\t<li>%s</li>\n' %  (pstat['text'][proc]))
                self.logger.debug('  %d. %s' % (i,pstat['text'][proc]))
                i+=1
                
        reshtml.write('\t\t</ol>\n')
        reshtml.close()


    def HTML_print_end(self):
    
        pstat = self.pstat
        options = self.options
        
        # open HTML file:
        reshtml = open(self.jobdir+'/overview.html', 'a+')
        
        # shows critical script errors:
        critical_script_error = False
        if (os.path.exists(self.jobdir+'/myapp.log') and os.path.exists(self.jobdir+'/myapp.log')):
            size_log = os.path.getsize(self.jobdir+'/myapp.log')
            size_err = os.path.getsize(self.jobdir+'/myapp.err')
            if (size_log != 0):
                size_log = int(size_log/1024.) or 1
            if (size_err != 0):
                size_err = int(size_err/1024.) or 1
                
            reshtml.write('<span style="color:red"><strong>A critical script error occured! Look at <a href="myapp.log">log</a> (%d kB) and <a href="myapp.err">error</a> (%d kB) file. </strong></span><br />'% (size_log, size_err))
            critical_script_error = True
        elif (os.path.isfile(self.jobdir+'/start.err.txt')):
            size_log = os.path.getsize(self.jobdir+'/start.logging.txt')
            size_err = os.path.getsize(self.jobdir+'/start.err.txt')
            if (size_log != 0):
                size_log = int(size_log/1024.) or 1
            if (size_err != 0):
                size_err = int(size_err/1024.) or 1
        
            reshtml.write('<span style="color:red"><strong>A critical script error occured! Look at main <a href="start.logging.txt">log</a> (%d kB) and <a href="start.err.txt">error</a> (%d kB) file. </strong></span><br />'% (size_log, size_err))
            critical_script_error = True

        # get all and processed modes:
        all_modes = pstat['short']
        processed_modes = list()
        for mode in all_modes:
            if (pstat['status'][mode[0]] == 'tbd'):
                processed_modes.append(mode)
        
        ## table with total statistics:
        reshtml.write('''
        <table border=\"1\" rules=\"all\" cellpadding=\"6\"><a name=\"mtable\"
            <tr>
                <th>Stage</th>
                <th>provided</th>
                <th>processed</th>
                <th>failed</th>
                <th>Elapsed time [s]</th>
                <th>Average time/record [s]</th>
            </tr>
''')
        for mode in all_modes:
            reshtml.write(
                '<tr %s><th>%s</th><td>%d</td><td>%d</td><td>%d</td><td>%7.3f</td><td>%7.3f</td></tr>' % (
                    'class="table-disabled"' if ('no' in pstat['status'].values()) else '',
##                    'class="table-disabled"' if (pstat['status'][mode[0]] == 'no') else '',
                    pstat['short'][mode],
                    self.get_stats('#total','#total',mode[0],'tcount'),
                    self.get_stats('#total','#total',mode[0],'count'),
                    self.get_stats('#total','#total',mode[0],'ecount'),
                    self.get_stats('#total','#total',mode[0],'time'),
                    self.get_stats('#total','#total',mode[0],'time') / \
                    self.get_stats('#total','#total',mode[0],'count') if (self.get_stats('#total','#total',mode[0],'count') != 0) else 0,
                )
            )
            
        reshtml.write('''
            <tr border=3><td colspan=3 rowspan=2><td><b>Get dataset list:</b></td><td>%7.3f</td><td>%7.3f</td></tr><br />
            <tr><td><b>Total:</b></td><td>%7.3f</td><td></td></tr>
                  \t\t</table>\n\n<br /><br />
            ''' % (
                      self.get_stats('#GetPackages','time'),
                      self.get_stats('#GetPackages','time') / self.get_stats('#GetPackages','count') if (self.get_stats('#GetPackages','count') != 0) else 0,
                      self.get_stats('#Start','TotalTime'),
            ))

        if len(list(self.get_stats('#AllRequests'))) > 0:
            ## table with details for every request:
            reshtml.write("\t\t<h2>Details per community and mdPrefix</h2>")
            reshtml.write('''\n
            <table border=\"1\" rules=\"all\" cellpadding=\"6\"><a name=\"table\">
                <col width=\"20%\">
                <tr>
                    <th> Community - mdPrefix</th>
                    <th rowspan=2>Time [s]</th>
''')
            reshtml.write(
                '\t\t<th colspan=\"%d\">#provided | #processed | #failed | Elapsed time [s] | Avgt./ proc. rec [s]</th>'
                    % (len(processed_modes)*5)
            )
            reshtml.write('''
                    <th> Details</th>
                </tr>\n
                <tr>
                    <th> Processes > </th>
''')

            for mode in processed_modes:
                reshtml.write('\t\t<th colspan=\"5\">%s</th>\n' % pstat['short'][mode])
                    
            reshtml.write('</tr>\n') 

            rcount = 0
            for request in self.get_stats('#AllRequests'):
                
                reshtml.write('<td valign=\"top\">%s</td><td>%7.3f</td>'% (
                    request,self.get_stats(request,'#total','#total','time')
                ))
                
                for mode in processed_modes:
                    reshtml.write('<td>%d</td><td>%d</td><td>%d<td>%7.3f</td><td>%7.3f</td>'% (
                        self.get_stats(request,'#total',mode[0],'tcount'),
                        self.get_stats(request,'#total',mode[0],'count'),
                        self.get_stats(request,'#total',mode[0],'ecount'),
                        self.get_stats(request,'#total',mode[0],'time'),
                        self.get_stats(request,'#total',mode[0],'avg')
                    ))
                reshtml.write('<td><a href="#details-%s">Details</a></td></tr>'%(request))
                
            reshtml.write("\t\t</table>\n<br/><br /> <br />")
        
            ## table with details for every subset:
            reshtml.write("\t\t<h2>Details per subset</h2>")
            for request in self.get_stats('#AllRequests'):
            
                reshtml.write('''\t\t<hr />
                <span class=\"details-head\">
                    <a name="details-%s" /> %s
                </span><br />   
                ''' % (request,request))
                
                reshtml.write('''\n
                <table border=\"1\" rules=\"all\" cellpadding=\"6\">
                    <col width=\"20%\">
                    <tr>
                        <th> Set</th>
''')
                reshtml.write('<th colspan=\"%d\">#provided | #processed | #failed</th>' % (len(processed_modes)*3))
                reshtml.write('''        
                        <th colspan=\"2\"> Output</th>
                    </tr>\n
                    <tr>
                        <th> Processes > </th>
''')
                for mode in processed_modes:
                    reshtml.write('\t\t<th colspan=\"3\">%s</th>\n' % pstat['short'][mode])
                reshtml.write('\t\t\t<th>Log</th><th>Error</th>\t\t</tr>\n')
                
                for subset in sorted(self.get_stats(request,'#AllSubsets')):
                        
                    # color this line red if an critical or a non-critical error occured:                       
                    if self.get_stats(request,subset,'#critical'):
                        reshtml.write('<tr class="table-critical">')
                    elif self.get_stats(request,subset,'#error'):
                        reshtml.write('<tr class="table-error">')
                    else:
                        reshtml.write('<tr>')
                    
                    reshtml.write('<td valign=\"top\">&rarr; %s</td>'% (subset))
                    
                    for mode in processed_modes:
                        reshtml.write('<td>%d</td><td>%d</td><td>%d'% (
                            self.get_stats(request,subset,mode[0],'tcount'),
                            self.get_stats(request,subset,mode[0],'count'),
                            self.get_stats(request,subset,mode[0],'ecount'),
                        ))
                
                    # link standard output files:
                    reshtml.write('<td valign=\"top\">')
                    for mode in processed_modes:
                        if (pstat['status'][mode[0]] == 'tbd'  and os.path.exists(self.jobdir+'/%s_%d.logging.txt'%(mode[0],self.get_stats(request,subset,'#id')))):
                            try:
                                size = os.path.getsize(self.jobdir+'/%s_%d.logging.txt'%(mode[0],self.get_stats(request,subset,'#id')))
                                if (size != 0):
                                    size = int(size/1024.) or 1
                                reshtml.write('<a href="%s_%d.logging.txt">%s</a> (%d kB)<br />'% (mode[0],self.get_stats(request,subset,'#id'),pstat['short'][mode],size))
                            except OSError :
                                reshtml.write('%s log file not available!<br /><small><small>(<i>%s</i>)</small></small><br />'% (pstat['short'][mode], e))
                    reshtml.write('</td>')
                
                    # link error files:
                    reshtml.write('<td valign=\"top\">')
                    for mode in processed_modes:
                        if (pstat['status'][mode[0]] == 'tbd' and os.path.exists(self.jobdir+'/%s_%d.err.txt'%(mode[0],self.get_stats(request,subset,'#id')))):
                            try:
                                size = os.path.getsize(self.jobdir+'/%s_%d.err.txt'%(mode[0],self.get_stats(request,subset,'#id')))
                                if (size != 0):
                                    size = int(size/1024.) or 1
                                reshtml.write('<a href="%s_%d.err.txt">%s</a> (%d kB)<br />'% (mode[0],self.get_stats(request,subset,'#id'),pstat['short'][mode],size))
                            except OSError :
                                reshtml.write('No %s error file! <br /><small><small>(<i>OSError</i>)</small></small><br />'% (pstat['short'][mode]))
                    reshtml.write('</td>')
                
                    reshtml.write('</tr>')
            
                reshtml.write('</table>')
        else:
            reshtml.write('\t\t<h2>No data found</h2>\n')
            if not critical_script_error:
                reshtml.write('This does not have to be an error. Maybe you just have no requests (or only commented requests) in your list file?')
            
        # close the html document and file:    
        reshtml.write("\t</body>\n</html>\n\n")
        reshtml.close()

    def print_convert_list(self,community,source,mdprefix,dir,fromdate):
        ## print_convert_list (OUT object, community, source, mdprefix, dir, fromdate) - method
        # Write directories with harvested files in convert_list
        #
        # Parameters:
        # -----------
        # ...
        #
        # Return Values:
        # --------------
        # None
        
        self.convert_list = 'convert_list_total'
        ##HEW-D else:
        ##HEW-D    self.convert_list = './convert_list_' + fromdate
        new_entry = '%s\t%s\t%s\t%s\t%s\n' % (community,source,os.path.dirname(dir),mdprefix,os.path.basename(dir))
        file = new_entry

        # don't create duplicated items:
        if(os.path.isfile(self.convert_list)):
            try:
                f = open(self.convert_list, 'r')
                file = f.read()
                f.close()

                if(not new_entry in file):
                    file += new_entry
            except IOError :
                logging.critical("Cannot read data from '{0}'".format(self.convert_list))
                f.close

        try:
            f = open(self.convert_list, 'w')
            f.write(file)
            f.close()
        except IOError :
            logging.critical("Cannot write data to '{0}'".format(self.convert_list))
            f.close
