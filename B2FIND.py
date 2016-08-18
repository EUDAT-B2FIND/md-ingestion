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

# system relevant modules:
import os, glob, sys
import time, datetime, subprocess

# program relevant modules:
import logging
### logger = logging.getLogger('root')
import traceback
import re

# needed for HARVESTER class:
import sickle as SickleClass
from sickle.oaiexceptions import NoRecordsMatch
from requests.exceptions import ConnectionError
import uuid, hashlib
import lxml.etree as etree
import xml.etree.ElementTree as ET
from itertools import tee 
import collections
# needed for CKAN_CLIENT
import urllib, urllib2, socket
import httplib
from urlparse import urlparse

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
import ckanapi

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
		    print '[ERROR] Action name '+ str(action) +' is not defined in CKAN_CLIENT!'
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

            print 'Total number of datasets: ' + str(len(data['result']))
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
            print 'Total number of datasets: ' + str(len(data['result']))
            #self.action('bulk_update_delete',{"datasets" : data['result'], "id":"enes"})
            for dataset in data['result']:
                pcount += 1
                print('\tTry to delete object (' + str(pcount) + ' of ' + str(len(data['result'])) + '): ' + str(dataset))
                print '\t', (self.action('package_update',{"name" : dataset[0], "state":"delete"}))['success']

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
            action_url = "http://{host}/api/3/action/{action}".format(host=self.ip_host,action=action)

        # make json data in conformity with URL standards
        encoding='utf-8'
        ##encoding='ISO-8859-15'
        data_string = urllib.quote(json.dumps(data_dict))##.encode("utf-8") ## HEW-D 160810 , encoding="latin-1" ))##HEW-D .decode(encoding)

        self.logger.debug('\t|-- Action %s\n\t|-- Calling %s ' % (action,action_url))	
        ##HEW-T logging.debug('\t|-- Object %s ' % data_dict)	
        try:
            request = urllib2.Request(action_url)
            if (self.api_key): request.add_header('Authorization', self.api_key)
            response = urllib2.urlopen(request,data_string)
        except urllib2.HTTPError as e:
            logging.debug('\tHTTPError %s : The server %s couldn\'t fulfill the action %s.' % (e.code,self.ip_host,action))
            if ( e.code == 403 ):
                self.logger.error('\tAccess forbidden, maybe the API key is not valid?')
                exit(e.code)
            elif ( e.code == 409 and action == 'package_create'):
                self.logger.debug('\tMaybe the dataset already exists => try to update the package')
                self.action('package_update',data_dict)
            elif ( e.code == 409):
                self.logger.debug('\tMaybe you have a parameter error?')
                return {"success" : False}
            elif ( e.code == 500):
                self.logger.error('\tInternal server error')
                exit(e.code)
        except urllib2.URLError as e:
            self.logger.error('\tURLError %s : %s' % (e,e.reason))
            exit('%s' % e.reason)
        else :
            out = json.loads(response.read())
            assert response.code >= 200
            return out

class HARVESTER(object):
    
    """
    ### HARVESTER - class
    # Provides methods to call a CKAN API request via urllib2
    #
    # Parameters:
    # -----------
    # 1. (OUT object)   OUT   - object of the OUTPUT class
    # 2. (dict)         pstat   - dictionary with the states of every process (was built by main.pstat_init())
    # 3. (path)         rootdir - rootdir where the subdirs will be created and the harvested files will be saved.
    # 4. (string)       fromdate  - filter for harvesting, format: YYYY-MM-DD
    #
    # Return Values:
    # --------------
    # 1. HARVESTER object
    #
    # Public Methods:
    # ---------------
    # .harvest(request) - harvest from a source via OAI-PMH using the python module 'Sickle'
    #
    # Usage:
    # ------

    # create HARVESTER object                       
    HV = HARVESTER(OUT object,pstat,rootdir,fromdate)

    # harvest from a source via sickle module:
    request = [
                    community,
                    source,
                    verb,
                    mdprefix,
                    mdsubset
                ]
    results = HV.harvest(request)

    if (results == -1):
        print "Error occured!"
    """
    
    def __init__ (self, OUT, pstat, base_outdir, fromdate):
        self.logger = logging.getLogger('root')
        self.pstat = pstat
        self.OUT = OUT
        self.base_outdir = base_outdir
        self.fromdate = fromdate
        
    
    def harvest(self, nr, request):
        ## harvest (HARVESTER object, [community, source, verb, mdprefix, mdsubset]) - method
        # Harvest all files with <mdprefix> and <mdsubset> from <source> via sickle module and store those to hard drive.
        # Generate every N. file a new subset directory.
        #
        # Parameters:
        # -----------
        # (list)  request - A request list with following items:
        #                    1. community
        #                    2. source
        #                    3. verb
        #                    4. mdprefix
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
        stats = {
            "tottcount" : 0,    # total number of all provided datasets
            "totcount"  : 0,    # total number of all successful harvested datasets
            "totecount" : 0,    # total number of all failed datasets
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
        
            def __init__ (self, ip_host): ##, api_key):
        	    self.ip_host = ip_host
        
        
        
            def JSONAPI(self, action, offset, key): # see oaireq
                ## JSONAPI (action, jsondata) - method
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
        	    return self.__action_api(action, offset, key)

            def __action_api (self, action, offset, key):
                # Make the HTTP request for get datasets from GBIF portal
                response=''
                rvalue = 0
                ## offset = 0
                limit=100
                api_url = "http://api.gbif.org/v1"
                if key :
                    action_url = "{apiurl}/{action}/{key}".format(apiurl=api_url,action=action,key=str(key))
                else:
                    action_url = "{apiurl}/{action}?offset={offset}&limit={limit}".format(apiurl=api_url,action=action,offset=str(offset),limit=str(limit))	
               ## print '\t|-- Action %s\n\t|-- Calling %s\n\t|-- Offset %d ' % (action,action_url,offset)
                try:
                   request = urllib2.Request(action_url)
                   ##if (self.api_key): request.add_header('Authorization', self.api_key)
                   response = urllib2.urlopen(request)
                except urllib2.HTTPError as e:
                   print '\t\tError code %s : The server %s couldn\'t fulfill the action %s.' % (e.code,self.ip_host,action)
                   if ( e.code == 403 ):
                       print '\t\tAccess forbidden, maybe the API key is not valid?'
                       exit(e.code)
                   elif ( e.code == 409):
                       print '\t\tMaybe you have a parameter error?'
                       return {"success" : False}
                   elif ( e.code == 500):
                       print '\t\tInternal server error'
                       exit(e.code)
                except urllib2.URLError as e:
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
        if (not req["mdsubset"]):
            subset = 'SET'
        elif req["mdsubset"].endswith('_'): # no OAI subsets, but different OAI-URLs for same community
            subset = req["mdsubset"][:-1]
            req["mdsubset"]=None
        else:
            subset = req["mdsubset"]
            
        if (self.fromdate):
            subset = subset + '_f' + self.fromdate
        
        # make subset dir:
        subsetdir = '/'.join([self.base_outdir,req['community']+'-'+req['mdprefix'],subset+'_'+str(count_set)])

        noffs=0 # set to number of record, where harvesting should start
        stats['tcount']=noffs
        fcount=0
        oldperc=0
        records=list()

        if req["lverb"] == 'JSONAPI':
            GBIF = GBIF_CLIENT(req['url'])   # create GBIF object   
            outtypedir='hjson'
            outtypeext='json'
            records=list()
            oaireq=getattr(GBIF,req["lverb"], None)
            choffset=0
            try:
                chunk=oaireq(**{'action':'dataset','offset':choffset,'key':None})

                while(not chunk['endOfRecords']):
                    records.extend(chunk['results'])
                    choffset+=100
                    chunk =oaireq(**{'action':'dataset','offset':choffset,'key':None})
            except urllib2.HTTPError as e:
                self.logger.critical("%s : Cannot harvest through request %s\n" % (e,req))
                return -1
            except ConnectionError as e:
                self.logger.critical("%s : Cannot harvest through request %s\n" % (e,req))
                return -1
            except Exception, e:
                logging.error("[ERROR %s ] : %s" % (e,traceback.format_exc()))
                return -1
                
            ntotrecs=len(records)

        elif req["lverb"].startswith('List'):
            sickle = SickleClass.Sickle(req['url'], max_retries=3, timeout=300)
            outtypedir='xml'
            outtypeext='xml'
            oaireq=getattr(sickle,req["lverb"], None)
            try:
                records,rc=tee(oaireq(**{'metadataPrefix':req['mdprefix'],'set':req['mdsubset'],'ignore_deleted':True,'from':self.fromdate}))
            except urllib2.HTTPError as e:
                self.logger.critical("%s : during harvest request %s\n" % (e,req))
                return -1
            except ConnectionError as e:
                self.logger.critical("%s : during harvest request %s\n" % (e,req))
                return -1
            except etree.XMLSyntaxError as e:
                self.logger.error("[ERROR: %s ] Cannot harvest through request %s\n" % (e,req))
                return -1
            except Exception, e:
                self.logger.error("[ERROR %s ] : %s" % (e,traceback.format_exc()))
                return -1

            ntotrecs=sum(1 for _ in rc)

        print "\t|- Iterate through %d records in %d sec" % (ntotrecs,time.time()-start)
        
        # Add all uid's to the related subset entry of the dictionary deleted_metadata
        deleted_metadata = dict()
        for s in glob.glob('/'.join([self.base_outdir,req['community']+'-'+req['mdprefix'],subset+'_[0-9]*'])):
            for subset in glob.glob(s+'/'+outtypedir+'/*.'+outtypeext):
                # save the uid as key and the subset as value:
                deleted_metadata[os.path.splitext(os.path.basename(subset))[0]] = subset
   
        logging.debug('    |   | %-4s | %-45s | %-45s |\n    |%s|' % ('#','OAI Identifier','DS Identifier',"-" * 106))

        start2=time.time()
        logging.info("\t|- Get records and store on disc ...")
        for record in records:
            stats['tcount'] += 1
            ## counter and progress bar
            fcount+=1
            if fcount <= noffs : continue
            perc=int(fcount*100/ntotrecs)
            bartags=perc/5
            if perc%10 == 0 and perc != oldperc :
                oldperc=perc
                print "\r\t[%-20s] %5d (%3d%%) in %d sec" % ('='*bartags, fcount, perc, time.time()-start2 )
                sys.stdout.flush()

            if req["lverb"] == 'JSONAPI':

                # set oai_id and generate a uniquely identifier for this dataset:
                oai_id = record['key']

                uid = str(uuid.uuid5(uuid.NAMESPACE_DNS, oai_id.encode('ascii','replace')))
                outfile = subsetdir + '/' + outtypedir + '/' + os.path.basename(uid) + '.' + outtypeext
                try:
                    logging.debug('    | h | %-4d | %-45s | %-45s |' % (stats['count']+1,record['key'],uid))
                    logging.debug('Try to write the harvested JSON record to %s' % outfile)
                    
                    # get the raw json content:
                    if (record is not None):
                        record["Organisation"]=oaireq(**{'action':'organization','offset':0,'key':record["publishingOrganizationKey"]})["title"]
                        if (not os.path.isdir(subsetdir+'/'+ outtypedir)):
                           os.makedirs(subsetdir+'/' + outtypedir)
                           
                        # write metadata in file:
                        try:
                            with open(outfile, 'w') as f:
                              json.dump(record,f, sort_keys = True, indent = 4)
                        except IOError, e:
                            logging.error("[ERROR] Cannot write metadata in out file '%s': %s\n" % (outfile,e))
                            stats['ecount'] +=1
                            continue
                        
                        stats['count'] += 1
                        logging.debug('Harvested JSON file written to %s' % outfile)
                        
                    else:
                        stats['ecount'] += 1
                        logging.warning('    [WARNING] No metadata available for %s' % record['key'])
                except TypeError as e:
                    logging.error('    [ERROR] TypeError: %s' % e)
                    stats['ecount']+=1        
                    continue
                except Exception as e:
                    logging.error("    [ERROR] %s and %s" % (e,traceback.format_exc()))
                    ## logging.debug(metadata)
                    stats['ecount']+=1
                    continue
                else:
                    # if everything worked then delete this metadata file from deleted_metadata
                    if uid in deleted_metadata:
                        del deleted_metadata[uid]
            else:  ## OAI-PMH harvesting of XML records using Python Sickle module
                if req["lverb"] == 'ListIdentifiers' :
                    if (record.deleted):
                       ##HEW-??? deleted_metadata[record.identifier] = 
                       stats['totdcount'] += 1
                       continue
                    else:
                       oai_id = record.identifier
                       record = sickle.GetRecord(**{'metadataPrefix':req['mdprefix'],'identifier':record.identifier})
                elif req["lverb"] == 'ListRecords' :
            	    if (record.header.deleted):
                       stats['totdcount'] += 1
            	       continue
                    else:
                       oai_id = record.header.identifier

                # generate a uniquely identifier for this dataset:
                uid = str(uuid.uuid5(uuid.NAMESPACE_DNS, oai_id.encode('ascii','replace')))
                
                xmlfile = subsetdir + '/xml/' + os.path.basename(uid) + '.xml'
                try:
                    logging.debug('    | h | %-4d | %-45s | %-45s |' % (stats['count']+1,oai_id,uid))
                    ## logging.debug('Harvested XML file written to %s' % xmlfile)
                    
                    # get the raw xml content:    
                    metadata = etree.fromstring(record.raw)
                    if (metadata is not None):
                        try:
                            metadata = etree.tostring(metadata, pretty_print = True)
                        except Exception as e:
                            self.logger.debug('%s : Metadata: %s ...' % (e,metadata[:20]))
                        try:
                            metadata = metadata.encode('utf-8')
                        except (Exception,UnicodeEncodeError) as e :
                            self.logger.debug('%s : Metadata : %s ...' % (e,metadata[20]))

                        if (not os.path.isdir(subsetdir+'/xml')):
                           os.makedirs(subsetdir+'/xml')
                           
                        # write metadata in file:
                        try:
                            f = open(xmlfile, 'w')
                            f.write(metadata)
                            f.close
                        except IOError, e:
                            logging.error("[ERROR] Cannot write metadata in xml file '%s': %s\n" % (xmlfile,e))
                            stats['ecount'] +=1
                            continue
                        
                        stats['count'] += 1
                        ## logging.debug('Harvested XML file written to %s' % xmlfile)
                        
                    else:
                        stats['ecount'] += 1
                        logging.warning('    [WARNING] No metadata available for %s' % oai_id)
                except TypeError as e:
                    logging.error('    [ERROR] TypeError: %s' % e)
                    stats['ecount']+=1        
                    continue
                except Exception as e:
                    logging.error("    [ERROR] %s and %s" % (e,traceback.format_exc()))
                    ## logging.debug(metadata)
                    stats['ecount']+=1
                    continue
                else:
                    # if everything worked delete current file from deleted_metadata
                    if uid in deleted_metadata:
                        del deleted_metadata[uid]
                                                                
            # Need a new subset?
            if (stats['count'] == count_break):
                logging.debug('    | %d records written to subset directory %s (if not failed).'% (stats['count'], subsetdir))
                subsetdir, count_set = self.save_subset(
                                req, stats, subset, subsetdir, count_set)
                                                        
                # add all subset stats to total stats and reset the temporal subset stats:
                for key in ['tcount', 'ecount', 'count', 'dcount']:
                    stats['tot'+key] += stats[key]
                    stats[key] = 0
                            
                    # start with a new time:
                    stats['timestart'] = time.time()
                
            logging.debug('    | %d records written to last subset directory %s (if not failed).'% (
                                stats['count'], subsetdir
                            ))

        ##HEW_?? except TypeError as e:
        ##HEW_??     logging.error('    [ERROR] Type Error: %s' % e)
        ##HEW_?? except NoRecordsMatch as e:
        ##HEW_??     logging.error('    [ERROR] No Records Match: %s. Request: %s' % (e,','.join(request)))
        ##HEW_?? except Exception as e:
        ##HEW_??     logging.error("    [ERROR] %s" % traceback.format_exc())
        ##HEW_?? else:

        # check for outdated harvested xml files and add to deleted_metadata, if not already listed
        now = time.time()
        for s in glob.glob('/'.join([self.base_outdir,req['community']+'-'+req['mdprefix'],subset+'_[0-9]*'])):
            for f in glob.glob(s+'/xml/*.xml'):
                 id=os.path.splitext(os.path.basename(f))[0]
                 if os.stat(f).st_mtime < now - 1 * 86400: ## at least 1 day old
                     if os.path.isfile(f):
                        if (id in deleted_metadata ):
                            logging.debug('file %s is already on deleted_metadata' % f)
                        else:
                           deleted_metadata[id] = f

        if (len(deleted_metadata) > 0): ##HEW and self.pstat['status']['d'] == 'tbd':
                ## delete all files in deleted_metadata and write the subset
                ## and the uid in '<outdir>/delete/<community>-<mdprefix>':
                stats['totdcount']=len(deleted_metadata)
                logging.info('    | %d files were not updated and will be deleted:' % (len(deleted_metadata)))
                
                # path to the file with all deleted uids:
                delete_file = '/'.join([self.base_outdir,'delete',req['community']+'-'+req['mdprefix']+'.del'])
                file_content = ''
                
                if(os.path.isfile(delete_file)):
                    try:
                        f = open(delete_file, 'r')
                        file_content = f.readlines()
                        f.close()
                    except IOError as (errno, strerror):
                        logging.critical("Cannot read data from '{0}': {1}".format(delete_file, strerror))
                        f.close
                elif (not os.path.exists(self.base_outdir+'/delete')):
                    os.makedirs(self.base_outdir+'/delete')    

                delete_mode=False
                  # add all deleted metadata to the file, subset in the 1. column and id in the 2. column:
                logging.info("   | List of id's to delete written to {0}.".format(delete_file))                
                if delete_mode == True :
                    logging.info("   |  and related xml and json files are removed")
                else:
                    logging.info("   |  but related are not removed yet") 
                for uid in deleted_metadata:
                    if delete_mode == True :
                        logging.info('    | d | %-4d | %-45s |' % (stats['totdcount'],uid))
                    
                        xmlfile = deleted_metadata[uid]
                        dsubset = os.path.dirname(xmlfile).split('/')[-2]
                        jsonfile = '/'.join(xmlfile.split('/')[0:-2])+'/json/'+uid+'.json'
                
                        # remove xml file:
                        try: 
                            os.remove(xmlfile)
                        except OSError, e:
                            logging.error("    [ERROR] Cannot remove xml file: %s" % (e))
                            stats['totecount'] +=1
                        
                        # remove json file:
                        if (os.path.exists(jsonfile)):
                            try: 
                                os.remove(jsonfile)
                            except OSError, e:
                                logging.error("    [ERROR] Cannot remove json file: %s" % (e))
                                stats['totecount'] +=1

                    # append uid to delete file, if not already exists:
                    if uid+'\n' not in file_content:
                         with open(delete_file, 'a') as file:
                           file.write(uid+'\n')

        # add all subset stats to total stats and reset the temporal subset stats:
        for key in ['tcount', 'ecount', 'count', 'dcount']:
                stats['tot'+key] += stats[key]
            
        print '   \t|- %-10s |@ %-10s |\n\t| Provided | Harvested | Failed | Deleted |\n\t| %8d | %9d | %6d | %6d |' % ( 'Finished',time.strftime("%H:%M:%S"),
                    stats['tottcount'],
                    stats['totcount'],
                    stats['totecount'],
                    stats['totdcount']
                )

        # save the current subset:
        if (stats['count'] > 0):
                self.save_subset(req, stats, subset, subsetdir, count_set)
            
    def save_subset(self, req, stats, subset, subsetdir, count_set):
        ## save_subset(self, req, stats, subset, subsetdir, count_set) - method
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
        self.OUT.print_convert_list(
            req['community'], req['url'], req['mdprefix'], subsetdir, self.fromdate
        )
        
        count_set += 1
            
        return('/'.join([self.base_outdir,req['community']+'-'+req['mdprefix'],subset+'_'+str(count_set)]), count_set)


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

    def __init__ (self, OUT):
        ##HEW-D logging = logging.getLogger()
        self.OUT = OUT
        self.logger = logging.getLogger('root')
        # Read in B2FIND metadata schema and fields
        schemafile =  '%s/mapfiles/b2find_schema.json' % (os.getcwd())
        with open(schemafile, 'r') as f:
            self.b2findfields=json.loads(f.read(), object_pairs_hook=OrderedDict)

##HEW-D        self.ckanfields=list()
##HEW-D        for val in self.b2findfields.values() :
##HEW-D            self.ckanfields.append(val["ckanName"])

        ## settings for pyparsing
        nonBracePrintables = ''
        unicodePrintables = u''.join(unichr(c) for c in xrange(65536) 
                                        if not unichr(c).isspace())
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
        except Exception, e:
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
        except Exception, e:
           logging.error('[ERROR] : %s - in replace of pattern %s in facet %s with new_value %s' % (e,old_value,facet,new_value))
           return dataset
        else:
           return dataset

        return dataset
 
    def map_identifiers(self, invalue):
        """
        Convert identifiers to data access links, i.e. to 'Source' (ds['url']) or 'PID','DOI' etc. pp
 
        Copyright (C) 2015 by Heinrich Widmann.
        Licensed under AGPLv3.
        """
        try:
            ## idarr=invalue.split(";")
            iddict=dict()

            for id in invalue :
                self.logger.debug(' id\t%s' % id)
                if id.startswith('http://data.theeuropeanlibrary'):
                    iddict['url']=id
                elif id.startswith('ivo:'):
                    iddict['IVO']='http://registry.euro-vo.org/result.jsp?searchMethod=GetResource&identifier='+id
                elif id.startswith('10.'): ##HEW-??? or id.startswith('10.5286') or id.startswith('10.1007') :
                    iddict['DOI'] = self.concat('http://dx.doi.org/',id)
                elif 'dx.doi.org/' in id:
                    iddict['DOI'] = id
                elif 'doi:' in id: ## and 'DOI' not in iddict :
                    iddict['DOI'] = 'http://dx.doi.org/doi:'+re.compile(".*doi:(.*)\s?.*").match(id).groups()[0].strip(']')
                elif 'hdl.handle.net' in id:
                    reurl = re.search("(?P<url>https?://[^\s<>]+)", id)
                    if reurl :
                        iddict['PID'] = reurl.group("url")
                elif 'hdl:' in id:
                    iddict['PID'] = id.replace('hdl:','http://hdl.handle.net/')
                ##  elif 'url' not in iddict: ##HEW!!?? bad performance --> and self.check_url(id) :
                elif 'http:' in id or 'https:' in id:
                    self.logger.debug(' id\t%s' % id)
                    reurl = re.search("(?P<url>https?://[^\s<>]+)", id)
                    if reurl :
                        iddict['url'] = reurl.group("url")

        except Exception, e:
            self.logger.error('[ERROR] : %s - in map_identifiers %s can not converted !' % (e,invalue))
            return None
        else:
            self.logger.debug(' iddict\t%s' % iddict)
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
        for lang in invalue:
            mcountry = mlang(lang)
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
        except GeocoderQuotaExceeded, err:
           logging.error('[ERROR] : %s - in map_geonames %s can not converted !' % (e,invalue.split(';')[0]))
           sleep(5)
           return None
        except Exception, e:
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
          if type(invalue) is list:
              invalue=invalue[0]
          if type(invalue) is dict :
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
            invlist=invalue.split(';')
            if len(invlist) == 1 :
                try:
                    desc+=' point in time : %s' % self.date2UTC(invlist[0]) 
                    return (desc,self.date2UTC(invlist[0]),self.date2UTC(invlist[0]))
                except ValueError:
                    return (desc,None,None)
##                else:
##                    desc+=': ( %s - %s ) ' % (self.date2UTC(invlist[0]),self.date2UTC(invlist[0])) 
##                    return (desc,self.date2UTC(invlist[0]),self.date2UTC(invlist[0]))
            elif len(invlist) == 2 :
                try:
                    desc+=' period : ( %s - %s ) ' % (self.date2UTC(invlist[0]),self.date2UTC(invlist[1])) 
                    return (desc,self.date2UTC(invlist[0]),self.date2UTC(invlist[1]))
                except ValueError:
                    return (desc,None,None)
            else:
                return (desc,None,None)
        except Exception, e:
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

                            ##print 'tttt %s, dec %s, unic' % (type(tuple[0]),type(tuple[0].encode('utf8')))

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
              valarr=self.flatten(invalue)
          else:
              valarr=[invalue]
              ##invalue=invalue.split() ##HEW??? [invalue]
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
        except Exception, e:
           logging.error('[ERROR] : %s - in map_spatial invalue %s can not converted !' % (e,invalue))
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
            inlist=[item for sublist in inlist for item in sublist]
        for indisc in inlist :
           ##indisc=indisc.encode('ascii','ignore').capitalize()
           indisc=indisc.encode('utf8').replace('\n',' ').replace('\r',' ').strip().title()
           maxr=0.0
           maxdisc=''
           for line in disctab :
             try:
               disc=line[2].strip()
               r=lvs.ratio(indisc,disc)
             except Exception, e:
                 logging.error('[ERROR] %s in map_discipl : %s can not compared to %s !' % (e,indisc,disc))
                 continue
             if r > maxr  :
                 maxdisc=disc
                 maxr=r
                 ##HEW-T                 print '--- %s \n|%s|%s| %f | %f' % (line,indisc,disc,r,maxr)
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
            logging.debug('[DEBUG]\nelem\t%s\npattern\t%s\nnfield\t%s' % (elem,pattern,nfield))
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
                    logging.debug('[DEBUG] rep\t%s' % rep)
                    if rep :
                        outvalue.append(rep)
                    else:
                        outvalue.append(elem)
            except Exception, e:
                logging.error("[ERROR] %s in cut() with invalue %s" % (e,invalue))

        return outvalue

    def list2dictlist(self,invalue,valuearrsep):
        """
        transfer list of strings/dicts to list of dict's { "name" : "substr1" } and
          - eliminate duplicates, numbers and 1-character- strings, ...      
        """

        dictlist=[]
        valarr=[]
        rm_chars = '(){}<>;|`\'\"\\' ## remoove chars not allowed in CKAN tags
        repl_chars = ':,=/?' ## replace chars not allowed in CKAN tags
        bad_words = ['and','or','the']
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
                    if entry in bad_words : continue
                    ##HEW-CHG entry=entry.decode('utf-8').strip()
                    ##HEW-??? 
                    entry=entry.encode('ascii','ignore').strip()
                    dictlist.append({ "name": entry })
            except AttributeError, err :
                logging.error('[ERROR] %s in list2dictlist of lentry %s , entry %s' % (err,lentry,entry))
                continue
            except Exception, e:
                logging.error('[ERROR] %s in list2dictlist of lentry %s, entry %s ' % (e,lentry,entry))
                continue
        return dictlist[:12]

    def uniq(self,input,joinsep=None):
        uniqset = set(input)

        ##if joinsep :
        ##    return joinsep.join(list(set))
        ##else :
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
        except Exception, e:
           logging.error('[ERROR] : %s - in utc2seconds date-time %s can not converted !' % (e,utc))
           return None

        return sec

    def splitstring2dictlist(self,dataset,facetName,valuearrsep,entrysep):
        """
        split string in list of string and transfer to list of dict's [ { "name1" : "substr1" }, ... ]      
        """
        na_arr=['not applicable']
        for facet in dataset:
          if facet == facetName and len(dataset[facet]) == 1 :
            valarr=dataset[facet][0]['name'].split(valuearrsep)
            valarr=list(OrderedDict.fromkeys(valarr)) ## this elimintas real duplicates
            dicttagslist=[]
            for entry in valarr:
               if entry in na_arr : continue
               entrydict={ "name": entry.replace('/','-') }  
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
            if debug: print "trace", expr, "/", path
            if expr:
                x = expr.split(';')
                loc = x[0]
                x = ';'.join(x[1:])
                if debug: print "\t", loc, type(obj)
                if loc == "*":
                    def f03(key, loc, expr, obj, path):
                        if debug > 1: print "\tf03", key, loc, expr, path
                        trace(s(key, expr), obj, path)
                    walk(loc, x, obj, path, f03)
                elif loc == "..":
                    trace(x, obj, path)
                    def f04(key, loc, expr, obj, path):
                        if debug > 1: print "\tf04", key, loc, expr, path
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
                        if debug > 1: print "index", loc
                        e = evalx(loc, obj)
                        trace(s(e,x), obj, path)
                        return
    
                    # ?(filter_expression)
                    if loc.startswith("?(") and loc.endswith(")"):
                        if debug > 1: print "filter", loc
                        def f05(key, loc, expr, obj, path):
                            if debug > 1: print "f05", key, loc, expr, path
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
                            if debug > 1: print "piece", piece
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
    
            if debug: print "evalx", loc
    
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
                if debug: print "eval disabled"
                raise Exception("eval disabled")
            if debug: print "eval", loc
            try:
                # eval w/ caller globals, w/ local "__obj"!
                v = eval(loc, caller_globals, {'__obj': obj})
            except Exception, e:
                if debug: print e
                return False
    
            if debug: print "->", v
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
              newds[field]=value

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
            ## print 'rule %s' % rule
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
                try:
                    for elem in obj.findall(fxpath,ns):
                        if elem.text :
                            retlist.append(elem.text)
                except Exception as e:
                    print 'ERROR %s : during xpath extraction of %s' % (e,fxpath)
                    return []
            elif func == '/':
                try:
                    for elem in obj.findall('.//',ns):
                        retlist.append(elem.text)
                except Exception as e:
                    print 'ERROR %s : during xpath extraction of %s' % (e,'./')
                    return []

        return retlist

    def xpathmdmapper(self,xmldata,xrules,namespaces):
        # returns list or string, selected from xmldata by xpath rules (and namespaces)
        logging.debug(' | %10s | %10s | %10s | \n' % ('Field','XPATH','Value'))

        jsondata=dict()

        for line in xrules:
          try:
            m = re.match(r'(\s+)<field name="(.*?)">', line)
            if m:
                field=m.group(2)
                if field in ['Discipline','oai_set','Source']: ## HEW!!! add all mandatory fields !!
                    retval=['Not stated']
            else:
                xpath=''
                r = re.compile('(\s+)(<xpath>)(.*?)(</xpath>)')
                m2 = r.search(line)
                rs = re.compile('(\s+)(<string>)(.*?)(</string>)')
                m3 = rs.search(line)
                if m3:
                    xpath=m3.group(3)
                    retval=xpath
                elif m2:
                    xpath=m2.group(3)
                    retval=self.evalxpath(xmldata, xpath, namespaces)
                else:
                    continue
                if retval and len(retval) > 0 :
                    jsondata[field]=retval ### .extend(retval)
                    logging.debug(' | %-10s | %10s | %20s | \n' % (field,xpath,retval[:20]))
                elif field in ['Discipline','oai_set']:
                    jsondata[field]=['Not stated']
          except Exception as e:
              logging.error('    | [ERROR] : %s in xpathmdmapper processing\n\tfield\t%s\n\txpath\t%s\n\tretvalue\t%s' % (e,field,xpath,retval))
              continue

        return jsondata

    def map(self,nr,community,mdprefix,path,target_mdschema):
        ## map(MAPPER object, community, mdprefix, path) - method
        # Maps the XML files in directory <path> to JSON files 
        # For each file two steps are performed
        #  1. select entries by Python XPATH converter according 
        #      the mapfile [<community>-]<mdprefix>.xml . 
        #  2. perform generic and semantic mapping versus iso standards and closed vovabularies ...
        #
        # Parameters:
        # -----------
        # 1. (string)   community - B2FIND community of the files
        # 2. (string)   mdprefix - Metadata prefix which was used by HARVESTER class for harvesting these files
        # 3. (string)   path - path to directory of harvested records (without 'xml' rsp. 'hjson' subdirectory)
        #
        # Return Values:
        # --------------
        # 1. (dict)     results statistics
    
        results = {
            'count':0,
            'tcount':0,
            'ecount':0,
            'time':0
        }
        
        # settings according to md format (xml or json processing)
        if mdprefix == 'json' :
            mapext='conf'
            insubdir='/hjson'
            infformat='json'
        else:
            mapext='xml'
            insubdir='/xml'
            infformat='xml'

        # check input and output paths
        if not os.path.exists(path):
            logging.error('[ERROR] The directory "%s" does not exist! No files to map are found!\n(Maybe your convert list has old items?)' % (path))
            return results
        elif not os.path.exists(path + insubdir) or not os.listdir(path + insubdir):
            logging.error('[ERROR] The input directory "%s%s" does not exist or no %s-files to convert are found !\n(Maybe your convert list has old items?)' % (path,insubdir,insubdir))
            return results
      
        # make output directory for mapped json's
        if (target_mdschema):
            outpath=path+'-'+target_mdschema+'/json'
        else:
            outpath=path+'/json'


        if (not os.path.isdir(outpath)):
           os.makedirs(outpath)

        # check and read rules from mapfile
        if (target_mdschema):
            mapfile='%s/mapfiles/%s-%s.%s' % (os.getcwd(),community,target_mdschema,mapext)
        else:
            mapfile='%s/mapfiles/%s-%s.%s' % (os.getcwd(),community,mdprefix,mapext)

        if not os.path.isfile(mapfile):
            mapfile='%s/mapfiles/%s.%s' % (os.getcwd(),mdprefix,mapext)
            if not os.path.isfile(mapfile):
                self.logger.error('[ERROR] Mapfile %s does not exist !' % mapfile)
                return results
        self.logger.debug('  |- Mapfile\t%s' % os.path.basename(mapfile))
        mf = codecs.open(mapfile, "r", "utf-8")
        maprules = mf.readlines()
        maprules = filter(lambda x:len(x) != 0,maprules) # removes empty lines

        # check namespaces
        namespaces=dict()
        for line in maprules:
            ns = re.match(r'(\s+)(<namespace ns=")(\w+)"(\s+)uri="(.*)"/>', line)
            if ns:
                namespaces[ns.group(3)]=ns.group(5)
                continue
        self.logger.debug('  |- Namespaces\t%s' % json.dumps(namespaces,sort_keys=True, indent=4))

        # check specific postproc mapping config file
        subset=os.path.basename(path).split('_')[0]
        specrules=None
        ppconfig_file='%s/mapfiles/mdpp-%s-%s.conf' % (os.getcwd(),community,mdprefix)
        if os.path.isfile(ppconfig_file):
            # read config file
            f = codecs.open(ppconfig_file, "r", "utf-8")
            specrules = f.readlines()[1:] # without the header
            specrules = filter(lambda x:len(x) != 0,specrules) # removes empty lines
            ## filter out community and subset specific specrules
            subsetrules = filter(lambda x:(x.startswith(community+',,'+subset)),specrules)
            if subsetrules:
                specrules=subsetrules
            else:
                specrules=filter(lambda x:(x.startswith('*,,*')),specrules)

        # instance of B2FIND discipline table
        disctab = self.cv_disciplines()
        # instance of B2FIND discipline table
        geotab = self.cv_geonames()
        # instance of British English dictionary
        ##HEW-T dictEn = enchant.Dict("en_GB")
        # loop over all files (harvested records) in input path ( path/xml or path/hjson) 
        ##HEW-D  results['tcount'] = len(filter(lambda x: x.endswith('.json'), os.listdir(path+'/hjson')))
        files = filter(lambda x: x.endswith(infformat), os.listdir(path+insubdir))
        results['tcount'] = len(files)
        fcount = 0
        oldperc=0
        err = None
        self.logger.debug(' %s     INFO  Processing of %s files in %s/%s' % (time.strftime("%H:%M:%S"),infformat,path,insubdir))
        
        ## start processing loop
        start = time.time()
        for filename in files:
            ## counter and progress bar
            fcount+=1
            perc=int(fcount*100/int(len(files)))
            bartags=perc/5
            if perc%10 == 0 and perc != oldperc:
                oldperc=perc
                self.logger.debug("\r\t[%-20s] %5d (%3d%%) in %d sec" % ('='*bartags, fcount, perc, time.time()-start ))
                sys.stdout.flush()

            jsondata = dict()

            infilepath=path+insubdir+'/'+filename      
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
                ## XPATH rsp. JPATH converter
                if  mdprefix == 'json':
                    try:
                        self.logger.debug(' |- %s    INFO %s to JSON FileProcessor - Processing: %s%s/%s' % (time.strftime("%H:%M:%S"),infformat,os.path.basename(path),insubdir,filename))
                        jsondata=self.jsonmdmapper(jsondata,maprules)
                    except Exception as e:
                        logging.error('    | [ERROR] %s : during %s 2 json processing' % (infformat,e) )
                        results['ecount'] += 1
                        continue
                else:
                    try:
                        # Run Python XPATH converter
                        logging.debug('    | xpath | %-4d | %-45s |' % (fcount,os.path.basename(filename)))
                        jsondata=self.xpathmdmapper(xmldata,maprules,namespaces)
                    except Exception as e:
                        logging.error('    | [ERROR] %s : during XPATH processing' % e )
                        results['ecount'] += 1
                        continue
                try:
                   ## md postprocessor
                   if (specrules):
                       logging.debug(' [INFO]:  Processing according specrules %s' % specrules)
                       jsondata=self.postprocess(jsondata,specrules)
                except Exception as e:
                    logging.error(' [ERROR] %s : during postprocessing' % (e))
                    continue

                iddict=dict()
                blist=list()
                spvalue=None
                stime=None
                etime=None
                publdate=None
                # loop over all fields
                for facet in jsondata:
                   self.logger.debug('Maping of facet %s ...' % facet)
                   try:
                       if facet == 'author':
                           jsondata[facet] = self.uniq(self.cut(jsondata[facet],'\(\d\d\d\d\)',1),';')
                       elif facet == 'tags':
                           jsondata[facet] = self.list2dictlist(jsondata[facet]," ")
                       elif facet == 'url':
                           iddict = self.map_identifiers(jsondata[facet])
                           if 'url' in iddict: ## and iddict['url'] != '':
                               jsondata[facet]=iddict['url']
                           else:
                               jsondata[facet]=''
                       elif facet == 'DOI':
                           iddict = self.map_identifiers(jsondata[facet])
                           if 'DOI' in iddict : 
                               jsondata[facet]=iddict['DOI']
                       elif facet == 'Checksum':
                           jsondata[facet] = self.map_checksum(jsondata[facet])
                       elif facet == 'Discipline':
                           jsondata[facet] = self.map_discipl(jsondata[facet],disctab.discipl_list)
                       elif facet == 'Publisher':
                           blist = self.cut(jsondata[facet],'=',2)
                           jsondata[facet] = self.uniq(blist,';')
                       elif facet == 'Contact':
                           if all(x is None for x in jsondata[facet]):
                               jsondata[facet] = ['Not stated']
                           else:
                               blist = self.cut(jsondata[facet],'=',2)
                               jsondata[facet] = self.uniq(blist,';')
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
                       elif facet == 'PublicationYear':
                           publdate=self.date2UTC(jsondata[facet])
                           if publdate:
                               jsondata[facet] = self.cut([publdate],'\d\d\d\d',0)
                       elif facet == 'fulltext':
                           encoding='utf-8'
                           jsondata[facet] = ' '.join([x.strip() for x in filter(None,jsondata[facet])]).encode(encoding)[:32000]
                   except Exception as e:
                       logging.error(' [WARNING] %s : during mapping of\n\tfield\t%s\n\tvalue%s' % (e,facet,jsondata[facet]))
                       continue

                if iddict :
                    if 'DOI' in iddict : jsondata['DOI']=iddict['DOI']
                    if 'PID' in iddict : jsondata['PID']=iddict['PID']
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
                        data = json.dumps(jsondata,sort_keys = True, indent = 4).decode('utf-8') ## needed, else : Cannot write json file ... : must be unicode, not str
                    except Exception as e:
                        self.logger.error('%s : Cannot decode jsondata %s' % (e,jsondata))
                    try:
                        self.logger.debug('Save json file')
                        json_file.write(data)
                    except TypeError, err :
                        self.logger.error(' %s : Cannot write data in json file %s ' % (jsonfilename,err))
                    except Exception as e:
                        self.logger.error(' %s : Cannot write json file %s' % (e,outpath+'/'+filename))
                        err+='Cannot write json file %s' % jsonfilename
                        results['ecount'] += 1
                        continue
            else:
                results['ecount'] += 1
                continue


        out=' %s to json stdout\nsome stuff\nlast line ..' % infformat
        if (err is not None ): logging.error('[ERROR] ' + err)

        print '   \t|- %-10s |@ %-10s |\n\t| Provided | Mapped | Failed |\n\t| %8d | %6d | %6d |' % ( 'Finished',time.strftime("%H:%M:%S"),
                    results['tcount'],
                    fcount,
                    results['ecount']
                )

        # search in output for result statistics
        last_line = out.split('\n')[-2]
        if ('INFO  Main - ' in last_line):
            string = last_line.split('INFO  Main ')[1]
            [results['count'], results['ecount']] = re.findall(r"\d{1,}", string)
            results['count'] = int(results['count']); results['ecount'] = int(results['ecount'])
        
    
        return results

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
            return urllib2.urlopen(url, timeout=1).getcode() < 501
        except IOError:
            return False
        except urllib2.URLError as e:
            return False    #catched
        except socket.timeout as e:
            return False    #catched
        except ValueError as e:
            return False    #catched
        except Exception as e:
            logging.error("    [ERROR] %s and %s" % (e,traceback.format_exc()))
            return False    #catched

    def is_valid_value(self,facet,valuelist):
        """
        checks if value is the consitent for the given facet
        """
        vall=list()
        if not isinstance(valuelist,list) : valuelist=[valuelist]

        for value in valuelist:
            if facet in ['title','notes','author','Publisher']:
                if isinstance(value, unicode) :
                    try:
                        ## value=value.decode('utf-8')
                        value=value.encode("iso-8859-1")
                    except UnicodeEncodeError as e :
                        self.logger.error("%s : Facet %s with value %s" % (e,facet,value))
                    except Exception as e:
                        logging.error('%s : ( %s:%s )' % (e,facet,value))
                    else:
                        vall.append(value)
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
            ##    print ' Following key-value errors fails validation:\n' + errlist 
            return vall
            
    
    def validate(self,community,mdprefix,path):
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

        results = {
            'count':0,
            'tcount':0,
            'ecount':0,
            'time':0
        }
        
        # check map file
        if mdprefix == 'json' :
            mapext='conf' ##!!!HEW --> json
        else:
            mapext='xml'
        mapfile='%s/mapfiles/%s-%s.%s' % (os.getcwd(),community,mdprefix,mapext)
        if not os.path.isfile(mapfile):
           mapfile='%s/mapfiles/%s.%s' % (os.getcwd(),mdprefix,mapext)
           if not os.path.isfile(mapfile):
              logger.error('Mapfile %s does not exist !' % mapfile)
              return results
        mf=open(mapfile) 

        # check paths
        if not os.path.exists(path):
            logger.error('[ERROR] The directory "%s" does not exist! No files to validate are found!\n(Maybe your convert list has old items?)' % (path))
            return results
        elif not os.path.exists(path + '/json') or not os.listdir(path + '/json'):
            logger.error('[ERROR] The directory "%s/json" does not exist or no json files to validate are found!\n(Maybe your convert list has old items?)' % (path))
            return results
    
        # find all .json files in path/json:
        files = filter(lambda x: x.endswith('.json'), os.listdir(path+'/json'))
        results['tcount'] = len(files)
        oaiset=path.split(mdprefix)[1].strip('/')
        
        self.logger.info(' %s Validation of %d files in %s/json' % (time.strftime("%H:%M:%S"),results['tcount'],path))
        if results['tcount'] == 0 :
            logging.error(' ERROR : Found no files to validate !')
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
            bartags=perc/10
            if perc%10 == 0 and perc != oldperc :
                oldperc=perc
                self.logger.debug("\r\t[%-20s] %d / %d%% in %d sec" % ('='*bartags, fcount, perc, time.time()-start ))
                sys.stdout.flush()

            jsondata = dict()
            self.logger.info('    | v | %-4d | %-s/json/%s |' % (fcount,os.path.basename(path),filename))

            if ( os.path.getsize(path+'/json/'+filename) > 0 ):
                with open(path+'/json/'+filename, 'r') as f:
                    try:
                        jsondata=json.loads(f.read())
                    except:
                        logging.error('    | [ERROR] Cannot load the json file %s' % path+'/json/'+filename)
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
            except IOError, e:
                self.logger.error(" %s in validation of facet '%s' and value '%s' \n" % (e,facet, value))
                exit()

        outfile='%s/%s' % (path,'validation.stat')
        printstats='\n Statistics of\n\tcommunity\t%s\n\tsubset\t\t%s\n\t# of records\t%d\n  see as well %s\n\n' % (community,oaiset,fcount,outfile)  
        printstats+=" |-> {:<16} <-- {:<20} \n  |- {:<10} | {:<9} | \n".format('Facet name','XPATH','Mapped','Validated')
        printstats+="  |-- {:>5} | {:>4} | {:>5} | {:>4} |\n".format('#','%','#','%')
        printstats+="      | Value statistics:\n      |- {:<5} : {:<30} |\n".format('#Occ','Value')
        printstats+=" ----------------------------------------------------------\n"
##HEW-D        for field in self.ckanfields : ## Print better b2findfields ??
        for key,facetdict in self.b2findfields.iteritems() : ###.values() :
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
                                contt='[...(%d chars follow)...]' % restchar 
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
            print printstats

        f = open(outfile, 'w')
        f.write(printstats)
        f.write("\n")
        f.close

        logging.debug('%s     INFO  B2FIND : %d records validated; %d records caused error(s).' % (time.strftime("%H:%M:%S"),fcount,results['ecount']))

        # count ... all .json files in path/json
        results['count'] = len(filter(lambda x: x.endswith('.json'), os.listdir(path)))

        print '   \t|- %-10s |@ %-10s |\n\t| Provided | Validated | Failed |\n\t| %8d | %9d | %6d |' % ( 'Finished',time.strftime("%H:%M:%S"),
                    results['tcount'],
                    fcount,
                    results['ecount']
                )

        return results

    def json2xml(self,json_obj, line_padding="", mdftag="", mapdict="b2findfields"):

        result_list = list()
        json_obj_type = type(json_obj)


        if json_obj_type is list:
            for sub_elem in json_obj:
                print 'SSSSSS  sub_elem %s' %   sub_elem
                result_list.append(json2xml(sub_elem, line_padding, mdftag, mapdict))

            return "\n".join(result_list)

        if json_obj_type is dict:
            for tag_name in json_obj:
                sub_obj = json_obj[tag_name]
                print 'TTTT SSSSSS  sub_obj %s' %   sub_obj
                if tag_name in mapdict : 
                    tag_name=mapdict[tag_name]
                    if not isinstance(tag_name,list) : tag_name=[tag_name]
                    print 'NNNN TTTT SSSSSS  tag_name %s' % tag_name
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
                        logging.debug ('[WARNING] : Field %s can not mapped to B2FIND schema' % tag_name)
                        continue
            
            return "\n".join(result_list)

        return "%s%s" % (line_padding, json_obj)

    def oaiconvert(self,community,mdprefix,path,target_mdschema):
        ## oaiconvert(MAPPER object, community, mdprefix, path) - method
        # Converts the JSON files in directory <path> to XML files in target format (=mdprefix ??)
        # Parameters:
        # -----------
        # 1. (string)   community - B2FIND community of the files
        # 2. (string)   mdprefix - metadata of original harvested source (not needed her)
        # 3. (string)   path - path to subset directory without (!) 'json' subdirectory
        #
        # Return Values:
        # --------------
        # 1. (dict)     results statistics
    
        results = {
            'count':0,
            'tcount':0,
            'ecount':0,
            'time':0
        }
        
        # check paths
        if (target_mdschema):
            path=path+'-'+target_mdschema
        else:
            logging.error('[ERROR] For OAI converter processing target metaschema must be given!')
            sys.exit()

        if not os.path.exists(path):
            logging.error('[ERROR] The directory "%s" does not exist! No files for oai-converting are found!\n(Maybe your convert list has old items?)' % (path))
            return results
        elif not os.path.exists(path + '/json') or not os.listdir(path + '/json'):
            logging.error('[ERROR] The directory "%s/json" does not exist or no json files for converting are found!\n(Maybe your convert list has old items?)' % (path))
            return results
    
        # run oai-converting
        # find all .json files in path/json:
        files = filter(lambda x: x.endswith('.json'), os.listdir(path+'/json'))
        
        results['tcount'] = len(files)

        ##oaiset=path.split(target_mdschema)[0].split('_')[0].strip('/')
        oaiset=os.path.basename(path)
        ## outpath=path.split(community)[0]+'/b2find-oai_b2find/'+community+'/'+mdprefix +'/'+path.split(mdprefix)[1].split('_')[0]+'/xml'
        ##HEW-D outpath=path.split(community)[0]+'b2find-oai_b2find/'+community+'/'+mdprefix +'/xml'
        outpath=path +'/xml'
        if (not os.path.isdir(outpath)):
             os.makedirs(outpath)

        logging.debug(' %s     INFO  OAI-Converter of files in %s/json' % (time.strftime("%H:%M:%S"),path))
        logging.debug('    |   | %-4s | %-40s | %-40s |\n   |%s|' % ('#','infile','outfile',"-" * 53))

        fcount = 0
        oldperc = 0
        start = time.time()
        for filename in files:
            ## counter and progress bar
            fcount+=1
            perc=int(fcount*100/int(len(files)))
            bartags=perc/10
            if perc%10 == 0 and perc != oldperc :
                oldperc=perc
                logging.info("\r\t[%-20s] %d / %d%% in %d sec" % ('='*bartags, fcount, perc, time.time()-start ))
                sys.stdout.flush()

            identifier=oaiset+'_%06d' % fcount
            createdate = str(datetime.datetime.utcnow())
            jsondata = dict()
            logging.debug(' |- %s     INFO  JSON2XML - Processing: %s/json/%s' % (time.strftime("%H:%M:%S"),os.path.basename(path),filename))
            outfile=outpath+'/'+community+'_'+oaiset+'_%06d' % fcount+'.xml'
            logging.debug('    | o | %-4d | %-45s | %-45s |' % (fcount,os.path.basename(filename),os.path.basename(outfile)))

            if ( os.path.getsize(path+'/json/'+filename) > 0 ):
                with open(path+'/json/'+filename, 'r') as f:
                    try:
                        jsondata=json.loads(f.read())
                    except:
                        logging.error('    | [ERROR] Cannot load the json file %s' % path+'/json/'+filename)
                        results['ecount'] += 1
                        continue
            else:
                results['ecount'] += 1
                continue
            
            
            ### oai-convert !!
            if target_mdschema == 'cera':
                convertfile='%s/mapfiles/%s%s.%s' % (os.getcwd(),'json2',target_mdschema,'json')
                with open(convertfile, 'r') as f:
                    try:
                        mapdict=json.loads(f.read())
                    except:
                        logging.error('    | [ERROR] Cannot load the convert file %s' % convertfile)
                        sys.exit()

                    for filetype in ['ds','exp']:
	                ### load xml template
	                templatefile='%s/mapfiles/%s_%s_%s.%s' % (os.getcwd(),target_mdschema,filetype,'template','xml')
	                with open(templatefile, 'r') as f:
	                    try:
	                        dsdata= f.read() ##HEW-D ET.parse(templatefile).getroot()
	                    except Exception as e:
	                        logging.error('    | [ERROR] %s : Cannot load tempalte file %s' % (e,templatefile))
	
	                data=dict()
	                for key in jsondata:
	                        if isinstance(jsondata[key],list) and len(jsondata[key])>0 :
	                            data[key]=' '.join(jsondata[key]).strip('\n ')
	                        else:
	                            data[key]=jsondata[key]
	
	                dsdata=dsdata%data
	                
	                outfile=outpath+'/'+filetype+'_hdcp2_'+data['ds.entry_acronym']+'.xml'
	
	                try:
	                    f = open(outfile, 'w')
	                    f.write(dsdata.encode('utf-8'))
	                    f.write("\n")
	                    f.close
	                except IOError, e:
	                    logging.error("[ERROR] Cannot write data in xml file '%s': %s\n" % (outfile,e))
	                    return(False, outfile , outpath, fcount)
	
            else:
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
                except IOError, e:
                    logging.error("[ERROR] Cannot write data in xml file '%s': %s\n" % (outfile,e))
                    return(False, outfile , outpath, fcount)

        logging.info('%s     INFO  B2FIND : %d records converted; %d records caused error(s).' % (time.strftime("%H:%M:%S"),fcount,results['ecount']))

        # count ... all .xml files in path/b2find
        results['count'] = len(filter(lambda x: x.endswith('.xml'), os.listdir(outpath)))
        print '   \t|- %-10s |@ %-10s |\n\t| Provided | Converted | Failed |\n\t| %8d | %6d | %6d |' % ( 'Finished',time.strftime("%H:%M:%S"),
                    results['tcount'],
                    fcount,
                    results['ecount']
                )
    
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
        print "Dataset is broken or does not pass the B2FIND standard"

    # CHECK DATASET IN CKAN
    ckanstatus = UP.check_dataset(dsname,checksum)

    # UPLOAD DATASET TO CKAN
    upload = UP.upload(dsname,ckanstatus,community,jsondata)
    if (upload == 1):
        print 'Creation of record succeed'
    elif (upload == 2):
        print 'Update of record succeed'
    else:
        print 'Upload of record failed'
    """
    
    def __init__(self, CKAN, OUT):
        ##HEW-D logging = logging.getLogger()
        self.CKAN = CKAN
        self.OUT = OUT
        self.logger = logging.getLogger('root')        

        self.package_list = dict()

        # Read in B2FIND metadata schema and fields
        schemafile =  '%s/mapfiles/b2find_schema.json' % (os.getcwd())
        with open(schemafile, 'r') as f:
            self.b2findfields=json.loads(f.read())

        # B2FIND metadata fields

##HEW-D        self.ckanfields=list()
##HEW-D        for val in self.b2findfields.values() :
##HEW-D            self.ckanfields.append(val["ckanName"])

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
        print 'result %s' % result

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
        print 'query %s' % query
        community_packages = (self.CKAN.action('package_search',{"q":query}))##['results']##['packages']
        print 'comm_packages %s' % community_packages

        # create a new dictionary of it:
        package_list = dict() 
        for ds in community_packages:
            print 'ds %s' % ds
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
        self.logger.debug('    | Adapt default fields for upload to CKAN')
        for key in self.ckandeffields :
            if key not in jsondata:
                self.logger.debug('[WARNING] : CKAN default key %s does not exist' % key)
            else:
                self.logger.debug('    | -- %-25s ' % key)
                if key in  ["author"] :
                    jsondata[key]=';'.join(list(jsondata[key]))
                elif key in ["title","notes"] :
                    jsondata[key]='\n'.join(list(jsondata[key]))
                if key in ["title","author","notes"] : ## HEW-D 1608: removed notes
                    try:
                        self.logger.info('Before encoding :\t%s:%s' % (key,jsondata[key]))
                        ## jsondata[key]=jsondata[key].decode("utf-8") ## encode to display e.g. 'Umlauts' correctly 
                        jsondata[key]=jsondata[key].encode("iso-8859-1") ## encode to display e.g. 'Umlauts' correctly 
                        self.logger.info('After encoding  :\t%s:%s' % (key,jsondata[key]))
                    except UnicodeEncodeError as e :
                        self.logger.error("%s : Facet %s with value %s" % (e,key,jsondata[key]))
                    except Exception as e:
                        self.logger.error('%s : ( %s:%s )' % (e,key,jsondata[key]))
                    finally:
                        pass
                        
        jsondata['extras']=list()
        extrafields=set(self.b2findfields.keys()) - set(self.b2fckandeffields)
        self.logger.debug('    | Append extra fields %s for upload to CKAN' % extrafields)
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
                self.logger.debug('    | %-20s | %-25s' % (key,value))
            else:
                self.logger.info('No data for key %s ' % key)

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
        
        ## check mandatory fields ...
        mandFields=['title','url','oai_identifier']
        for field in mandFields :
            if field not in jsondata: ##  or jsondata[field] == ''):
                raise Exception("The mandatory field '%s' is missing" % field)

        ##HEW-D elif ('url' in jsondata and not self.check_url(jsondata['url'])):
        ##HEW-D     errmsg = "'url': The source url is broken"
        ##HEW-D     if(status > 1): status = 1  # set status
            
        # ... OAI Set
##HEW-?        if('oai_set' in jsondata and ';' in  jsondata['oai_set']):
##HEW-?            jsondata['oai_set'] = jsondata['oai_set'].split(';')[-1] 
            
##HEW-D             # shrink field fulltext
##HEW-D             if('fulltext' in jsondata):
##HEW-D                 encoding='utf-8' ## ?? Best encoding for fulltext ??? encoding='ISO-8859-15'
##HEW-D                 encoded = ' '.join(filter(None,jsondata['fulltext'])).encode(encoding)[:32000]
##HEW-D                 encoded=re.sub('\s+',' ',encoded)
##HEW-D                 jsondata['fulltext']=encoded.decode(encoding, 'ignore')

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
                    raise Exception("Value %s of key %s has incorrect data format, should be YYYY-MM-DDThh:mm:ssZ" % (jsondata[key],key))
                    del jsondata[key] # delete this field from the jsondata

        return jsondata

    def upload(self, ds, dsstatus, community, jsondata):
        ## upload (UPLOADER object, dsname, dsstatus, community, jsondata) - method
        # Uploads a dataset <jsondata> with name <dsname> as a member of <community> to CKAN. 
        #   <dsstatus> describes the state of the package and is 'new', 'changed', 'unchanged' or 'unknown'.         #   In the case of a 'new' or 'unknown' package this method will call the API 'package_create' 
        #   and in the case of a 'changed' package the API 'package_update'. 
        #   Nothing happens if the state is 'unchanged'
        #
        # Parameters:
        # -----------
        # 1. (string)   dsname - Name of the dataset
        # 2. (string)   dsstatus - Status of the dataset: can be 'new', 'changed', 'unchanged' or 'unknown'.
        #                           See also .check_dataset()
        # 3. (string)   dsname - A B2FIND community in CKAN
        # 4. (dict)     jsondata - Metadata fields of the dataset in JSON format
        #
        # Return Values:
        # --------------
        # 1. (integer)  upload result:
        #               0 - critical error occured
        #               1 - no error occured, uploaded with 'package_create'
        #               2 - no error occured, uploaded with 'package_update'
    
        rvalue = 0
        
        # add some general CKAN specific fields to dictionary:
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
    
        rvalue = 0
        
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
   
        # if the dataset exists set it to status deleted in CKAN:
        if (not dsstatus == 'new'):
            self.logger.debug('\t - Try to set dataset %s on status deleted' % dsname)
            
            results = self.CKAN.action('package_update',jsondata)
            if (results and results['success']):
                rvalue = 1
            else:
                self.logger.debug('\t - Deletion failed. Maybe dataset already removed.')
        
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
            return urllib2.urlopen(url, timeout=1).getcode() < 501
        except IOError:
            return False
        except urllib2.URLError as e:
            return False    #catched
        except socket.timeout as e:
            return False    #catched

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
            except OSError, e:
                print("[ERROR] Cannot move log and error files to %s and %s: %s\n" % (logfile,errfile,e))
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
        

        if ('#' in ''.join([request,subset,mode])):
            
            if (request == '#AllRequests'):
                # returns all requests except all which start with an '#'
                return filter(lambda x: not x.startswith('#'), self.stats.keys())
            elif (subset == '#AllSubsets'):
                # returns all subsets except all which start with an '#'
                return filter(lambda x: not x.startswith('#'), self.stats[request].keys())
                
    
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
            elif('#' in mode):
                return self.stats[request][subset][mode]
                
            return self.stats[request][subset]
        
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

        if len(self.get_stats('#AllRequests')) > 0:
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
                            except OSError,e:
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
                            except OSError,e:
                                reshtml.write('No %s error file! <br /><small><small>(<i>%s</i>)</small></small><br />'% (pstat['short'][mode], e))
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
        
        if (fromdate == None):
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
            except IOError as (errno, strerror):
                logging.critical("Cannot read data from '{0}': {1}".format(self.convert_list, strerror))
                f.close

        try:
            f = open(self.convert_list, 'w')
            f.write(file)
            f.close()
        except IOError as (errno, strerror):
            logging.critical("Cannot write data to '{0}': {1}".format(self.convert_list, strerror))
            f.close
