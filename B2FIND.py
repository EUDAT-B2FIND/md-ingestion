"""B2FIND.py - classes for B2FIND management : 
  - CKAN_CLIENT Executes CKAN APIs (interface to CKAN)
  - HARVESTER   Harvests from a OAI-PMH server
  - CONVERTER   Converts XML files to JSON files with Lari's converter and by using mapfiles
  - UPLOADER    Uploads JSON files to CKAN portal
  - POSTPROCESS Run postprocess routines on JSON files
  - OUTPUT      Initializes the logger class and provides methods for saving log data and for printing those.    

Install required modules as simplejson, e.g. by :

  > sudo pip install <module>

Copyright (c) 2014 John Mrziglod (DKRZ)
Modified      2015 Heinrich Widmann (DKRZ)

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

This is a prototype and not ready for production use.

"""

# system relevant modules:
import os, glob, sys
import time, datetime, calendar, subprocess

# program relevant modules:
import logging as log
import traceback
import re

# needed for HARVESTER class:
import sickle as SickleClass
from sickle.oaiexceptions import NoRecordsMatch
import uuid, hashlib
import lxml.etree as etree

# needed for CKAN_CLIENT
import urllib, urllib2, socket
import httplib
from urlparse import urlparse

# needed for CONVERTER :
from babel.dates import format_datetime
import codecs
import simplejson as json
import csv, io
import Levenshtein as lvs
import iso639

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
	    self.logger = log.getLogger()
	
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
	            self.logger.info('\tTry to activate object: ' + str(dataset))
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
        data_string = urllib.quote(json.dumps(data_dict))

        self.logger.debug('\t|-- Action %s\n\t|-- Calling %s\n\t|-- Object %s ' % (action,action_url,data_dict))	
        try:
            request = urllib2.Request(action_url)
            if (self.api_key): request.add_header('Authorization', self.api_key)
            response = urllib2.urlopen(request,data_string)
        except urllib2.HTTPError as e:
            self.logger.debug('\tHTTPError %s : The server %s couldn\'t fulfill the action %s.' % (e.code,self.ip_host,action))
            if ( e.code == 403 ):
                self.logger.error('\tHTTPError %s :Access forbidden, maybe the API key is not valid?' % e.code)
                exit(e.code)
            elif ( e.code == 409 and action == 'package_create'):
                print self.logger.debug('\tHTTPError %s :Maybe the dataset already exists or you have a parameter error?' % e.code)
                self.action('package_update',data_dict)
                return {"success" : False}
            elif ( e.code == 409):
                self.logger.error('\tHTTPError %s :Maybe you have a parameter error?' % e.code)
                return {"success" : False}
            elif ( e.code == 500):
                self.logger.error('\tHTTPError %s :Internal server error' % e.code)
                ##exit(e.code)
                return {"success" : False}
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
        self.logger = log.getLogger()
        self.pstat = pstat
        self.OUT = OUT
        self.base_outdir = base_outdir
        self.fromdate = fromdate
        
    
    def harvest(self, request):
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
        
        
        
            def JSONAPI(self, action, offset): # see oaireq
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
        	    
        	    return self.__action_api(action, offset)
        		
        
        
            def __action_api (self, action, offset):
                # Make the HTTP request for get datasets from GBIF portal
                response=''
                rvalue = 0
                ## offset = 0
                limit=100
                api_url = "http://api.gbif.org/v1"
                action_url = "{apiurl}/dataset?offset={offset}&limit={limit}".format(apiurl=api_url,offset=str(offset),limit=str(limit))	# default for get 'dataset'
               # normal case:
               ###  action_url = "http://{host}/api/3/action/{action}".format(host=self.ip_host,action=action)
        
               # make json data in conformity with URL standards
               ## data_string = urllib.quote(json.dumps(data_dict))
        
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

        # create sickle object
        sickle = SickleClass.Sickle(req['url'], max_retries=3, timeout=300)

        # create GBIF object 
        GBIF = GBIF_CLIENT(req['url'])
     
        requests_log = log.getLogger("requests")
        requests_log.setLevel(log.WARNING)
        
        # if the number of files in a subset dir is greater than <count_break>
        # then create a new one with the name <set> + '_' + <count_set>
        count_break = 5000
        count_set = 1
       
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
        
        # Get all files in the current subset directories and put those in the dictionary deleted_metadata
        deleted_metadata = dict()
   
        self.logger.info('    |   | %-4s | %-45s | %-45s |\n    |%s|' % ('#','OAI Identifier','DS Identifier',"-" * 106))

        try:
          if req["lverb"] == 'JSONAPI':
            for s in glob.glob('/'.join([self.base_outdir,req['community']+'-'+req['mdprefix'],subset+'_[0-9]*'])):
              for f in glob.glob(s+'/hjson/*.json'):
                # save the uid as key and the subset as value:
                deleted_metadata[os.path.splitext(os.path.basename(f))[0]] = f
            noffs=0
            data = GBIF.JSONAPI('package_list',noffs)
            while(not data['endOfRecords']): ## and nj<10):
              ## 
              print 'data-end-of-rec %s' % data['endOfRecords']
              for record in data['results']:
              ## for record in GBIF.JSONAPI('package_list',noffs)['results']:
                ## how to handle 'deleted' records ???

                stats['tcount'] += 1

                # set oai_id and generate a uniquely identifier for this dataset:
                oai_id = record['key']
                uid = str(uuid.uuid5(uuid.NAMESPACE_DNS, oai_id.encode('ascii','replace')))

                jsonfile = subsetdir + '/hjson/' + os.path.basename(uid) + '.json'
                try:
                    self.logger.info('    | h | %-4d | %-45s | %-45s |' % (stats['count']+1,record['key'],uid))
                    self.logger.debug('Try to write the harvested JSON record to %s' % jsonfile)
                    
                    # get the raw json content:    
                    if (record is not None):
                        ###metadata = etree.tostring(metadata, pretty_print = True) 
                        ###metadata = metadata.encode('ascii', 'ignore')
                        if (not os.path.isdir(subsetdir+'/hjson')):
                           os.makedirs(subsetdir+'/hjson')
                           
                        # write metadata in file:
                        try:
                            with open(jsonfile, 'w') as f:
                              json.dump(record,f, sort_keys = True, indent = 4)
                        except IOError, e:
                            self.logger.error("[ERROR] Cannot write metadata in json file '%s': %s\n" % (jsonfile,e))
                            stats['ecount'] +=1
                            continue
                        
                        stats['count'] += 1
                        self.logger.debug('Harvested JSON file written to %s' % jsonfile)
                        
                        # Need a new subset?
                        if (stats['count'] == count_break):
                        
                            # save the stats of the old subset and get the new subsetdir:
                            subsetdir, count_set = self.save_subset(
                                req, stats, subset, subsetdir, count_set)
                            
                            self.logger.info('    | subset ( %d records) harvested in %s (if not failed).'% (
                                stats['count'], subsetdir
                            ))
                            
                            # add all subset stats to total stats and reset the temporal subset stats:
                            for key in ['tcount', 'ecount', 'count', 'dcount']:
                                stats['tot'+key] += stats[key]
                                stats[key] = 0

                            # start with a new time:
                            stats['timestart'] = time.time()
                    else:
                        stats['ecount'] += 1
                        self.logger.warning('    [WARNING] No metadata available for %s' % record['key'])
                except TypeError as e:
                    self.logger.error('    [ERROR] TypeError: %s' % e)
                    stats['ecount']+=1        
                    continue
                except Exception as e:
                    self.logger.error("    [ERROR] %s and %s" % (e,traceback.format_exc()))
                    ## self.logger.debug(metadata)
                    stats['ecount']+=1
                    continue
                else:
                    # if everything worked then delete this metadata file from deleted_metadata
                    if uid in deleted_metadata:
                        del deleted_metadata[uid]
              noffs+=100
              data = GBIF.JSONAPI('package_list',noffs)
                
          else:  ## OAI-PMH harvesting of XML records using Python Sickle module
            for s in glob.glob('/'.join([self.base_outdir,req['community']+'-'+req['mdprefix'],subset+'_[0-9]*'])):
              for f in glob.glob(s+'/xml/*.xml'):
                # save the uid as key and the subset as value:
                deleted_metadata[os.path.splitext(os.path.basename(f))[0]] = f
            oaireq=getattr(sickle,req["lverb"], None)
            for record in oaireq(**{'metadataPrefix':req['mdprefix'],'set':req['mdsubset'],'ignore_deleted':False,'from':self.fromdate}):

                if req["lverb"] == 'ListIdentifiers' :
                    if (record.deleted):
                       continue
                    else:
                       oai_id = record.identifier
                       record = sickle.GetRecord(**{'metadataPrefix':req['mdprefix'],'identifier':record.identifier})
                elif req["lverb"] == 'ListRecords' :
            	    if (record.header.deleted):
            	       continue
                    else:
                       oai_id = record.header.identifier

                stats['tcount'] += 1

                # generate a uniquely identifier for this dataset:
                uid = str(uuid.uuid5(uuid.NAMESPACE_DNS, oai_id.encode('ascii','replace')))
                
                xmlfile = subsetdir + '/xml/' + os.path.basename(uid) + '.xml'
                try:
                    self.logger.info('    | h | %-4d | %-45s | %-45s |' % (stats['count']+1,oai_id,uid))
                    self.logger.debug('Harvested XML file written to %s' % xmlfile)
                    
                    # get the raw xml content:    
                    metadata = etree.fromstring(record.raw)
                    if (metadata is not None):
                        metadata = etree.tostring(metadata, pretty_print = True) 
                        metadata = metadata.encode('ascii', 'ignore')
                        if (not os.path.isdir(subsetdir+'/xml')):
                           os.makedirs(subsetdir+'/xml')
                           
                        # write metadata in file:
                        try:
                            f = open(xmlfile, 'w')
                            f.write(metadata)
                            f.close
                        except IOError, e:
                            self.logger.error("[ERROR] Cannot write metadata in xml file '%s': %s\n" % (xmlfile,e))
                            stats['ecount'] +=1
                            continue
                        
                        stats['count'] += 1
                        self.logger.debug('Harvested XML file written to %s' % xmlfile)
                        
                        # Need a new subset?
                        if (stats['count'] == count_break):
                        
                            # save the stats of the old subset and get the new subsetdir:
                            subsetdir, count_set = self.save_subset(
                                req, stats, subset, subsetdir, count_set)
                            
                            self.logger.info('    | subset ( %d records) harvested in %s (if not failed).'% (
                                stats['count'], subsetdir
                            ))
                            
                            # add all subset stats to total stats and reset the temporal subset stats:
                            for key in ['tcount', 'ecount', 'count', 'dcount']:
                                stats['tot'+key] += stats[key]
                                stats[key] = 0
                            
                            # start with a new time:
                            stats['timestart'] = time.time()
                    else:
                        stats['ecount'] += 1
                        self.logger.warning('    [WARNING] No metadata available for %s' % oai_id)
                            
                        
                except TypeError as e:
                    self.logger.error('    [ERROR] TypeError: %s' % e)
                    stats['ecount']+=1        
                    continue
                except Exception as e:
                    self.logger.error("    [ERROR] %s and %s" % (e,traceback.format_exc()))
                    ## self.logger.debug(metadata)
                    stats['ecount']+=1
                    continue
                else:
                    # if everything worked then deleted this metadata file from deleted_metadata
                    if uid in deleted_metadata:
                        del deleted_metadata[uid]
        except TypeError as e:
            self.logger.error('    [ERROR] Type Error: %s' % e)
        except NoRecordsMatch as e:
            self.logger.error('    [ERROR] No Records Match: %s. Request: %s' % (e,','.join(request)))
        except Exception as e:
            self.logger.error("    [ERROR] %s" % traceback.format_exc())
        else:

            # check for outdated harvested xml files and add to deleted_metadata, if not already listed
            now = time.time()
            for s in glob.glob('/'.join([self.base_outdir,req['community']+'-'+req['mdprefix'],subset+'_[0-9]*'])):
               for f in glob.glob(s+'/xml/*.xml'):
                 ##HEW-T print 'date %s of file %s' % (os.stat(f).st_mtime,f)
                 if os.stat(f).st_mtime < now - 7 * 86400:
                     if os.path.isfile(f):
                        if (deleted_metadata[os.path.splitext(os.path.basename(f))[0]]):
                           print 'file %s is already on deleted_metadata' % f
                        else:
                           deleted_metadata[os.path.splitext(os.path.basename(f))[0]] = f
                        print 'oudated %s file %s' % (os.stat(f).st_mtime,f)
                        ## os.remove(os.path.join(path, f))

            if (len(deleted_metadata) > 0): ##HEW and self.pstat['status']['d'] == 'tbd':
                ## delete all files in deleted_metadata and write the subset
                ## and the uid in '<outdir>/delete/<community>-<mdprefix>':
                self.logger.info('    | These [%d] files were not updated and will be deleted:' % (len(deleted_metadata)))
                
                # path to the file with all deleted uids:
                delete_file = '/'.join([self.base_outdir,'delete',req['community']+'-'+req['mdprefix']+'.del'])
                file_content = ''
                
                if(os.path.isfile(delete_file)):
                    try:
                        f = open(delete_file, 'r')
                        file_content = f.readlines()
                        f.close()
                    except IOError as (errno, strerror):
                        self.logger.critical("Cannot read data from '{0}': {1}".format(delete_file, strerror))
                        f.close
                elif (not os.path.exists(self.base_outdir+'/delete')):
                    os.makedirs(self.base_outdir+'/delete')    

                try:
                  # add all deleted metadata to the file, subset in the 1. column and id in the 2. column:
                  for uid in deleted_metadata:
                    self.logger.info('    | d | %-4d | %-45s |' % (stats['totdcount'],uid))
                    
                    xmlfile = deleted_metadata[uid]
                    dsubset = os.path.dirname(xmlfile).split('/')[-2]
                    jsonfile = '/'.join(xmlfile.split('/')[0:-2])+'/json/'+uid+'.json'
                
                    fline = '%s\t%s\n' % (dsubset,uid)
                    stats['totdcount'] += 1
                    
                    # remove xml file:
                    try: 
                        ## os.remove(xmlfile)
                        print 'rm xmlf'
                    except OSError, e:
                        self.logger.error("    [ERROR] Cannot remove xml file: %s" % (e))
                        stats['totecount'] +=1
                        
                    # remove json file:
                    if (os.path.exists(jsonfile)):
                        try: 
                            os.remove(jsonfile)
                        except OSError, e:
                            self.logger.error("    [ERROR] Cannot remove json file: %s" % (e))
                            stats['totecount'] +=1
                
                    # write fline in delete file:
                    found=False
                    for line in file_content:
                         if fline in line:
                           print "Found it"
                           found = True
                    if not found:
                         with open(delete_file, 'a') as file:
                           file.write(fline)                        


                except IOError as strerror:
                   self.logger.critical("Cannot write data to '{0}': {1}".format(delete_file, strerror))
        

            # add all subset stats to total stats and reset the temporal subset stats:
            for key in ['tcount', 'ecount', 'count', 'dcount']:
                stats['tot'+key] += stats[key]
            
            self.logger.info(
                '\n## Harvesting finished:\n # | Provided | Harvested | Failed | Deleted | \n # | %8d | %9d | %6d | %7d |' 
                % (
                    stats['tottcount'],
                    stats['totcount'],
                    stats['totecount'],
                    stats['totdcount']
                ))
        
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


class CONVERTER(object):

    """
    ### CONVERTER - class
    # Convert XML files to JSON files with Lari's java converter in md-mapper
    #
    # Parameters:
    # -----------
    # 1. (OUT object)   OUT - object of the OUTPUT class
    # 2. * (path)       root  - path to java converter directory
    #
    # Return Values:
    # --------------
    # 1. CONVERTER object
    #
    # Public Methods:
    # ---------------
    # .convert(community, mdprefix, path)  - Convert all files in <path> to JSON format by using mapfiles in md-mapping. 
    #                                        Store those files in a parallel subdirectory '../json'
    #
    # Usage:
    # ------

    # create CONVERTER object:
    CV = CONVERTER(OUT)

    path = 'oaidata/enes-iso/subset1'
    community = 'enes'
    mdprefix  = 'iso'

    # convert all files of the 'xml' dir in <path> by using mapfile which is defined by <community> and <mdprefix>
    results = CV.convert(community,mdprefix,path)
    """

    def __init__ (self, OUT, root='../mapper/current'):
        self.logger = log.getLogger()
        self.root = root
        self.OUT = OUT
        
        if (not os.path.exists(root)):
            self.logger.critical('[CRITICAL] "%s" does not exist! No converter script is found!' % (root))
            exit()
    
        # searching for all java class packages in root/lib
        self.cp = ".:"+":".join(filter(lambda x: x.endswith('.jar'), os.listdir(root+'/lib')))
        
        # get the java converter name:
        self.program = (filter(lambda x: x.endswith('.jar') and x.startswith('md-mapper-'), os.listdir(root)))[0]
        
    class cv_diciplines(object):
        """
        This class represents the closed vocabulary used for the mapoping of B2FIND discipline mapping
        Copyright (C) 2014 Heinrich Widmann.

        """
        def __init__(self):
##           self.discipl_file =  '%s/%s/mapfiles/b2find_disciplines.tab' % (os.getcwd(),CONVERTER.root)
           self.discipl_list = self.get_list()

        @staticmethod
        def get_list():
            import csv
            import os
            root='../mapper/current'
            discipl_file =  '%s/%s/mapfiles/b2find_disciplines.tab' % (os.getcwd(),root)
            disctab = []
            with open(discipl_file, 'r') as f:
                ## define csv reader object, assuming delimiter is tab
                tsvfile = csv.reader(f, delimiter='\t')

                ## iterate through lines in file
                for line in tsvfile:
                   disctab.append(line)
                   
            return disctab

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
        utc = re.compile(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z')
        # UTC2 format =  YYYY-MM-DDThh:mm:ss.mss+mmss
        utcn = re.compile(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}\+\d{4}')
       
        utc_day = re.compile(r'\d{4}-\d{2}-\d{2}') # day (YYYY-MM-DD)
        utc_year = re.compile(r'\d{4}') # year (4-digit number)
        if utc.search(old_date):
            new_date = utc.search(old_date).group()
            return new_date
        elif utc_day.search(old_date):
            day = utc_day.search(old_date).group()
            new_date = day + 'T11:59:59Z'
            return new_date
        elif utc_year.search(old_date):
            year = utc_year.search(old_date).group()
            new_date = year + '-07-01T11:59:59Z'
            return new_date
        else:
            return '' # if converting cannot be done, make date empty

    def replace(self,setname,dataset,facetName,old_value,new_value):
        """
        replaces old value - can be a regular expression - with new value for a given facet
        """

        old_regex = re.compile(old_value)

        for facet in dataset:
            if facet == facetName :
               if setname != '*' or re.match(old_regex, dataset[facet]):
                  dataset[facet] = new_value
                  return dataset
            if facet == 'extras':
                for extra in dataset[facet]:
                    if extra['key'] == facetName :
                       if setname != '*' or re.match(old_regex, extra['value']) :
                          extra['value'] = new_value
                          return dataset
        return dataset
 
    def map_identifier(self, invalue):
        """
        Convert identifiers to data access links, i.e. to 'Source' (ds['url']) or 'PID','DOI' etc. pp
 
        Copyright (C) 2015 by heinrich Widmann.
        Licensed under AGPLv3.
        """

        idarr=invalue.split(';')
        iddict=dict()

        for id in idarr :
          if id.startswith('ivo:'):
             iddict['IVO']='http://registry.astrogrid.org/astrogrid-registry/main/tree'+id[len('ivo:'):]
             favurl=iddict['IVO']
          elif id.startswith('10.1594'):
             iddict['DOI'] = self.concat('http://dx.doi.org/',id)
             favurl=iddict['DOI']
          elif 'doi:' in id:
             iddict['DOI'] = id
             favurl=iddict['DOI']
          else:
             ## check_url !!
             iddict['url']=id

        if not 'url' in iddict :
             iddict['url']=favurl
          

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
            if len(language) == 2:
                try: return iso639.languages.get(alpha2=language.lower())
                except KeyError: pass
            elif len(language) == 3:
                try: return iso639.languages.get(alpha3=language.lower())
                except KeyError: pass
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

        ## for facet in dataset:
        ##    if facet == 'extras':
        ##        for extra in dataset[facet]:
        ##            if extra['key'] == 'Language':
        mcountry = mlang(invalue)
        if mcountry:
            newvalue = mcountry.name
            return newvalue
        return None
 
    def map_geonames(self,invalue):
        """
        Map geonames to coordinates
 
        Copyright (C) 2014 Heinrich Widmann
        Licensed under AGPLv3.
        """
        from geopy.geocoders import Nominatim
        geolocator = Nominatim()
        try:
          location = geolocator.geocode(invalue.split(';')[0])
          if not location :
            return (None,None)
        except Exception, e:
           self.logger.error('[ERROR] : %s - in map_geonames %s can not converted !' % (e,invalue.split(';')[0]))
           return (None,None)
        else:
          return (location.latitude, location.longitude)

    def map_temporal(self,invalue):
        """
        Map date-times to B2FIND start and end time
 
        Copyright (C) 2015 Heinrich Widmann
        Licensed under AGPLv3.
        """
        try:
          if type(invalue) is dict :
            if invalue["start"] and invalue["end"] :
               return (self.date2UTC(invalue["start"]),self.date2UTC(invalue["end"]))
          else:
            return
        except Exception, e:
           self.logger.error('[ERROR] : %s - in map_temporal %s can not converted !' % (e,invalue))
           return (None,None)

    def map_spatial(self,invalue):
        """
        Map coordinates to spatial
 
        Copyright (C) 2014 Heinrich Widmann
        Licensed under AGPLv3.
        """
        try:
          if type(invalue) is dict :
            if invalue["boundingBox"] :
               coordict=invalue["boundingBox"]
               return (coordict["minLatitude"],coordict["maxLongitude"],coordict["maxLatitude"],coordict["minLongitude"])
          else:
            coordarr=invalue.split()
            for coord in coordarr:
              try:
                float(coord)
              except ValueError:
                return (None,None,None,None)
            if len(coordarr)==2 :
              return(coordarr[0],coordarr[1],coordarr[0],coordarr[1])
            elif  len(coordarr)==4 :
              return(coordarr[0],coordarr[1],coordarr[2],coordarr[3])
            else:
              return
##HEW-D          elif:
##HEW-D            lat,lon=self.map_geonames(extra['value'])
        except Exception, e:
           self.logger.error('[ERROR] : %s - in map_spatial %s can not converted !' % (e,invalue))
           return (None,None,None,None) 

    def map_discipl(self,invalue,disctab):
        """
        Convert disciplines along B2FIND disciplinary list
 
        Copyright (C) 2014 Heinrich Widmann
        Licensed under AGPLv3.
        """
        
        invalue=invalue.encode('ascii','ignore').capitalize()
        maxr=0.0
        for line in disctab :
            disc='%s' % line[3]
            r=lvs.ratio(invalue,disc.title())
            ##if r > 0.7 :
            ##  print '--- %s \n|%s|%s| %f | %f' % (line,invalue,disc,r,maxr)
            if r > maxr  :
                maxdisc=disc
                maxr=r
        if maxr == 1 and invalue == maxdisc :
            self.logger.debug('  | Perfect match of %s : nothing to do' % invalue)
        elif maxr > 0.98 :
            self.logger.info('   | Similarity ratio %f is > 0.98 : replace value >>%s<< with best match --> %s' % (maxr,invalue,maxdisc))
            return maxdisc
        elif maxr > 0.7 :
            self.logger.debug('   | Similarity ratio %f is > 0.7 : compare value >>%s<< and discipline >>%s<<' % (maxr,invalue,maxdisc))
            return None
        else:
            self.logger.debug('   | Similarity ratio %f is < 0.7 compare value >>%s<< and discipline >>%s<<' % (maxr,invalue,maxdisc))
            return None
        return invalue
        
    def cut(self,invalue,pattern,nfield):
        """
        splits invalue according to delimiter and cuts out field number nfield.
        If delimiter is not in invalue truncate from character nfield.

        Copyright (C) 2015 Heinrich Widmann.
        Licensed under AGPLv3.
        """

        if pattern in invalue:
           return invalue.split(pattern)[0]
        elif nfield:
           return invalue[:nfield]
        elif re.findall(pattern, invalue):
           ## e.g. pattern = '\d+' or '\d\d\d\d'
           return re.findall(pattern, invalue)[0]
        
        return invalue

    def list2dictlist(self,invalue,valuearrsep):
        """
        transfer list of strings to list of dict's { "name" : "substr1" }      
        """

        if len(invalue) == 1 :
            valarr=invalue[0]['name'].split(valuearrsep)
            ##valarr=list(OrderedDict.fromkeys(valarr)) ## this eliminates real duplicates
            valarr=list(set(valarr)) ## this eliminates real duplicates
            dictlist=[]
            for entry in valarr:
               entrydict={ "name": entry }  
               dictlist.append(entrydict)
        else:
            return invalue
        return dictlist

    def uniq(self,input):
        output = []
        for x in input:
            if x not in output:
                output.append(x)
        return output


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
        utc=self.date2UTC(dt)
        try:
           utctime = datetime.datetime.strptime(utc, "%Y-%m-%dT%H:%M:%SZ") ##HEW-?? .isoformat()
           ##utctime = format_datetime(utc, "%Y-%m-%dT%H:%M:%S", locale='en')
           sec=int(time.mktime(utctime.timetuple()))+year1epochsec
        except Exception, e:
           self.logger.error('[ERROR] : %s - in utc2seconds date-time %s can not converted !' % (e,utc))
           return False

        return sec

    def remove_duplicates(self,dataset,facetName,valuearrsep,entrysep):
        """
        remove duplicates      
        """
        for facet in dataset:
          if facet == facetName:
            valarr=dataset[facet].split(valuearrsep)
            valarr=list(OrderedDict.fromkeys(valarr)) ## this elimintas real duplicates
            nvalarr=valarr
            revvalarr=[]
            for entry in valarr:
              reventry=entry.split(entrysep)
              if (len(reventry) > 1): 
                reventry.reverse()
                reventry=''.join(reventry)
                revvalarr.append(reventry)
                for reventry in revvalarr:
                  if reventry == entry :
                     nvalarr.remove(reventry)
            dataset[facet]=valuearrsep.join(nvalarr)
        return dataset       
      
    def splitstring2dictlist(self,dataset,facetName,valuearrsep,entrysep):
        """
        split string in list of string and transfer to list of dict's { "name" : "substr1" }      
        """
        na_arr=['not applicable']
        for facet in dataset:
          if facet == facetName and len(dataset[facet]) == 1 :
            valarr=dataset[facet][0]['name'].split(valuearrsep)
            valarr=list(OrderedDict.fromkeys(valarr)) ## this elimintas real duplicates
            dicttagslist=[]
            for entry in valarr:
               if entry in na_arr : continue
               entrydict={ "name": entry }  
               dicttagslist.append(entrydict)
       
            dataset[facet]=dicttagslist
        return dataset       


    def changeDateFormat(self,dataset,facetName,old_format,new_format):
        """
        changes date format from old format to a new format
        current assumption is that the old format is anything (indicated in the config file 
        by * ) and the new format is UTC
        """
        for facet in dataset:
            if self.str_equals(facet,facetName) and old_format == '*':
                if self.str_equals(new_format,'UTC'):
                    old_date = dataset[facet]
                    new_date = date2UTC(old_date)
                    dataset[facet] = new_date
                    return dataset
            if facet == 'extras':
                for extra in dataset[facet]:
                    if self.str_equals(extra['key'],facetName) and old_format == '*':
                        if self.str_equals(new_format,'UTC'):
                            old_date = extra['value']
                            new_date = self.date2UTC(old_date)
                            extra['value'] = new_date
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
    
           # XXX wrap this in a try??
           trace(cleaned_expr, obj, '$')

           ##HEW-T print 'result %s' % result    
           if len(result) > 0:
               return result
       return False
    

    def jsonmdmapper(self,dataset,jrules):
        """
        changes JSON dataset field values according to configuration
        """  
        format = 'VALUE'
        newds=dict()
        newds['extras']=[]
      
        for rule in jrules:
           ##HEW-T print 'rule %s' % rule.strip('\n')
           if rule.startswith('#'):
             continue
           field=rule.strip('\n').split(' ')[0]
           jpath=rule.strip('\n').split(' ')[1]

           if not jpath.startswith('$') :
              value=jpath
           else:
              try: 
                result=self.jsonpath(dataset, jpath, format)      
                if isinstance(result, (list, tuple)) and (len(result)>0):
                     if (len(result)==1):
                        value=self.jsonpath(dataset, jpath, format)[0]
                     else:
                        value=self.jsonpath(dataset, jpath, format)
                else:
                     continue
              except Exception as e:
                self.logger.error(' %s:[ERROR] %s : processing rule %s : %s : %s' % (self.jsonmdmapper.__name__,e,field,jpath,value))
                continue

           if (field.split('.')[0] == 'extras'): # append extras field
              newds['extras'].append({"key": field.split('.')[1], "value": value})
           else: # default field
              newds[field]=value

###        else:
###                   continue

###
###
###
#####                   print 'extr1 %s' % field.split('.')[0]
###                   if (field.split('.')[0] == 'extras'):
###                     if (len(result)==1):
###                        newds['extras'].append({"key": field.split('.')[1], "value": self.jsonpath(dataset, jpath, format)[0]})
###                     else:
###                        newds['extras'].append({"key": field.split('.')[1], "value": self.jsonpath(dataset, jpath, format)})                        
###                   else:
###                     if (len(result)==1):
###                        newds[field]=self.jsonpath(dataset, jpath, format)[0]
###                     else:
###                        newds[field]=self.jsonpath(dataset, jpath, format)
###                else:
###                   continue
###                   ##print ' | %10s | %10s | %10s | \n' % (field,jpath,newds[field])
###              except:
###                log.info('    | [ERROR] processing rule %s : %s : %s' % (field,jpath,value))
###                continue        
###        ##HEW-T print 'newds %s' % newds
        return newds
      
    def postprocess(self,dataset,rules):
        """
        changes dataset field values according to configuration
        """  
    
        for rule in rules:
          try: 
            # rules can be checked for correctness
            assert(rule.count(',,') == 5),"a double comma should be used to separate items in rule"
            
            rule = rule.rstrip('\n').split(',,') # splits  each line of config file
            groupName = rule[0]
            setName = rule[1]
            facetName = rule[2]
            old_value = rule[3]
            new_value = rule[4]
            action = rule[5]
                        
            r = dataset.get("extras",None)
            if filter(lambda extra: extra['key'] == 'oai_set', r):
              oai_set=filter(lambda extra: extra['key'] == 'oai_set', r)[0]['value']
    
            ## call action
            if action == "replace":
                dataset = self.replace(setName,dataset,facetName,old_value,new_value)
##            elif action == "truncate":
##                dataset = self.truncate(dataset,facetName,old_value,new_value)
            elif action == "changeDateFormat":
                dataset = self.changeDateFormat(dataset,facetName,old_value,new_value)
            elif action == 'remove_duplicates':
                dataset = self.remove_duplicates(dataset,facetName,old_value,new_value)
            elif action == 'splitstring2dictlist':
                dataset = self.splitstring2dictlist(dataset,facetName,old_value,new_value)
            elif action == "another_action":
                pass
            else:
                pass
          except Exception as e:
            self.logger.error(" [ERROR] %s : processing rule %s" % (e,rule))
            continue

        return dataset
    
    def convert(self,community,mdprefix,path):
        ## convert (CONVERTER object, community, mdprefix, path) - method
        # Converts the XML files in directory <path> to JSON files with Lari's java converter by using mapfile
        # which is defined by <community> and <mdprefix>. 
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
        
        # check mdprefix and in paths
        if ( mdprefix == 'json' ): # convert harvested json records using jsonpath rules
          # check JPATH mapfile
          jmapfile='%s/%s/mapfiles/%s-%s.conf' % (os.getcwd(),self.root,community,mdprefix)
          if not os.path.isfile(jmapfile):
              jmapfile='%s/%s/mapfiles/%s.xml' % (os.getcwd(),self.root,mdprefix)
              if not os.path.isfile(jmapfile):
                self.logger.error('[ERROR] JSON2JSON Mapfile %s does not exist !' % jmapfile)
                return results
          # read map file 
          self.logger.debug('[INFO]: Run JSON2JSON converter with json mapfile %s' % jmapfile)
          f = codecs.open(jmapfile, "r", "utf-8")
          jrules = f.readlines() # without the header
          jrules = filter(lambda x:len(x) != 0,jrules) # removes empty lines


          # make directory for mapped json's
          if (not os.path.isdir(path+'/json')):
             os.makedirs(path+'/json')

          # loop over all .json files in path/hjson (harvested json records):
          results['tcount'] = len(filter(lambda x: x.endswith('.json'), os.listdir(path+'/hjson')))
          files = filter(lambda x: x.endswith('.json'), os.listdir(path+'/hjson'))
          results['tcount'] = len(files)
          fcount = 0
          err=None
          self.logger.info(' %s     INFO  JSONPATH - Processing of files in %s/hjson' % (time.strftime("%H:%M:%S"),path))
 
          for filename in files:
              fcount+=1
              hjsondata = dict()
              jsondata = dict()
        
              if ( os.path.getsize(path+'/hjson/'+filename) > 0 ):
                with open(path+'/hjson/'+filename, 'r') as f:
                   try:
                        hjsondata=json.loads(f.read())
                   except Exception as e:
                        log.error('    | [ERROR] %s : Cannot load json file %s' % (e,path+'/hjson/'+filename))
                        results['ecount'] += 1
                        continue
                   try:
                       # Run json2json converter
                       self.logger.info(' |- %s    INFO J2J FileProcessor - Processing: %s/hjson/%s' % (time.strftime("%H:%M:%S"),os.path.basename(path),filename))
                       jsondata=self.jsonmdmapper(hjsondata,jrules)
                   except Exception as e:
                       log.error('    | [ERROR] %s : during json 2 json processing' % e )
                       results['ecount'] += 1
                       exit()
                       continue

                   with io.open(path+'/json/'+filename, 'w') as json_file:
                       try:
                           log.debug('   | [INFO] decode json data')
                           data = json.dumps(jsondata,sort_keys = True, indent = 4).decode('utf8')
                       except Exception as e:
                          log.error('    | [ERROR] %s : Cannot decode jsondata %s' % (e,jsondata))
                       try:
                          log.debug('   | [INFO] save json file')
                          json_file.write(data)
                       except TypeError, err :
                          # Decode data to Unicode first
                          log.error('    | [ERROR] Cannot write json file %s : %s' % (path+'/json/'+filename,err))
##                        json_file.write(data.decode('utf8'))
##                     try:
##                        json.dump(jsondata,json_file, sort_keys = True, indent = 4, ensure_ascii=False)
                       except Exception as e:
                          log.error('    | [ERROR] %s : Cannot write json file %s' % (e,path+'/json/'+filename))
                          err+='Cannot write json file %s' % path+'/json/'+filename
                          results['ecount'] += 1
                          continue
              else:
                results['ecount'] += 1
                continue

          out=' JSON2JSON stdout\nsome stuff\nlast line ..'
          # check output and print it
          ##HEW-D self.logger.info(out)
          if (err is not None ): self.logger.error('[ERROR] ' + err)
          ##exit()
        else: # convert xml records using XPATH rules
          if not os.path.exists(path):
              self.logger.error('[ERROR] The directory "%s" does not exist! No files for converting are found!\n(Maybe your convert list has old items?)' % (path))
              return results
          elif not os.path.exists(path + '/xml') or not os.listdir(path + '/xml'):
              self.logger.error('[ERROR] The directory "%s/xml" does not exist or no xml files for converting are found!\n(Maybe your convert list has old items?)' % (path))
              return results
      
          # check XPATH mapfile
          mapfile='%s/%s/mapfiles/%s-%s.xml' % (os.getcwd(),self.root,community,mdprefix)
          if not os.path.isfile(mapfile):
             mapfile='%s/%s/mapfiles/%s.xml' % (os.getcwd(),self.root,mdprefix)
             if not os.path.isfile(mapfile):
                self.logger.error('[ERROR] Mapfile %s does not exist !' % mapfile)
                return results

          # find all .xml files in path/xml
          results['tcount'] = len(filter(lambda x: x.endswith('.xml'), os.listdir(path+'/xml')))
          proc = subprocess.Popen(
              ["cd '%s'; java -cp lib/%s -jar %s inputdir=%s/xml outputdir=%s mapfile=%s"% (
                  os.getcwd()+'/'+self.root, self.cp,
                  self.program,
                  path, path, mapfile
              )], stdout=subprocess.PIPE, shell=True)
          (out, err) = proc.communicate()
          # check output and print it
          self.logger.info(out)
          if err: self.logger.error('[ERROR] ' + err)

        
        # check for mapper postproc config file
        rules=None
        ppconfig_file='%s/%s/mapfiles/mdpp-%s-%s.conf' % (os.getcwd(),self.root,community,mdprefix)
        if os.path.isfile(ppconfig_file):
            # read config file 
            f = codecs.open(ppconfig_file, "r", "utf-8")
            rules = f.readlines()[1:] # without the header
            rules = filter(lambda x:len(x) != 0,rules) # removes empty lines

        ##  instance of B2FIND discipline table
        disctab = self.cv_diciplines()

        # loop over all .json files in dir/json:
        files = filter(lambda x: x.endswith('.json'), os.listdir(path+'/json'))
        fcount = 0
        self.logger.info(' %s     INFO  B2FIND - Mapping files in %s/json' % (time.strftime("%H:%M:%S"),path))
        for filename in files:
              fcount+=1
              self.logger.info(' |- %s     INFO  Post - Processing: %s/json/%s' % (time.strftime("%H:%M:%S"),os.path.basename(path),filename))

              jsondata = dict()
        
              if ( os.path.getsize(path+'/json/'+filename) > 0 ):
                with open(path+'/json/'+filename, 'r') as f:
                   try:
                        jsondata=json.loads(f.read())
                   except:
                        log.error('    | [ERROR] Cannot load json file %s' % path+'/json/'+filename)
                        results['ecount'] += 1
                        continue
                try:
                   ## md postprocessor
                   if (rules):
                       self.logger.debug(' [INFO]:  Processing according rules %s' % rules)
                       jsondata=self.postprocess(jsondata,rules)
                except Exception as e:
                    self.logger.error(' [ERROR] %s : during postprocessing' % (e))
                    continue

                iddict=dict()
                stime=None
                etime=None
                # loop over all fields
                for facet in jsondata:
                   if facet == 'author':
                         jsondata[facet] = self.cut(jsondata[facet],'(.*)\(\d\d\d\d\)',0)
                   elif facet == 'tags':
                         jsondata[facet] = self.list2dictlist(jsondata[facet],"   ")
                   ##elif facet == 'title' : ## or facet == 'notes'
                   ##      jsondata[facet] = jsondata[facet]## .encode('latin1','replace')
                   elif facet == 'extras':
                      try: ### Semantic mapping of extra keys
                         for extra in jsondata[facet]:
                            if type(extra['value']) is list:
                              extra['value']=self.uniq(extra['value'])
                              if len(extra['value']) == 1:
                                 extra['value']=extra['value'][0] 
                            elif extra['key'] == 'identifiers':
                              iddict = self.map_identifier(extra['value'])
                                   ##HEW-T print 'key %s' % key
                            elif extra['key'] == 'Discipline': # generic mapping of discipline
                              extra['value'] = self.map_discipl(extra['value'],disctab.discipl_list)
                            elif extra['key'] == 'SpatialCoverage':
                               slat,wlon,nlat,elon=self.map_spatial(extra['value'])
                               if wlon and slat and elon and nlat :
                                 spvalue="{\"type\":\"Polygon\",\"coordinates\":[[[%s,%s],[%s,%s],[%s,%s],[%s,%s],[%s,%s]]]}" % (wlon,slat,wlon,nlat,elon,nlat,elon,slat,wlon,slat)
                                 jsondata['extras'].append({"key" : "spatial", "value" : spvalue }) 
                               extra['value'] = extra['value']['description'] or ''
                            elif extra['key'] == 'TemporalCoverage':
                               stime,etime=self.map_temporal(extra['value'])
                               extra['value']=extra['value']['@type']+': ( %s - %s ) ' % (stime,etime)
                            elif extra['key'] == 'Language': # generic mapping of languages
                              extra['value'] = self.map_lang(extra['value'])
                            elif extra['key'] == 'PublicationYear': # generic mapping of PublicationYear
                              extra['value'] = self.cut(extra['value'],'-',4)
                            elif extra['key'] == 'PublicationTimestamp' or extra['key'].startswith('Temporal') : # generic mapping of TempCoverageEnd
                              extra['value'] = self.date2UTC(extra['value'])
                      except Exception as e:
                          self.logger.error(' [ERROR] %s : during mapping of field %s' % (e,extra['key']))
                          ##HEW??? results['ecount'] += 1
                          continue
                   ##elif isinstance(jsondata[facet], basestring) :
                   ##    ### mapping of default string fields
                   ##    jsondata[facet]=jsondata[facet].encode('ascii', 'ignore')
                for key in iddict:
                    if key == 'url':
                        jsondata['url']=iddict['url']
                    else:
                        jsondata['extras'].append({"key" : key, "value" : iddict[key] }) 
                if stime and etime :
                    jsondata['extras'].append({"key" : "TemporalCoverage:BeginDate", "value" : stime }) 
                    jsondata['extras'].append({"key" : "TempCoverageBegin", "value" : self.utc2seconds(stime)}) 
                    jsondata['extras'].append({"key" : "TemporalCoverage:EndDate", "value" : stime }) 
                    jsondata['extras'].append({"key" : "TempCoverageEnd", "value" : self.utc2seconds(etime)})

                with io.open(path+'/json/'+filename, 'w', encoding='utf8') as json_file:
                ##with io.open(path+'/json/'+filename, 'w') as json_file:
                   try:
		       log.debug('   | [INFO] decode json data')
                       data = json.dumps(jsondata,sort_keys = True, indent = 4).decode('utf8')
                   except Exception as e:
                       log.error('    | [ERROR] %s : Cannot decode jsondata %s' % (e,jsondata))
                   try:
                       log.debug('   | [INFO] save json file')
                       json_file.write(data)
                   except TypeError, e :
                       # Decode data to Unicode first
                       log.error('    | [ERROR] Cannot write json file %s : %s' % (path+'/json/'+filename,e))
##                       json_file.write(data.decode('utf8'))
##                   try:
##                        json.dump(jsondata,json_file, sort_keys = True, indent = 4, ensure_ascii=False)
                   except Exception as e:
                        log.error('    | [ERROR] %s : Cannot write json file %s' % (e,path+'/json/'+filename))
                        results['ecount'] += 1
                        continue
              else:
                results['ecount'] += 1
                continue

        self.logger.info('%s     INFO  B2FIND : %d records mapped; %d records caused error(s).' % (time.strftime("%H:%M:%S"),fcount,results['ecount']))


        # search in output for result statistics
        last_line = out.split('\n')[-2]
        if ('INFO  Main - ' in last_line):
            string = last_line.split('INFO  Main ')[1]
            [results['count'], results['ecount']] = re.findall(r"\d{1,}", string)
            results['count'] = int(results['count']); results['ecount'] = int(results['ecount'])
        
    
        return results

    def reconvert(self,community,mdprefix,path):
        ## convert (CONVERTER object, community, path) - method
        # 'Re'-Converts the JSON files in directory <path>, 
        # which is defined by <community>, to XML files in B2FIND md format             #
        # Parameters:
        # -----------
        # 1. (string)   community - B2FIND community of the files
        # 2. (string)   path - path to subset directory without (!) 'json' subdirectory
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
        if not os.path.exists(path):
            self.logger.error('[ERROR] The directory "%s" does not exist! No files for re-converting are found!\n(Maybe your convert list has old items?)' % (path))
            return results
        elif not os.path.exists(path + '/json') or not os.listdir(path + '/json'):
            self.logger.error('[ERROR] The directory "%s/json" does not exist or no json files for converting are found!\n(Maybe your convert list has old items?)' % (path))
            return results
    
        # run re-converting
        # find all .json files in path/json:
        files = filter(lambda x: x.endswith('.json'), os.listdir(path+'/json'))
        
        results['tcount'] = len(files)

        if (not os.path.isdir(path+'/b2find')):
             os.makedirs(path+'/b2find')

        fcount = 1
        for filename in files:
            jsondata = dict()
        
            if ( os.path.getsize(path+'/json/'+filename) > 0 ):
                with open(path+'/json/'+filename, 'r') as f:
                    try:
                        jsondata=json.loads(f.read())
                    except:
                        log.error('    | [ERROR] Cannot load the json file %s' % path+'/json/'+filename)
                        results['ecount'] += 1
                        continue
            else:
                results['ecount'] += 1
                continue
            
            # remove field fulltext
            extras_counter = 0
            for extra in jsondata['extras']:
                if(extra['key'] == 'fulltext'):
                    jsondata['extras'].pop(extras_counter)
                    break
                extras_counter  += 1

            # get dataset name from filename (a uuid generated identifier):
            ds_id = os.path.splitext(filename)[0]
            xmlfile=ds_id+'.xml'            

            self.logger.info('    | r | %-4d | %-40s |' % (fcount,ds_id))
            
            # get OAI identifier from json data extra field 'oai_identifier':
            oai_id  = None
            for extra in jsondata['extras']:
                if(extra['key'] == 'oai_identifier'):
                    oai_id = extra['value']
                    break
            self.logger.debug("        |-> identifier: %s\n" % (oai_id))
            
            ### reconvert !!
            try:
              xml = xmltools.WriteToXMLString(jsondata)
              # write B2FIND xml file:
              f = open(path+'/b2find/'+xmlfile, 'w')
              f.write(xml)
              f.close
            except IOError, e:
              self.logger.error("[ERROR] Cannot write data in xml file '%s': %s\n" % (xmlfile,e))
              stats['ecount'] +=1
              return(False, ds_id, path+'/b2find/', count_set)
             
        # count ... all .xml files in path/b2find
        results['count'] = len(filter(lambda x: x.endswith('.xml'), os.listdir(path+'/b2find')))
    
        return results    

class UPLOADER (object):

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
    # .validate(jsondata)       - Validates the fields in the <jsondata> by using B2FIND standard
    #
    # Usage:
    # ------

    # create UPLOADER object:
    UP = UPLOADER(CKAN,OUT)

    # VALIDATE JSON DATA
    if (not UP.validate(jsondata)):
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
        self.logger = log.getLogger()
        self.CKAN = CKAN
        self.OUT = OUT
        
        self.package_list = dict()
        
        
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

    def validate(self, jsondata):
        ## validate (UPLOADER object, json data) - method
        # Validates the json data (e.g. the PublicationTimestamp field) by using B2FIND standard
        #
        # Parameters:
        # -----------
        # 1. (dict)    jsondata - json dictionary with metadata fields with B2FIND standard
        #
        # Return Values:
        # --------------
        # 1. (integer)  validation result:
        #               0 - critical error occured
        #               1 - non-critical error occured
        #               2 - no error occured    
    
        status = 2
        errmsg = ''
        must_have_extras = {
            # "extra_field_name" : (Integer), 0 for critical, 1 for non-critical
            ##"oai_identifier":0
        }
        
        ## check main fields ...
        if (not('title' in jsondata) or jsondata['title'] == ''):
            errmsg = "'title': The title is missing"
            status = 0  # set status
        ##HEW-D elif ('url' in jsondata and not self.check_url(jsondata['url'])):
        ##HEW-D     errmsg = "'url': The source url is broken"
        ##HEW-D     if(status > 1): status = 1  # set status
            
        if errmsg: self.logger.warning("        [WARNING] field %s" % errmsg)
        
        ## check extra fields ...
        counter = 0
        for extra in jsondata['extras']:
            errmsg = ''
            # ... OAI Identifier
            if(extra['key'] == 'oai_identifier' and extra['value'] == ''):
                errmsg = "'oai_identifier': The ID is missing"
                status = 0  # set status

            # shrink field fulltext
            elif(extra['key'] == 'fulltext' and sys.getsizeof(extra['value']) > 31999):
                errmsg = "'fulltext': Too big ( %d bytes, %d len)" % (sys.getsizeof(extra['value']),len(extra['value']))
                encoding='utf-8'
                encoded = extra['value'].encode(encoding)[:32000]
                extra['value']=encoded.decode(encoding, 'ignore')
                ##HEW!!! print "cut off : 'fulltext': now ( %d bytes, %d len)" % (sys.getsizeof(extra['value']),len(extra['value']))
                status = 2  # set status

            elif(extra['key'] == 'PublicationYear'):            
                try:
                   datetime.datetime.strptime(extra['value'], '%Y')
                except ValueError:
                    errmsg = "%s value %s has incorrect data format, should be YYYY" % (extra['key'],extra['value'])
                    # delete this field from the jsondata:
                    jsondata['extras'].pop(counter)
                    
                    if(status > 1): status = 1  # set status
                
            # ... PublicationTimestamp
            elif(extra['key'] == 'PublicationTimestamp'):
                try:
                    datetime.datetime.strptime(extra['value'], '%Y-%m-%d'+'T'+'%H:%M:%S'+'Z')
                except ValueError:
                    errmsg = "'PublicationTimestamp' value %s has incorrect data format, should be YYYY-MM-DDThh:mm:ssZ" % extra['value']
                    
                    # delete this field from the jsondata:
                    jsondata['extras'].pop(counter)
                    
                    if(status > 1): status = 1  # set status
            
            # ... TemporalCoverage:BeginDate
            elif(extra['key'] == 'TemporalCoverage:BeginDate'):
                try:
                    datetime.datetime.strptime(extra['value'], '%Y-%m-%d'+'T'+'%H:%M:%S'+'Z')
                except ValueError:
                    errmsg = "'TemporalCoverage:BeginDate' value %s has incorrect data format, should be YYYY-MM-DDThh:mm:ssZ" % extra['value']
                    
                    # delete this field from the jsondata:
                    jsondata['extras'].pop(counter)
                    
                    if(status > 1): status = 1  # set status
            
            # ... TemporalCoverage:EndDate
            elif(extra['key'] == 'TemporalCoverage:EndDate'):
                try:
                    datetime.datetime.strptime(extra['value'], '%Y-%m-%d'+'T'+'%H:%M:%S'+'Z')
                except ValueError:
                    errmsg = "'TemporalCoverage:EndDate' value %s has incorrect data format, should be YYYY-MM-DDThh:mm:ssZ" % extra['value']
                    
                    # delete this field from the jsondata:
                    jsondata['extras'].pop(counter)
                    
                    if(status > 1): status = 1  # set status
            
            # print warning:
            if errmsg: self.logger.warning("        [WARNING] extra field %s" % errmsg)
            
            # delete key from the must have extras dictionary:
            if extra['key'] in must_have_extras:
               del must_have_extras[extra['key']]
        
            counter+=1
            
        if (len(must_have_extras) > 0):
            self.logger.warning("        [WARNING] extra fields %s are missing" % must_have_extras.keys())
            status = min(status,must_have_extras.values())
            
        return status


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

class OUTPUT (object):

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
            'log':log.INFO,
            'err':log.ERROR,
            'std':log.INFO,
        }
        
        if self.verbose == 1:
            self.log_level = {
                'log':log.DEBUG,
                'err':log.ERROR,
                'std':log.INFO,
            }
        elif self.verbose == 2:
            self.log_level = {
                'log':log.DEBUG,
                'err':log.ERROR,
                'std':log.DEBUG,
            }
            
        # create the logger and start it:
        self.start_logger()
        
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
    
        logger = log.getLogger()
        logger.setLevel(log.DEBUG)
        
        # create file handler which logs even debug messages
        lh = log.FileHandler(self.jobdir + '/myapp.log', 'w')
        lh.setLevel(self.log_level['log'])
        
        # create file handler which logs only error messages
        eh = log.FileHandler(self.jobdir + '/myapp.err', 'w')
        eh.setLevel(self.log_level['err'])
        
        # create console handler with a higher log level
        ch = log.StreamHandler()
        ch.setLevel(self.log_level['std'])
        
        # create formatter and add it to the handlers
        formatter_l = log.Formatter("%(message)s")
        formatter_h = log.Formatter("%(message)s\t[%(module)s, %(funcName)s, NO: %(lineno)s]\n")
        
        lh.setFormatter(formatter_l)
        ch.setFormatter(formatter_l)
        eh.setFormatter(formatter_h)
        
        # add the handlers to the logger
        logger.addHandler(lh)
        logger.addHandler(ch)
        logger.addHandler(eh)
        
        self.logger = logger

    
    def save_stats(self,request,subset,mode,stats):
        ## save_stats (OUT object, request, subset, mode, stats) - method
        # Saves the statistics of a process (harvesting, converting or uploading) per subset in <OUTPUT.stats>. 
        # <OUTPUT.stats> is a big dictionary with all results statistics of the harvesting, converting and uploading routines.
        # Requests which start with a '#' are special requests like '#Start' or '#GetPackages'
        # and will be ignored in the most actions.
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
        # (string)  mode - process mode (can be 'h', 'c' or 'u')
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
                    'r':{
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
            log.shutdown()
            
            # make the new log dir if it is necessary:
            logdir= self.jobdir
            if (not os.path.exists(logdir)):
               os.makedirs(logdir)     

            # generate new log and error filename:
            logfile, errfile = '',''
            if (request == '#Start'):
                logfile='%s/start.log.txt' % (logdir)
                errfile='%s/start.err.txt' % (logdir)
            else:
                logfile='%s/%s_%s.log.txt' % (logdir,mode,self.get_stats(request,subset,'#id'))
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
                % (self.start_time, self.jid, options.epic_check)
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
        elif (os.path.getsize(self.jobdir+'/start.err.txt')):
            size_log = os.path.getsize(self.jobdir+'/start.log.txt')
            size_err = os.path.getsize(self.jobdir+'/start.err.txt')
            if (size_log != 0):
                size_log = int(size_log/1024.) or 1
            if (size_err != 0):
                size_err = int(size_err/1024.) or 1
        
            reshtml.write('<span style="color:red"><strong>A critical script error occured! Look at main <a href="start.log.txt">log</a> (%d kB) and <a href="start.err.txt">error</a> (%d kB) file. </strong></span><br />'% (size_log, size_err))
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
                        if (pstat['status'][mode[0]] == 'tbd'  and os.path.exists(self.jobdir+'/%s_%d.log.txt'%(mode[0],self.get_stats(request,subset,'#id')))):
                            try:
                                size = os.path.getsize(self.jobdir+'/%s_%d.log.txt'%(mode[0],self.get_stats(request,subset,'#id')))
                                if (size != 0):
                                    size = int(size/1024.) or 1
                                reshtml.write('<a href="%s_%d.log.txt">%s</a> (%d kB)<br />'% (mode[0],self.get_stats(request,subset,'#id'),pstat['short'][mode],size))
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
    
    def print_convert_list(self,community,source,mdprefix,dir,fromdate):
        if (fromdate == None):
           self.convert_list = './convert_list_total'
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
                self.logger.critical("Cannot read data from '{0}': {1}".format(self.convert_list, strerror))
                f.close

        try:
            f = open(self.convert_list, 'w')
            f.write(file)
            f.close()
        except IOError as (errno, strerror):
            self.logger.critical("Cannot write data to '{0}': {1}".format(self.convert_list, strerror))
            f.close
