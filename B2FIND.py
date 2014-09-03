"""B2FIND.py - classes for B2FIND management : 
  - CKAN_CLIENT Executes CKAN APIs (interface to CKAN)
  - HARVESTER   Harvests from a OAI-PMH server
  - CONVERTER   Converts XML files to JSON files with Lari's converter and by using mapfiles
  - UPLOADER    Uploads JSON files to CKAN portal
  - POSTPROCESS Run postprocess routines on JSON files
  - OUTPUT      Initializes the logger class and provides methods for saving log data and for printing those.    

Install required modules simplejson, e.g. by :

  > sudo pip install <module>

Copyright (c) 2014 John Mrziglod (DKRZ)

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
import time, datetime, subprocess

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
import urllib, urllib2
import httplib
from urlparse import urlparse

# needed for CONVERTER :
import codecs
import simplejson as json
##import xmltools

# needed for UPLOADER and CKAN class:
from collections import OrderedDict

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
            print '\t\tError code %s : The server %s couldn\'t fulfill the action %s.' % (e.code,self.ip_host,action)
            if ( e.code == 403 ):
                print '\t\tAccess forbidden, maybe the API key is not valid?'
                exit(e.code)
            elif ( e.code == 409 and action == 'package_create'):
                print '\t\tMaybe the dataset already exists or you have a parameter error?'
                self.action('package_update',data_dict)
                return {"success" : False}
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
    # .harvest_sickle(request)   - harvest from a source via sickle module
    # [deprecated] .harvest(request) - harvest from a source via (old) OAI-PMH module
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
    results = HV.harvest_sickle(request)

    if (results == -1):
        print "Error occured!"
    """
    
    def __init__ (self, OUT, pstat, base_outdir, fromdate):
        self.logger = log.getLogger()
        self.pstat = pstat
        self.OUT = OUT
        self.base_outdir = base_outdir
        self.fromdate = fromdate
        
    
    def harvest_sickle(self, request):
        ## harvest_sickle (HARVESTER object, [community, source, verb, mdprefix, mdsubset]) - method
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
            "mdsubset"  : request[4]   if len(request)>4 else ''
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
        
        # create sickle object and sets the default log output of the 'request' module to WARNING level:
        sickle = SickleClass.Sickle(req['url'], max_retries=3, timeout=30)
     
        requests_log = log.getLogger("requests")
        requests_log.setLevel(log.WARNING)
        
        # if the number of files in a subset dir is greater than <count_break>
        # then create a new one with the name <set> + '_' + <count_set>
        count_break = 5000
        count_set = 1
       
        # set subset:
        if (not req["mdsubset"]):
            subset = 'SET'
        else:
            subset = req["mdsubset"]
            
        if (self.fromdate):
            subset = subset + '_f' + self.fromdate
        
        # make subset dir:
        subsetdir = '/'.join([self.base_outdir,req['community']+'-'+req['mdprefix'],subset+'_'+str(count_set)])
        
        # Get all files in the current subset directories and put those in the dictionary deleted_metadata
        deleted_metadata = dict()
        for s in glob.glob('/'.join([self.base_outdir,req['community']+'-'+req['mdprefix'],subset+'_[0-9]*'])):
            for f in glob.glob(s+'/xml/*.xml'):
                # save the uid as key and the subset as value:
                deleted_metadata[os.path.splitext(os.path.basename(f))[0]] = f
    
        self.logger.info('    |   | %-4s | %-45s | %-45s |\n    |%s|' % ('#','OAI Identifier','DS Identifier',"-" * 106))
        try:
            for record in sickle.ListRecords(**{'metadataPrefix':req['mdprefix'],'set':req['mdsubset'],'ignore_deleted':False,'from':self.fromdate}):
            
            	if (record.header.deleted):
            	    continue
                
                stats['tcount'] += 1

                # get the id of the metadata file:
                oai_id = record.header.identifier
                
                # generate a uniquely identifier for this dataset:
                uid = str(uuid.uuid5(uuid.NAMESPACE_DNS, oai_id.encode('ascii','replace')))
                
                xmlfile = subsetdir + '/xml/' + os.path.basename(uid) + '.xml'
                try:
                    self.logger.info('    | h | %-4d | %-45s | %-45s |' % (stats['count']+1,oai_id,uid))
                    self.logger.debug('Harvested XML file written to %s' % xmlfile)
                    
                    # get the raw xml content:    
                    metadata = record.raw
                    metadata = etree.fromstring(metadata)
                    ## etree.tostring(x, pretty_print = True)
                    if (metadata):
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
                    self.logger.error("    [ERROR] %s" % traceback.format_exc())
                    self.logger.info(metadata)
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
            if (len(deleted_metadata) > 0) and self.pstat['status']['d'] == 'tbd':
                ## delete all files in deleted_metadata and write the subset
                ## and the uid in '<outdir>/delete/<community>-<mdprefix>':
                self.logger.info('    | These [%d] files were not updated and will be deleted:' % (len(deleted_metadata)))
                
                # path to the file with all deleted uids:
                delete_file = '/'.join([self.base_outdir,'delete',req['community']+'-'+req['mdprefix']+'.del'])
                file_content = ''
                
                if(os.path.isfile(delete_file)):
                    try:
                        f = open(delete_file, 'r')
                        file_content = f.read()
                        f.close()
                    except IOError as (errno, strerror):
                        self.logger.critical("Cannot read data from '{0}': {1}".format(delete_file, strerror))
                        f.close
                elif (not os.path.exists(self.base_outdir+'/delete')):
                    os.makedirs(self.base_outdir+'/delete')    
                
                # add all deleted metadata to the file, subset in the 1. column and id in the 2. column:
                for uid in deleted_metadata:
                    self.logger.info('    | d | %-4d | %-45s |' % (stats['totdcount'],uid))
                    
                    xmlfile = deleted_metadata[uid]
                    subset = os.path.dirname(xmlfile).split('/')[-2]
                    jsonfile = '/'.join(xmlfile.split('/')[0:-2])+'/json/'+uid+'.json'
                
                    file_content += '%s\t%s\n' % (subset,uid)
                    stats['totdcount'] += 1
                    
                    # remove xml file:
                    try: 
                        os.remove(xmlfile)
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
                
                # write file_content in delete file:
                try:
                    f = open(delete_file, 'w')
                    f.write(file_content)
                    f.close()
                except IOError as (errno, strerror):
                    self.logger.critical("Cannot write data to '{0}': {1}".format(delete_file, strerror))
                    f.close
        
        
            # add all subset stats to total stats and reset the temporal subset stats:
            for key in ['tcount', 'ecount', 'count', 'dcount']:
                stats['tot'+key] += stats[key]
            
            self.logger.info(
                '## [%d] files provided by %s / %s, [%d] records are harvested, [%d] failed and [%d] are deleted.' 
                % (
                    stats['tottcount'],
                    req['mdprefix'],
                    req['mdsubset'],
                    stats['totcount'],
                    stats['totecount'],
                    stats['totdcount']
                ))
        
            # save the current subset:
            if (stats['count'] > 0):
                self.save_subset(req, stats, subset, subsetdir, count_set)
            
            
    
    ## DEPRECATED ##
    """    
    ## harvest(self, request) - method
    # call OAI listrecords or listidentifier+getrecord to retrieve xml records
    # and store metadata records in target directory.
    #
    # Parameters:
    # -----------
    # (list)    request - A list with four or five elements:
    #           1. community
    #           2. url - The URL to the OAI-PMH server
    #           3. lverb - 'ListIdentifiers' or 'ListRecords'
    #           4. mdprefix - Prefix of the metadata
    #           5. mdsubset - Subset of the metadata (optional)
                       
    def harvest(self, request):
    
        # create a request dictionary:
        req = {
            "community" : request[0],
            "url"   : request[1],
            "lverb" : request[2],
            "mdprefix"  : request[3],
            "mdsubset"  : request[4]   if len(request)>4 else ''
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
   
        # create client object for harvesting:
        registry = MetadataRegistry()
        registry.registerReader(req['mdprefix'], Reader())
        client = Client(req['url'], registry)
        
        # if the number of files in a subset dir is greater than <count_break>
        # then create a new one with the name <set> + '_' + <count_set>
        count_break = 5000
        count_set = 1
       
        # set subset:
        if (not req["mdsubset"]):
            subset = 'SET'
        else:
            subset = req["mdsubset"]
            
        if (self.fromdate):
            subset = subset + '_f' + str(self.fromdate.date())
        
        # make subset dir:
        subsetdir = '/'.join([self.base_outdir,req['community']+'-'+req['mdprefix'],subset+'_'+str(count_set)])
        
        # Get all files in the current subset directories and put those in the dictionary files_to_delete
        deleted_metadata = dict()
        for s in glob.glob('/'.join([self.base_outdir,req['community']+'-'+req['mdprefix'],subset+'_[0-9]*'])):
            for f in glob.glob(s+'/xml/*.xml'):
                # save the id as key and the subset as value:
                deleted_metadata[os.path.splitext(os.path.basename(f))[0]] = f
        
        self.logger.info('    |   | %-4s | %-45s |\n    |%s|' % ('#','OAI Identifier',"-" * 58))
        try:
            if ( req['lverb'] == 'ListIdentifiers'):
            
                if not req['mdsubset']:
                    for data in client.listIdentifiers(metadataPrefix=req['mdprefix']):
                        success, id, subsetdir, count_set = \
                            self.loop(client,data,req,stats,subset,subsetdir,count_set,count_break)
                        
                        if success and id in deleted_metadata:
                            del deleted_metadata[id]
                            
                else:      
                    for data in client.listIdentifiers(metadataPrefix=req['mdprefix'], set=req['mdsubset']):
                        success, id, subsetdir, count_set = \
                            self.loop(client,data,req,stats,subset,subsetdir,count_set,count_break)
                        
                        if success and id in deleted_metadata:
                            del deleted_metadata[id]
            else:
            
                if not req['mdsubset']:
                    for data in client.listRecords(metadataPrefix=req['mdprefix']):
                        success, id, subsetdir, count_set = \
                            self.loop(client,data,req,stats,subset,subsetdir,count_set,count_break)
                        
                        if success and id in deleted_metadata:
                            del deleted_metadata[id]
                else:      
                    for data in client.listRecords(metadataPrefix=req['mdprefix'], set=req['mdsubset']):
                        success, id, subsetdir, count_set = \
                            self.loop(client,data,req,stats,subset,subsetdir,count_set,count_break)
                        
                        if success and id in deleted_metadata:
                            del deleted_metadata[id]
 
        except TypeError as e:
            self.logger.error('    [ERROR] Type Error: %s' % e)
        except NoRecordsMatchError as e:
            self.logger.error('    [ERROR] No Records Match: %s' % e)
        except BadVerbError as e:
            self.logger.error('    [ERROR] %s : %s ' % (e,req['lverb']))
        except DatestampError as e:
            self.logger.error('    [ERROR] DatestampError : %s' % e)
        finally:
            if len(deleted_metadata) > 0:
                ## delete all files in deleted_metadata and write the subset
                ## and the id in '<outdir>/delete/<community>-<mdprefix>':
                self.logger.info('    | These [%d] files were not updated and will be deleted:' % (len(deleted_metadata)))
                
                # path to the file with all deleted ids:
                delete_file = '/'.join([self.base_outdir,'delete',req['community']+'-'+req['mdprefix']+'.del'])
                file_content = ''
                
                if(os.path.isfile(delete_file)):
                    try:
                        f = open(delete_file, 'r')
                        file_content = f.read()
                        f.close()
                    except IOError as (errno, strerror):
                        self.logger.critical("Cannot read data from '{0}': {1}".format(delete_file, strerror))
                        f.close
                elif (not os.path.exists(self.base_outdir+'/delete')):
                    os.makedirs(self.base_outdir+'/delete')    
                
                # add all deleted metadata to the file, subset in the 1. column and id in the 2. column:
                for id in deleted_metadata:
                    self.logger.info('    | d | %-4d | %-45s |' % (stats['totdcount'],id))
                    
                    xmlfile = deleted_metadata[id]
                    subset = os.path.dirname(xmlfile).split('/')[-2]
                    jsonfile = '/'.join(xmlfile.split('/')[0:-2])+'/json/'+id+'.json'
                
                    file_content += '%s\t%s\n' % (subset,id)
                    stats['totdcount'] += 1
                    
                    # remove xml file:
                    try: 
                        os.remove(xmlfile)
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
                
                # write file_content in delete file:
                try:
                    f = open(delete_file, 'w')
                    f.write(file_content)
                    f.close()
                except IOError as (errno, strerror):
                    self.logger.critical("Cannot write data to '{0}': {1}".format(delete_file, strerror))
                    f.close
        
        
            # add all subset stats to total stats and reset the temporal subset stats:
            for key in ['tcount', 'ecount', 'count', 'dcount']:
                stats['tot'+key] += stats[key]
            
            self.logger.info(
                '## [%d] files provided by %s / %s, [%d] records are harvested, [%d] failed and [%d] are deleted.' 
                % (
                    stats['tottcount'],
                    req['mdprefix'],
                    req['mdsubset'],
                    stats['totcount'],
                    stats['totecount'],
                    stats['totdcount']
                ))
        
            # save the current subset:
            if (stats['count'] > 0):
                self.save_subset(req, stats, subset, subsetdir, count_set)
        
        return 1
    
    
    
    
    ## loop (HARVESTER object, client, data, req, stats, subset, subsetdir, count_set, count_break) - method
    # Get a metadata file from a ListIdentifiers or a ListRecords loop, check it (time stamp, delete flag)
    # and store it to the given <subsetdir>
    #
    # Parameters:
    # -----------
    # (object)  client - oaipmh object
    # (object)  data - metadata file
    # (dict)    req - request dictionary (community, oai-url, lverb, mdprefix, mdsubset)
    # (dict)    stats - statistics dictionary with file counter and time stopper
    # (string)  subset - subset of the metadata
    # (string)  subsetdir - directory in which all harvested data will be stored
    # (integer) count_set - counter of the current subset
    # (integer) count_break - if the file counter stats['count'] is greater than this number
    #           a new subset directory will be created
    #
    # Return Values:
    # --------------
    # 1. (boolean)  success 
    # 2. (string)   metadata id 
    # 3. (string)   current subsetdir 
    # 4. (integer)  current counter of the subset 
        
    def loop(self,client,data,req,stats,subset,subsetdir,count_set,count_break):
        stats['tcount'] += 1
        
        # get the header of the metadata file:
        header = data if req['lverb'] == 'ListIdentifiers' else data[0]
        
        # get the id of the metadata file:
        id = header.identifier()
        
        # check the datestamp of the metadata:
        if (self.fromdate) and (self.fromdate >= header.datestamp()):
            self.logger.debug('Dataset with id "%s" is too old!' % id)
            return(False, id, subsetdir, count_set) 
        
        xmlfile = subsetdir + '/xml/' + os.path.basename(id) + '.xml'
        try:
            # dataset state is 'deleted':
            if header.isDeleted():
                self.logger.info('    | d | %-4d | %-45s |' % (stats['dcount']+1,id))
            
                jsonfile = subsetdir + '/json/' + id + '.json'
                 
                # remove json file and add ID to the 'delete' file:
                if (os.path.exists(jsonfile)):
                    # write the id in '<outdir>/delete/<community>-<mdprefix>':
                    delete_file = '/'.join([self.base_outdir,'delete',req['community']+'-'+req['mdprefix']+'.del'])
                    file_content = ''
                    
                    if(os.path.isfile(delete_file)):
                        try:
                            f = open(delete_file, 'r')
                            file_content = f.read()
                            f.close()
                        except IOError as (errno, strerror):
                            self.logger.critical("Cannot read data from '{0}': {1}".format(delete_file, strerror))
                            f.close
                    elif (not os.path.exists(self.base_outdir+'/delete')):
                        os.makedirs(self.base_outdir+'/delete')

                    # add subset and id to delete file:
                    file_content += '%s\t%s\n' % (subset,id)
                    try:
                        f = open(delete_file, 'w')
                        f.write(file_content)
                        f.close()
                    except IOError as (errno, strerror):
                        self.logger.critical("Cannot write data to '{0}': {1}".format(delete_file, strerror))
                        self.logger.critical("Cannot add id %s to delete file!" %(id))
                        f.close
                        
                    # now remove the jsonfile:
                    try: 
                        os.remove(jsonfile)
                    except OSError, e:
                        self.logger.error("[ERROR] Cannot remove json file '%s': %s\n" % (jsonfile,e))
                        stats['ecount'] +=1
                        return(False, id, subsetdir, count_set)
                
                # remove xml file:        
                if (os.path.exists(xmlfile)):
                    try: 
                        os.remove(xmlfile)
                    except OSError, e:
                        self.logger.error("[ERROR] Cannot remove xml file '%s': %s\n" % (xmlfile,e))
                        stats['ecount'] +=1
                        return(False, id, subsetdir, count_set)
                        
                stats['dcount'] += 1
            
            # dataset state is 'active':    
            else:
                self.logger.info('    | h | %-4d | %-45s |' % (stats['count']+1,id))
                self.logger.debug('Harvested XML file written to %s' % xmlfile)

                # if lverb was only 'ListIdentifiers' then get the rest of the metadata:
                if (req['lverb'] == 'ListIdentifiers'):
                    try: 
                        data = client.getRecord(identifier=id, metadataPrefix = req['mdprefix'])
                    except CannotDisseminateFormatError as e:
                        self.logger.error('    [ERROR] %s' % e)
                        stats['ecount']+=1
                        return(False, id, subsetdir, count_set)
                    except  NoMetadataFormatsError as e:
                        self.logger.error('    [ERROR] No Metadata: %s' % e)
                        stats['ecount']+=1
                        return(False, id, subsetdir, count_set)
                    except XMLSyntaxError as e:
                        self.logger.error('    [ERROR] XML syntax: %s' % e)
                        stats['ecount']+=1
                        return(False, id, subsetdir, count_set)
                    
                metadata = data[1]
                if (metadata):
                    # make xml dirs:
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
                        return(False, id, subsetdir, count_set)
                    
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
                    self.logger.warning('    [WARNING] No metadata available for %s' % id)
                    
                
        except TypeError:
            self.logger.error('    [ERROR] %s' % e)
            stats['ecount']+=1        
            return(False, id, subsetdir, count_set)
        except Exception as e:
            self.logger.error('    [ERROR] %s' % e)
            stats['ecount']+=1
            return(False, id, subsetdir, count_set)
        else:
            return(True, id, subsetdir, count_set)
    """

        
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
##HEW-SVN        self.program = (filter(lambda x: x.endswith('.jar') and x.startswith('converter-'), os.listdir(root)))[0]
##HEW-GIT
        self.program = (filter(lambda x: x.endswith('.jar') and x.startswith('md-mapper-'), os.listdir(root)))[0]
        

    def replace(self,dataset,facetName,old_value,new_value):
        """
        replaces old value - can be a regular expression - with new value for a given facet
        """

        old_regex = re.compile(old_value)

        for facet in dataset:
            if facet == facetName and re.match(old_regex, dataset[facet]):
                dataset[facet] = new_value
                return dataset
            if facet == 'extras':
                for extra in dataset[facet]:
                    if extra['key'] == facetName and re.match(old_regex, extra['value']):
                        extra['value'] = new_value
                        return dataset
        return dataset
 
    def truncate(self,dataset,facetName,old_value,size):
        """
        truncates old value with new value for a given facet
        """
        for facet in dataset:
            if facet == facetName and dataset[facet] == old_value:
                dataset[facet] = old_value[:size]
                return dataset
            if facet == 'extras':
                for extra in dataset[facet]:
                    if extra['key'] == facetName and extra['value'] == old_value:
                        extra['value'] = old_value[:size]
                        return dataset
        return dataset

    def remove_duplicates(self,dataset,facetName,valuearrsep,entrysep):
        """
        remove duplicates      
        """
        for facet in dataset:
          if facet == facetName:
            valarr=dataset[facet].split(valuearrsep)
            valarr=list(OrderedDict.fromkeys(valarr)) ## this elimintas real duplicates
            revvalarr=[]
            for entry in valarr:
               reventry=entry.split(entrysep) ### 
               reventry.reverse()
               reventry=''.join(reventry)
               revvalarr.append(reventry)
               for reventry in revvalarr:
                  if reventry == entry :
                     valarr.remove(reventry)
            dataset[facet]=valuearrsep.join(valarr)
        return dataset       
      
    def splitstring2dictlist(self,dataset,facetName,valuearrsep,entrysep):
        """
        split string in list of string and transfer to list of dict's { "name" : "substr1" }      
        """
        for facet in dataset:
          if facet == facetName:
            ##HEW?? print 'sep %s' % valuearrsep
            valarr=dataset[facet][0]['name'].split()
            valarr=list(OrderedDict.fromkeys(valarr)) ## this elimintas real duplicates
            dicttagslist=[]
            for entry in valarr:
               entrydict={ "name": entry }  
               dicttagslist.append(entrydict)
       
            dataset[facet]=dicttagslist
        return dataset       


    def changeDateFormat(dataset,facetName,old_format,new_format):
        """
        changes date format from old format to a new format
        current assumption is that the old format is anything (indicated in the config file 
        by * ) and the new format is UTC
        """
        for facet in dataset:
            if str_equals(facet,facetName) and old_format == '*':
                if str_equals(new_format,'UTC'):
                    old_date = dataset[facet]
                    new_date = date2UTC(old_date)
                    dataset[facet] = new_date
                    return dataset
            if facet == 'extras':
                for extra in dataset[facet]:
                    if str_equals(extra['key'],facetName) and old_format == '*':
                        if str_equals(new_format,'UTC'):
                            old_date = extra['value']
                            new_date = date2UTC(old_date)
                            extra['value'] = new_date
                            return dataset
        return dataset
      
    def postprocess(self,dataset,rules):
        """
        changes dataset field values according to configuration
        """  
     
        for rule in rules:
            # rules can be checked for correctness
            assert(rule.count(',,') == 5),"a double comma should be used to separate items"
            
            rule = rule.rstrip('\n').split(',,') # splits  each line of config file 
            groupName = rule[0]
            datasetName = rule[1]
            facetName = rule[2]
            old_value = rule[3]
            new_value = rule[4]
            action = rule[5]
                        
            r = dataset.get("group",None)
            if groupName != '*' and  groupName != r:
                return dataset
    
            r = dataset.get("name",None)
            if datasetName != '*' and datasetName != r:
                return dataset
    
            if action == 'replace':
                dataset = self.replace(dataset,facetName,old_value,new_value)
            if action == "truncate":
                pass
            if action == 'remove_duplicates':
                dataset = self.remove_duplicates(dataset,facetName,old_value,new_value)
            if action == 'splitstring2dictlist':
                dataset = self.splitstring2dictlist(dataset,facetName,old_value,new_value)
            if action == "another_action":
                pass
            
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
        # 3. (string)   path - path to subset directory without (!) 'xml' subdirectory
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
            self.logger.error('[ERROR] The directory "%s" does not exist! No files for converting are found!\n(Maybe your convert list has old items?)' % (path))
            return results
        elif not os.path.exists(path + '/xml') or not os.listdir(path + '/xml'):
            self.logger.error('[ERROR] The directory "%s/xml" does not exist or no xml files for converting are found!\n(Maybe your convert list has old items?)' % (path))
            return results
        #elif not os.path.exists(path + '/json'):
        #    self.logger.error('[ERROR] The directory "%s/json" does not exist !' % (path))
        #    return results
    
        # check mapfile
        mapfile='%s/%s/mapfiles/%s-%s.xml' % (os.getcwd(),self.root,community,mdprefix)
        if not os.path.isfile(mapfile):
           mapfile='%s/%s/mapfiles/%s.xml' % (os.getcwd(),self.root,mdprefix)
           if not os.path.isfile(mapfile):
              self.logger.error('[ERROR] Mapfile %s does not exist !' % mapfile)
              return results

        # run the converter
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
        ppconfig_file='%s/%s/mapfiles/mdpp-%s-%s.conf' % (os.getcwd(),self.root,community,mdprefix)
        if os.path.isfile(ppconfig_file):
            # read config file 
            f = codecs.open(ppconfig_file, "r", "utf-8")
            rules = f.readlines()[1:] # without the header
            rules = filter(lambda x:len(x) != 0,rules) # removes empty lines
            # find all .json files in dir/json:
            files = filter(lambda x: x.endswith('.json'), os.listdir(path+'/json'))
        
            fcount = 1
            for filename in files:
              jsondata = dict()
        
              if ( os.path.getsize(path+'/json/'+filename) > 0 ):
                with open(path+'/json/'+filename, 'r') as f:
                   try:
                        jsondata=json.loads(f.read())
                        ### Mapper post processing
                        ##HEW-T print 'set %s' % os.path.basename(path)
                        if ( os.path.basename(path) == 'a0337_1' or re.match(re.compile('a0005_'+'[0-9]*'),os.path.basename(path)) or os.path.basename(path) == 'a0336_1' ) :
                        ##print 'set %s' % os.path.basename(path)
                        ##if ( os.path.basename(path) == 'a0337_1' or os.path.basename(path) == 'a0336_1'  or os.path.basename(path) == 'a0005_test' ) :
                           for extra in jsondata['extras']:
                              if(extra['key'] == 'Discipline'):
                                 extra['value'] = 'Arts'
                                 break
                        elif ( os.path.basename(path) == 'a0338_1' ) :
                           for extra in jsondata['extras']:
                              if(extra['key'] == 'Discipline'):
                                 extra['value'] = 'Philology'
                                 break
                        elif ( os.path.basename(path) == 'a1057_1' or os.path.basename(path) == 'a0340_1' ):
                           for extra in jsondata['extras']:
                              if(extra['key'] == 'Discipline'):
                                 extra['value'] = 'History'
                                 break
                        elif ( os.path.basename(path) == 'a1025_1' ):
                           ## add extra key 'Discipline'
                           jsondata['extras'].append({"key": "Discipline","value": "History"})
                        jsondata=self.postprocess(jsondata,rules)
                        ##f.seek(0)
                        ##json.dump(jsondata,f, sort_keys = True, indent = 4)
                   except:
                        log.error('    | [ERROR] Cannot load json file %s' % path+'/json/'+filename)
                        results['ecount'] += 1
                        continue
                with open(path+'/json/'+filename, 'w') as f:
                   try:
                        ##f.write("{}\n".format(json.dumps(jsondata)))
                        json.dump(jsondata,f, sort_keys = True, indent = 4)
                   except:
                        log.error('    | [ERROR] Cannot write json file %s' % path+'/json/'+filename)
                        results['ecount'] += 1
                        continue
              else:
                results['ecount'] += 1
                continue

        # search in output for result statistics
        last_line = out.split('\n')[-2]
        if ('INFO  Main - ' in last_line):
            string = last_line.split('INFO  Main ')[1]
            [results['count'], results['ecount']] = re.findall(r"\d{1,}", string)
            results['count'] = int(results['count']); results['ecount'] = int(results['ecount'])
        
        # find all .xml files in path/xml
        results['tcount'] = len(filter(lambda x: x.endswith('.xml'), os.listdir(path+'/xml')))
    
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
    
        ##HEW?  check re-mapfile
        ##HEW? remapfile='%s/%s/mapfiles/%s-%s.xml' % (os.getcwd(),self.root,community,mdprefix)
        ##HEW?if not os.path.isfile(mapfile):
        ##HEW?   remapfile='%s/%s/mapfiles/%s.xml' % (os.getcwd(),self.root,mdprefix)
        ##HEW?  if not os.path.isfile(mapfile):
        ##HEW?    self.logger.error('[ERROR] Mapfile %s does not exist !' % mapfile)
        ##HEW?      return results

        # run re-converting
        # find all .json files in path/json:
        files = filter(lambda x: x.endswith('.json'), os.listdir(path+'/json'))
        
        results['tcount'] = len(files)

        if (not os.path.isdir(path+'/b2find')):
             os.makedirs(path+'/b2find')

        print 'ppp %s' %  path+'/b2find'       
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
            
            ### Mapper post processing
            ##rules=[u'*,,*,,Language,,de,,German,,replace\n']
            ##HEW? if ( mappp == 'True' ):
            ##HEW?   jsondata=UP.postprocess(jsondata,rules)
            ## print 'pjsondata %s' % pjsondata

            ##HEW?### VALIDATE JSON DATA
            ##HEW?if (not UP.validate(jsondata)):
            ##HEW?    logger.info('        |-> Reconvert is aborted')
            ##HEW?    results['ecount'] += 1
            ##HEW?    continue

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
             
        # check output and print it
        ##HEW-D self.logger.info(out)
        ##HEW-D if err: self.logger.error('[ERROR] ' + err)
        
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

    def replace(self,dataset,facetName,old_value,new_value):
        """
        replaces old value - can be a regular expression - with new value for a given facet
        """

        old_regex = re.compile(old_value)

        for facet in dataset:
            if facet == facetName and re.match(old_regex, dataset[facet]):
                dataset[facet] = new_value
                return dataset
            if facet == 'extras':
                for extra in dataset[facet]:
                    if extra['key'] == facetName and re.match(old_regex, extra['value']):
                        extra['value'] = new_value
                        return dataset
        return dataset
 
    def truncate(self,dataset,facetName,old_value,size):
        """
        truncates old value with new value for a given facet
        """
        for facet in dataset:
            if facet == facetName and dataset[facet] == old_value:
                dataset[facet] = old_value[:size]
                return dataset
            if facet == 'extras':
                for extra in dataset[facet]:
                    if extra['key'] == facetName and extra['value'] == old_value:
                        extra['value'] = old_value[:size]
                        return dataset
        return dataset

    def remove_duplicates(self,dataset,facetName,valuearrsep,entrysep):
        """
        remove duplicates      
        """
        for facet in dataset:
          if facet == facetName:
            valarr=dataset[facet].split(valuearrsep)
            valarr=list(OrderedDict.fromkeys(valarr)) ## this elimintas real duplicates
            revvalarr=[]
            for entry in valarr:
               reventry=entry.split(entrysep) ### 
               reventry.reverse()
               reventry=''.join(reventry)
               revvalarr.append(reventry)
               for reventry in revvalarr:
                  if reventry == entry :
                     valarr.remove(reventry)
            dataset[facet]=valuearrsep.join(valarr)
        return dataset       
      
    def splitstring2dictlist(self,dataset,facetName,valuearrsep,entrysep):
        """
        split string in list of string and transfer to list of dict's { "name" : "substr1" }      
        """
        for facet in dataset:
          if facet == facetName:
            ##HEW?? print 'sep %s' % valuearrsep
            valarr=dataset[facet][0]['name'].split()
            valarr=list(OrderedDict.fromkeys(valarr)) ## this elimintas real duplicates
            dicttagslist=[]
            for entry in valarr:
               entrydict={ "name": entry }  
               dicttagslist.append(entrydict)
       
            dataset[facet]=dicttagslist
        return dataset       
      
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
        elif ('url' in jsondata and not self.check_url(jsondata['url'])):
            errmsg = "'url': The source url is broken"
            if(status > 1): status = 1  # set status
            
        if errmsg: self.logger.warning("        [WARNING] field %s" % errmsg)
        
        ## check extra fields ...
        counter = 0
        for extra in jsondata['extras']:
            errmsg = ''
            # ... OAI Identifier
            if(extra['key'] == 'oai_identifier' and extra['value'] == ''):
                errmsg = "'oai_identifier': The ID is missing"
                status = 0  # set status

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
        # Uploads a dataset <jsondata> with name <dsname> as a member of <community> to CKAN. <dsstatus> describes the
        # state of the package and is 'new', 'changed', 'unchanged' or 'unknown'. In the case of a 'new' or 'unknown'
        # package this method will call the API 'package_create' and in the case of a 'changed' package the API 
        # 'package_update'. Nothing happens if the state is 'unchanged'
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
            return urllib.urlopen(url).getcode() < 501
        except IOError:
            return False

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
                self.logger.info('  %d. %s' % (i,pstat['text'][proc]))
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
                    'class="table-disabled"' if (pstat['status'][mode[0]] == 'no') else '',
                    mode[1],
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
                reshtml.write('\t\t<th colspan=\"5\">%s</th>\n' % mode[1])
                    
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
                    reshtml.write('\t\t<th colspan=\"3\">%s</th>\n' % mode[1])
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
                                reshtml.write('<a href="%s_%d.log.txt">%s</a> (%d kB)<br />'% (mode[0],self.get_stats(request,subset,'#id'),mode[1],size))
                            except OSError,e:
                                reshtml.write('%s log file not available!<br /><small><small>(<i>%s</i>)</small></small><br />'% (mode[1], e))
                    reshtml.write('</td>')
                
                    # link error files:
                    reshtml.write('<td valign=\"top\">')
                    for mode in processed_modes:
                        if (pstat['status'][mode[0]] == 'tbd' and os.path.exists(self.jobdir+'/%s_%d.err.txt'%(mode[0],self.get_stats(request,subset,'#id')))):
                            try:
                                size = os.path.getsize(self.jobdir+'/%s_%d.err.txt'%(mode[0],self.get_stats(request,subset,'#id')))
                                if (size != 0):
                                    size = int(size/1024.) or 1
                                reshtml.write('<a href="%s_%d.err.txt">%s</a> (%d kB)<br />'% (mode[0],self.get_stats(request,subset,'#id'),mode[1],size))
                            except OSError,e:
                                reshtml.write('No %s error file! <br /><small><small>(<i>%s</i>)</small></small><br />'% (mode[1], e))
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
        else:
           self.convert_list = './convert_list_' + str(fromdate.date())
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
