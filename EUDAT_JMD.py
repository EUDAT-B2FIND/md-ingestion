"""EUDAT_JMD.py - classes for JMD management : 
  - CKAN_CLIENT  Executes CKAN APIs (interface to CKAN)
  - HARVESTER
  - CONVERTER
  - UPLOADER

Install required modules simplejson, e.g. by :

  > sudo pip install <module>

Or - if this not works for some reason for xml - try
  
  > sudo apt-get install python-lxml


Copyright (c) 2013 John Mrziglod (DKRZ)

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
import re

# needed for HARVESTER class:
###JM# from oaipmh.client import Client
###JM# from oaipmh.metadata import MetadataRegistry
###JM# from oaipmh.error import NoRecordsMatchError, CannotDisseminateFormatError, XMLSyntaxError, NoMetadataFormatsError, BadVerbError, DatestampError

import sickle as SickleClass
from sickle.oaiexceptions import NoRecordsMatch
import uuid, hashlib

# needed for UPLOADER and CKAN class:
import simplejson as json
import urllib, urllib2
import httplib
from urlparse import urlparse

from lxml import etree
import traceback

### CKAN_CLIENT - class
# Provides the method action()

class CKAN_CLIENT(object):

    def __init__ (self, ip_host, api_key):
	    self.ip_host = ip_host
	    self.api_key = api_key
	    self.logger = log.getLogger()

	    # which api should call for this action?
	    self.allowed_action = {
		    'package_create': 1,
		    'package_update': 1,
		    'package_owner_org_update': 1,
		    'package_delete': 1,
		    'package_delete_all': 1,
		    'package_activate_all': 1,
		    'package_show': 1,
		    'package_list': 1,
		    'group_show':1,
		    'group_create':1,
		    'group_package_show':1,
		    'group_list':1,
		    'group_purge':1,
		    'member_create': 1,
		    'member_list':1,
		    'organization_member_create':1,
		    'bulk_update_delete':1,
	    }
	
    def validate_action(self,action):
        return True
        if (action in self.allowed_action):
            return True
        else:
            return False
	
	## action (CKAN_CLIENT object, action, data) - method
	# Call the api action <action> on the <CKAN_CLIENT object>.
	#
	# Parameters:
	# -----------
	# (string)  action - ...
	# (dict)    data - dictionary with the json data
	#
	# Return Values:
	# --------------
	# ...
	
    def action(self, action, data={}):
	    if (not self.validate_action(action)):
		    print '[ERROR] Action name '+ str(action) +' is not defined in CKAN_CLIENT! Allowed actions are:'
		    list = self.allowed_action.keys()
		    print '\n'.join(sorted(list))
	    else:
		    return self.__action_api(action, data)
		
    def __action_api (self, action, data_dict):
        # Make the HTTP request for data set generation.
        response=''
        rvalue = 0
        api_url = "http://{host}/api/rest".format(host=self.ip_host)
        action_url = "{apiurl}/dataset".format(apiurl=api_url)	# default for 'package_create'

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
        else:
            action_url = "http://{host}/api/3/action/{action}".format(host=self.ip_host,action=action)

        data_string = urllib.quote(json.dumps(data_dict))

        self.logger.debug('\t|-- Action %s\n\t|-- Calling %s\n\t|-- Object %s ' % (action,action_url,data_dict))	
        try:
            request = urllib2.Request(action_url)
            request.add_header('Authorization', self.api_key)
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





### HARVESTER - class
# The HARVESTER class provides the method 'harvester()' which can harvest metadata from a OAI-PMH server

class HARVESTER(object):
    
    def __init__ (self, OUT, pstat, base_outdir, fromdate):
        self.logger = log.getLogger()
        self.pstat = pstat
        self.OUT = OUT
        self.base_outdir = base_outdir

        # convert 'fromdate' string to datetime object
#        if (fromdate) and ('-' in fromdate):
#            fromdate = list(map(int,fromdate.split('-')))
#            fromdate = datetime.datetime(fromdate[0],fromdate[1],fromdate[2],fromdate[3],fromdate[4],fromdate[5])
            
        self.fromdate = fromdate

    def checkURL(self,url): 
        from django.core.validators import URLValidator, ValidationError
        from django.conf import settings
        settings.configure()

        val = URLValidator()
        try:
            val(url)
        except ValidationError, e:
            print (e,url)
            return False
        except Exception, e:
            print (e,url)
            return False                    
        else:
            return True
            
    
    def harvest_sickle(self, request):
    
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
        
        # create sickle object:
        sickle = SickleClass.Sickle(req['url'], max_retries=3, timeout=30)
        
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
    
        self.logger.info('    |   | %-4s | %-45s | %-45s |\n    |%s|' % ('#','OAI Identifier','DS Identifier',"-" * 103))
        try:
            for record in sickle.ListRecords(**{'metadataPrefix':req['mdprefix'],'set':req['mdsubset'],'ignore_deleted':True,
                'from':self.fromdate}):
                
                stats['tcount'] += 1

                # get the id of the metadata file:
                oai_id = record.header.identifier
                
                # generate a uniquely identifier for this dataset:
                uid = str(uuid.uuid5(uuid.NAMESPACE_DNS, oai_id.encode('ascii','replace')))
                
                xmlfile = subsetdir + '/xml/' + os.path.basename(uid) + '.xml'
                try:
                    self.logger.info('    | h | %-4d | %-45s | %-45s |' % (stats['count']+1,oai_id,uid))
                    self.logger.debug('Harvested XML file written to %s' % xmlfile)
                        
                    metadata = record.raw
                    if (metadata):
                        metadata = metadata.encode('ascii', 'ignore')
                    
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
                            
                        
                except TypeError:
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
    
    
    ## save_subset(self, req, stats, subset, subsetdir, count_set) - method
    # Save stats per subset and add subset item to the convert_list via OUT.print_convert_list()
    #
    # Return Values:
    # --------------
    # 1. (string)   directory to the current subset folder
    # 2. (integer)  counter of the current subset
        
    def save_subset(self, req, stats, subset, subsetdir, count_set):
    
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



### CONVERTER - class
# Provides the method convert()

class CONVERTER(object):

##HEW-SVN    def __init__ (self, OUT, root='converter/java/target/current'):
##HEW-GIT
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



    ## convert (CONVERTER object, community, mdprefix, path) - method
    # Converts the XML files in directory <path> to JSON files with Lari's java converter
    #
    # Parameters:
    # -----------
    # (string)  community - ...
    # (string)  mdprefix - ...
    # (string)  path - path to subset directory
    #
    # Return Values:
    # --------------
    # 1. (dict) results statistics
    
    def convert(self,community,mdprefix,path):
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

        # run the converter:
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
        
        last_line = out.split('\n')[-2]
        if ('INFO  Main - ' in last_line):
            string = last_line.split('INFO  Main ')[1]
            [results['count'], results['ecount']] = re.findall(r"\d{1,}", string)
            results['count'] = int(results['count']); results['ecount'] = int(results['ecount'])
        
        # find all .xml files in path/xml:
        results['tcount'] = len(filter(lambda x: x.endswith('.xml'), os.listdir(path+'/xml')))
    
        return results



### UPLOADER - class
# Provides the methods get_packages(), check_dataset() and upload()

class UPLOADER (object):
    
    def __init__(self, CKAN, OUT):
        self.logger = log.getLogger()
        self.CKAN = CKAN
        self.OUT = OUT
        
        self.package_list = dict()
        
    def get_packages(self,community):
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
        self.OUT.save_stats('#GetPackages','','time',ptime)
        self.OUT.save_stats('#GetPackages','','count',len(package_list))
    
    ## validate (UPLOADER object, json data) - method
    # Validate the json data (e.g. the PublicationTimestamp field)
    #
    # Parameters:
    # -----------
    # (dict)    json data
    #
    # Return Values:
    # --------------
    # 1. (boolean) validation result
    
    def validate(self, jsondata):
        result = True
        errmsg = ''
        must_have_extras = {
            "oai_identifier":True
        }
        
        ## check main fields ...
        if (not('title' in jsondata) or jsondata['title'] == ''):
            errmsg = "'title': The title is missing"
            result = False
        elif ('url' in jsondata and not self.check_url(jsondata['url'])):
            errmsg = "'url': The source url is broken"
            result = False
        
        # check extra fields ...
        for extra in jsondata['extras']:
            # ... OAI Identifier
            if(extra['key'] == 'oai_identifier' and extra['value'] == ''):
                errmsg = "'oai_identifier': The ID is missing"
                result = False
            
            # ... PublicationTimestamp
            elif(extra['key'] == 'PublicationTimestamp'):
                try:
                    datetime.datetime.strptime(extra['value'], '%Y-%m-%d'+'T'+'%H:%M:%S'+'Z')
                except ValueError:
                    errmsg = "'PublicationTimestamp': Incorrect data format, should be YYYY-MM-DDThh:mm:ssZ"
                    result = False
            
            if not result: break
            if extra['key'] in must_have_extras: del must_have_extras[extra['key']]
                    
        if (not result):
            self.logger.warning("        [ERROR] JSON field %s" % errmsg)
        elif (len(must_have_extras) > 0):
            self.logger.warning("        [ERROR] JSON extra fields %s are missing" % must_have_extras.keys())
            result = False
            
        return result

    def upload(self, ds, dsstatus, community, jsondata):
       
        rvalue = 0
        jsondata["name"] = ds
        jsondata["state"]='active'
        jsondata["groups"]=[{ "name" : community }]
        jsondata["owner_org"]="eudat"
   
        # if the dataset checked as new is not in ckan package_list then create it with package_create:
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
        
        # if the dsstatus is changed then update it with package_update:
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


    def delete(self, ds, ckanstatus):
        rvalue = 0
        jsondata = {
            "name" : ds,
            "state" : 'deleted'
        }
   
        # if the dataset exists set it to status deleted in CKAN:
        if (not ckanstatus == 'new'):
            self.logger.debug('\t - Try to set dataset %s on status deleted' % ds)
            
            results = self.CKAN.action('package_update',jsondata)
            if (results and results['success']):
                rvalue = 1
            else:
                self.logger.debug('\t - Deletion failed. Maybe dataset already removed.')
        
        return rvalue
        
    def check_dataset(self,ds,checksum): 
        ckanstatus='failed'
        if not (ds in self.package_list):
            ckanstatus="new"
        else:
            if ( checksum == self.package_list[ds]):
                ckanstatus="unchanged"
            else:
                ckanstatus="changed"
        return ckanstatus
    
    def check_url(self,url):
        return urllib.urlopen(url).getcode() < 400


### OUTPUT - class
# Provides methods to create the log and error files and the overview HTML file

class OUTPUT (object):
    def __init__(self, pstat, now, jid, options):

        self.options = options
        self.pstat = pstat
        self.start_time = now
        self.jid = jid
        
        # create jobdir if it is necessary:
        if (options.jobdir):
            self.jobdir = options.jobdir
        else:
            self.jobdir='log/%s/%s/%s_%s_%s' % (now.split(' ')[0],now.split(' ')[1].split(':')[0],jid,options.mode,options.list)
            
        if not os.path.exists(self.jobdir):
            os.makedirs(self.jobdir)
            
        self.convert_list = None
        self.verbose = options.verbose
        
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


    
    ## save_stats (OUT object, request, subset, mode, stats) - method
    # Saves the stats of a process (harvesting, converting or uploading) per subset.
    # Requests which start with a '#' are special requests like '#Start' or '#GetPackages'
    # will be ignored in the most actions
    #
    # Parameters:
    # -----------
    # (string)  request - main request (community-mdprefix)
    # (string)  subset - ...
    # (string)  mode - process mode (can be 'h', 'c' or 'u')
    # (dict)    stats - a dictionary with results stats
    #
    # Return Values:
    # --------------
    # None
    
    def save_stats(self,request,subset,mode,stats):
        # self.stats is a list with all results statistics 
        # of the harvesting, converting and uploading routines
        
        
        if(not request in self.stats):
            # create request dictionary:
            self.stats[request] = dict()
        
        if (request.startswith('#')):
            # special request e.g. '#Start':
            self.stats[request][mode] += stats
        else:
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
        
            for k in stats:
                self.stats[request][subset][mode][k] += stats[k]
            
            if (self.stats[request][subset][mode]['count'] != 0): 
                self.stats[request][subset][mode]['avg'] = \
                    self.stats[request][subset][mode]['time'] / self.stats[request][subset][mode]['count']
            else: 
                self.stats[request][subset][mode]['avg'] = 0

        
        if (not request.startswith('#') or request == '#Start'):
            ## Error and log files are used for all normal requests and the #Start procedure

            # shutdown the logger                                                                       
            log.shutdown()
            
            
            logdir='%s' % self.jobdir
            if (not os.path.exists(logdir)):
               os.makedirs(logdir)     

            logfile, errfile = '',''
            if (request == '#Start'):
                logfile='%s/start.log.txt' % (logdir)
                errfile='%s/start.err.txt' % (logdir)
            else:
                logfile='%s/%s_%s.log.txt' % (logdir,mode,self.get_stats(request,subset,'#id'))
                errfile='%s/%s_%s.err.txt' % (logdir,mode,self.get_stats(request,subset,'#id'))

            try:
                # move log files:
                if (os.path.exists(logdir+'/myapp.log')):
                    os.rename(logdir+'/myapp.log',logfile )
                if (os.path.exists(logdir+'/myapp.err')):                                                        
                    os.rename(logdir+'/myapp.err',errfile )
            except OSError, e:
                print("[ERROR] Cannot move log and error files to %s and %s: %s\n" % (logfile,errfile,e))
            else:
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
                # returns the sum of the keys in the modes in all subsets in the request
                
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
        pstat = self.pstat
        options = self.options
    
        # open results.html
        reshtml = open(self.jobdir+'/overview.html', 'w')
        
        # write header of html file:
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
        reshtml.write("\t\t<h1>Results of JMD Ingest workflow</h1>\n")
        reshtml.write('\t\t<b>Date:</b> %s UTC, <b>Process ID:</b> %s, <b>Epic check:</b> %s<br />\n\t\t<ol>\n' % (self.start_time, self.jid, options.epic_check))
        
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
            

class Reader(object):
     '''Very simple encoder of harvested content: convert to string'''
     def __call__(self, element):
        return etree.tostring(element, pretty_print=True, encoding='UTF8')
