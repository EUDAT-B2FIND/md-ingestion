#!/usr/bin/env python

"""mdmanager.py
  Management of metadata 

Copyright (c) 2016 Heinrich Widmann (DKRZ) Licensed under AGPLv3.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""

import os, optparse, sys, glob
import time, datetime
import simplejson as json

import logging
import traceback

# 
import urllib2
import urllib, urllib2, socket
from itertools import tee 

# needed for HARVESTER class:
import sickle as SickleClass
from sickle.oaiexceptions import NoRecordsMatch
from requests.exceptions import ConnectionError
import lxml.etree as etree
import uuid

# needed for MAPPER :
from pyparsing import *
import Levenshtein as lvs
import iso639
import codecs
import re
import xml.etree.ElementTree as ET
import io

# needed for UPLOADER class:
from collections import OrderedDict
import ckanapi

class HARVESTER(object):
    
    """
    ### HARVESTER - class
    # Provides methods to harvest metadata records via protocols as OAI-PMH
    #
    # Parameters:
    # -----------
    # 1. (dict)         pstat   - dictionary with the states of every process (was built by main.pstat_init())
    # 2. (path)         rootdir - rootdir where the subdirs will be created and the harvested files will be saved.
    # 3. (string)       fromdate  - filter for harvesting, format: YYYY-MM-DD
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
    HV = HARVESTER(pstat,rootdir,fromdate)

    # harvest from a source via sickle module:
    request = [
                    community,
                    source,
                    verb,
                    mdprefix,
                    mdsubset
                ]
    results = HV.harvest(request)
    """
    
    def __init__ (self, pstat, base_outdir, fromdate):
        # choose the debug level:
##        log.basicConfig(format='III')
##        self.log_level = {
##            'log':log.INFO,
##            'err':log.ERROR,
##            'err':log.DEBUG,
##            'std':log.INFO,
##        }
        
##        self.logger = log.getLogger()
##        self.logger.setLevel(log.DEBUG)
        self.pstat = pstat
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
        
        start=time.time()

        sickle = SickleClass.Sickle(req['url'], max_retries=3, timeout=300)
        outtypedir='xml'
        outtypeext='xml'
        oaireq=getattr(sickle,req["lverb"], None)
        try:
            records,rc=tee(oaireq(**{'metadataPrefix':req['mdprefix'],'set':req['mdsubset'],'ignore_deleted':True,'from':self.fromdate}))
        except urllib2.HTTPError as e:
            logging.error("[ERROR: %s ] Cannot harvest through request %s\n" % (e,req))
            return -1
        except ConnectionError as e:
            logging.error("[ERROR: %s ] Cannot harvest through request %s\n" % (e,req))
            return -1
        except etree.XMLSyntaxError as e:
            self.logger.error("[ERROR: %s ] Cannot harvest through request %s\n" % (e,req))
            return -1
        except Exception, e:
            self.logger.error("[ERROR %s ] : %s" % (e,traceback.format_exc()))
            return -1

        ntotrecs=sum(1 for _ in rc)

        logging.info("\t|- Iterate through %d records in %d sec" % (ntotrecs,time.time()-start))
        
        logging.debug('    |   | %-4s | %-45s | %-45s |\n    |%s|' % ('#','OAI Identifier','DS Identifier',"-" * 106))

        # set subset:
        if (not req["mdsubset"]):
            subset = 'SET'
        else:
            subset = req["mdsubset"]

        # make subset dir:
        subsetdir = '/'.join([self.base_outdir,req['community']+'-'+req['mdprefix'],subset])

        noffs=0 # set to number of record, where harvesting should start
        start2=time.time()
        fcount=0
        oldperc=0

        logging.info("\t|- Get records and store on disc ...")
        for record in records:
            stats['tcount'] += 1
            ## counter and progress bar
            fcount+=1
            if fcount <= noffs : continue
            perc=int(fcount*100/ntotrecs)
            bartags=perc/5 #HEW-D fcount/100
            if perc%10 == 0 and perc != oldperc :
                oldperc=perc
                print "\r\t[%-20s] %5d (%3d%%) in %d sec" % ('='*bartags, fcount, perc, time.time()-start2 )
                sys.stdout.flush()


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

class MAPPER():

    """
    ### MAPPER - class
    # Parameters:
    # -----------
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

    def __init__ (self):
        self.logger = logging.getLogger()
        
        # B2FIND metadata fields
        self.b2findfields =["title","notes","tags","url","DOI","PID","Checksum","Rights","Discipline","author","Publisher","PublicationYear","PublicationTimestamp","Language","TemporalCoverage","SpatialCoverage","Format","Contact","MetadataAccess"]


        self.ckan2b2find = OrderedDict()
        self.ckan2b2find={
                   "title" : "title", 
                   "notes" : "description",
                   "tags" : "tags",
                   "url" : "Source", 
                   "DOI" : "DOI",
###                   "IVO" : "IVO",
                   "PID" : "PID",
                   "Checksum" : "checksum",
                   "Rights" : "rights",
##                   "Community" : "community",
                   "Discipline" : "discipline",
                   "author" : "Creator", 
                   "Publisher" : "Publisher",
                   "PublicationYear" : "PublicationYear",
                   "PublicationTimestamp" : "PublicationTimestamp",
                   "Language" : "language",
                   "TemporalCoverage" : "temporalcoverage",
                   "SpatialCoverage" : "spatialcoverage",
                   "spatial" : "spatial",
                   "Format" : "format",
                   "Contact" : "contact",
                   "MetadataAccess" : "metadata"
                              }  

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

    def date2UTC(self,old_date):
        """
        changes date to UTC format
        """
        # UTC format =  YYYY-MM-DDThh:mm:ssZ
        try:
            utc = re.compile(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z')

            utc_day1 = re.compile(r'\d{4}-\d{2}-\d{2}') # day (YYYY-MM-DD)
            utc_day = re.compile(r'\d{8}') # day (YYYYMMDD)
            utc_year = re.compile(r'\d{4}') # year (4-digit number)
            if utc.search(old_date):
                new_date = utc.search(old_date).group()
                return new_date
            elif utc_day1.search(old_date):
                day = utc_day1.search(old_date).group()
                new_date = day + 'T11:59:59Z'
                return new_date
            elif utc_day.search(old_date):
                rep=re.findall(utc_day, old_date)[0]
                new_date = rep[0:4]+'-'+rep[4:6]+'-'+rep[6:8] + 'T11:59:59Z'
                return new_date
            elif utc_year.search(old_date):
                year = utc_year.search(old_date).group()
                new_date = year + '-07-01T11:59:59Z'
                return new_date
            else:
                return '' # if converting cannot be done, make date empty
        except Exception, e:
           self.logger.error('[ERROR] : %s - in date2UTC replace old date %s by new date %s' % (e,old_date,new_date))
           return ''
        else:
           return new_date


    def map_identifiers(self, invalue):
        """
        Convert identifiers to data access links, i.e. to 'Source' (ds['url']) or 'PID','DOI' etc. pp
 
        Copyright (C) 2015 by Heinrich Widmann.
        Licensed under AGPLv3.
        """
        try:
            ## idarr=invalue.split(";")
            iddict=dict()
            favurl=invalue[0]  ### idarr[0]
  
            for id in invalue :
                if id.startswith('http://data.theeuropeanlibrary'):
                    iddict['url']=id
                elif id.startswith('ivo:'):
                    iddict['IVO']='http://registry.euro-vo.org/result.jsp?searchMethod=GetResource&identifier='+id
                    favurl=iddict['IVO']
                elif id.startswith('10.'): ##HEW-??? or id.startswith('10.5286') or id.startswith('10.1007') :
                    iddict['DOI'] = self.concat('http://dx.doi.org/',id)
                    favurl=iddict['DOI']
                elif 'dx.doi.org/' in id:
                    iddict['DOI'] = id
                    favurl=iddict['DOI']
                elif 'doi:' in id: ## and 'DOI' not in iddict :
                    iddict['DOI'] = 'http://dx.doi.org/doi:'+re.compile(".*doi:(.*)\s?.*").match(id).groups()[0].strip(']')
                    favurl=iddict['DOI']
                elif 'hdl.handle.net' in id:
                    iddict['PID'] = id
                    favurl=iddict['PID']
                elif 'hdl:' in id:
                    iddict['PID'] = id.replace('hdl:','http://hdl.handle.net/')
                    favurl=iddict['PID']
                ##  elif 'url' not in iddict: ##HEW!!?? bad performance --> and self.check_url(id) :
                    ##     iddict['url']=id

            if 'url' not in iddict :
                iddict['url']=favurl
        except Exception, e:
            self.logger.error('[ERROR] : %s - in map_identifiers %s can not converted !' % (e,invalue))
            return None
        else:
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
           self.logger.debug('[ERROR] : %s - in map_temporal %s can not converted !' % (e,invalue))
           return (None,None,None)
        else:
            return (desc,None,None)

    def is_float_try(self,str):
            try:
                float(str)
                return True
            except ValueError:
                return False

    def map_spatial(self,invalue):
        """
        Map coordinates to spatial
 
        Copyright (C) 2014 Heinrich Widmann
        Licensed under AGPLv3.
        """
        desc=''
        pattern = re.compile(r";|\s+")
        try:
          if type(invalue) is not list :
              invalue=invalue.split() ##HEW??? [invalue]
          coordarr=list()
          nc=0
          for val in invalue:
              if type(val) is dict :
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
                  valarr=val.split()
                  for v in valarr:
                      if self.is_float_try(v) is True :
                          coordarr.append(v)
                          nc+=1
                      else:
                          desc+=' '+v
          if len(coordarr)==2 :
              desc+=' boundingBox : [ %s , %s , %s, %s ]' % (coordarr[0],coordarr[1],coordarr[0],coordarr[1])
              return(desc,coordarr[0],coordarr[1],coordarr[0],coordarr[1])
          elif len(coordarr)==4 :
              desc+=' boundingBox : [ %s , %s , %s, %s ]' % (coordarr[0],coordarr[1],coordarr[2],coordarr[3])
              return(desc,coordarr[0],coordarr[1],coordarr[2],coordarr[3])
        except Exception, e:
           self.logger.error('[ERROR] : %s - in map_spatial invalue %s can not converted !' % (e,invalue))
           return (None,None,None,None,None) 
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
                 self.logger.error('[ERROR] %s in map_discipl : %s can not compared to %s !' % (e,indisc,disc))
                 continue
             if r > maxr  :
                 maxdisc=disc
                 maxr=r
                 ##HEW-T                   print '--- %s \n|%s|%s| %f | %f' % (line,indisc,disc,r,maxr)
           if maxr == 1 and indisc == maxdisc :
               self.logger.debug('  | Perfect match of %s : nothing to do' % indisc)
               retval.append(indisc.strip())
           elif maxr > 0.90 :
               self.logger.debug('   | Similarity ratio %f is > 0.90 : replace value >>%s<< with best match --> %s' % (maxr,indisc,maxdisc))
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
                if pattern is None :
                    if nfield :
                        outvalue.append(elem[nfield])
                    else:
                        outvalue.append(elem)
                else:
                    rep=re.findall(pattern, elem)
                    if len(rep) > 0 :
                        outvalue.append(rep[nfield])
                    else:
                        outvalue.append(elem)
                        
        ##else:
        ##    log.error('[ERROR] : cut expects as invalue (%s) a list' % invalue)
            ## return None

        return outvalue

    def list2dictlist(self,invalue,valuearrsep):
        """
        transfer list of strings/dicts to list of dict's { "name" : "substr1" } and
          - eliminate duplicates, numbers and 1-character- strings, ...      
        """

        dictlist=[]
        valarr=[]
        bad_chars = '(){}<>:'
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
                    ##valarr=filter(None, re.split(r"([,\!?:;])+",lentry)) ## ['name']))
                    valarr=re.findall('\[[^\]]*\]|\([^\)]*\)|\"[^\"]*\"|\S+',lentry)
                for entry in valarr:
                    entry="". join(c for c in entry if c not in bad_chars)
                    if isinstance(entry,int) or len(entry) < 2 : continue
                    entry=entry.encode('utf-8').strip()
                    dictlist.append({ "name": entry.replace('/','-') })
            except AttributeError, err :
                log.error('[ERROR] %s in list2dictlist of lentry %s , entry %s' % (err,lentry,entry))
                continue
            except Exception, e:
                log.error('[ERROR] %s in list2dictlist of lentry %s, entry %s ' % (e,lentry,entry))
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
           ##utctime=datetime.datetime(utc).isoformat()
           ##print 'utctime %s' % utctime
           utctime = datetime.datetime.strptime(utc, "%Y-%m-%dT%H:%M:%SZ") ##HEW-?? .isoformat()
           diff = utc1900 - utctime
           diffsec= int(diff.days) * 24 * 60 *60
           if diff > datetime.timedelta(0): ## date is before 1900
              sec=int(time.mktime((utc1900).timetuple()))-diffsec+year1epochsec
           else:
              sec=int(time.mktime(utctime.timetuple()))+year1epochsec
        except Exception, e:
           self.logger.error('[ERROR] : %s - in utc2seconds date-time %s can not converted !' % (e,utc))
           return None

        return sec

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
        self.logger.debug(' | %10s | %10s | %10s | \n' % ('Field','XPATH','Value'))

        jsondata=dict()

        for line in xrules:
          try:
            m = re.match(r'(\s+)<field name="(.*?)">', line)
            if m:
                field=m.group(2)
                if field in ['Discipline','oai_set']: ## HEW!!! expand to all mandatory fields !!
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
                    self.logger.debug(' | %-10s | %10s | %20s | \n' % (field,xpath,retval[:20]))
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
            mapext='conf' ##HEW!! --> json !!!!
            insubdir='/hjson'
            infformat='json'
        else:
            mapext='xml'
            insubdir='/xml'
            infformat='xml'

        # check input and output paths
        if not os.path.exists(path):
            self.logger.error('[ERROR] The directory "%s" does not exist! No files to map !' % (path))
            return results
        elif not os.path.exists(path + insubdir) or not os.listdir(path + insubdir):
            self.logger.error('[ERROR] The input directory "%s%s" does not exist or no %s-files to convert are found !\n(Maybe your convert list has old items?)' % (path,insubdir,insubdir))
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
                ##??? perc=int(fcount*100/int(100)) ##HEW-?? len(records) not known

            if perc%10 == 0 and perc != oldperc:
                oldperc=perc
                self.logger.info("\r\t[%-20s] %5d (%3d%%) in %d sec" % ('='*bartags, fcount, perc, time.time()-start ))
                sys.stdout.flush()

            jsondata = dict()

            infilepath=path+insubdir+'/'+filename      
            if ( os.path.getsize(infilepath) > 0 ):
                ## load and parse raw xml rsp. json
                with open(infilepath, 'r') as f:
                    try:
                        if  mdprefix == 'json':
                            jsondata=json.loads(f.read())
                            ##HEW-D ???!!! hjsondata=json.loads(f.read())
                        else:
                            xmldata= ET.parse(infilepath)
                    except Exception as e:
                        logging.error('    | [ERROR] %s : Cannot load or parse %s-file %s' % (e,infformat,infilepath))
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
                        self.logger.debug('    | xpath | %-4d | %-45s |' % (fcount,os.path.basename(filename)))
                        jsondata=self.xpathmdmapper(xmldata,maprules,namespaces)
                    except Exception as e:
                        logging.error('    | [ERROR] %s : during XPATH processing' % e )
                        results['ecount'] += 1
                        continue
                try:
                   ## md postprocessor
                   if (specrules):
                       self.logger.debug(' [INFO]:  Processing according specrules %s' % specrules)
                       jsondata=self.postprocess(jsondata,specrules)
                except Exception as e:
                    self.logger.error(' [ERROR] %s : during postprocessing' % (e))
                    continue

                iddict=dict()
                blist=list()
                spvalue=None
                stime=None
                etime=None
                publdate=None
                # loop over all fields
                for facet in jsondata:
                   logging.debug('facet %s ...' % facet)
                   try:
                       if facet == 'author':
                           jsondata[facet] = self.uniq(self.cut(jsondata[facet],'\(\d\d\d\d\)',1),';')
                       elif facet == 'tags':
                           jsondata[facet] = self.list2dictlist(jsondata[facet]," ")
                       elif facet == 'url':
                           iddict = self.map_identifiers(jsondata[facet])
                           if 'url' in iddict: ## and iddict['url'] != '': 
                               jsondata[facet]=iddict['url']
                       elif facet == 'DOI':
                           iddict = self.map_identifiers(jsondata[facet])
                           if 'DOI' in iddict : 
                               jsondata[facet]=iddict['DOI']
                               ##if 'url' not in jsondata:
                               ##    jsondata['url']=iddict['DOI']
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
                           spdesc,slat,wlon,nlat,elon = self.map_spatial(jsondata[facet])
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
                            publdate=self.date2UTC(jsondata[facet][0])
                            if publdate:
                                jsondata[facet] = self.cut([publdate],'\d\d\d\d',0)
                            else:
                                jsondata[facet] = None
                   except Exception as e:
                       self.logger.error(' [WARNING] %s : during mapping of\n\tfield\t%s\n\tvalue%s' % (e,facet,jsondata[facet]))
                       continue

                if iddict :
                    if 'DOI' in iddict :
                        jsondata['DOI']=iddict['DOI']
                    if 'PID' in iddict : jsondata['PID']=iddict['PID']
                if 'url' not in jsondata:
                    if 'DOI' in jsondata:
                        jsondata['url']=jsondata['DOI']
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
                        logging.debug('   | [INFO] decode json data')
                        data = json.dumps(jsondata,sort_keys = True, indent = 4).decode('utf8')
                    except Exception as e:
                        logging.error('    | [ERROR] %s : Cannot decode jsondata %s' % (e,jsondata))
                    try:
                        logging.debug('   | [INFO] save json file')
                        json_file.write(data)
                    except TypeError, err :
                        logging.error('    | [ERROR] Cannot write json file %s : %s' % (outpath+'/'+filename,err))
                    except Exception as e:
                        logging.error('    | [ERROR] %s : Cannot write json file %s' % (e,outpath+'/'+filename))
                        err+='Cannot write json file %s' % outpath+'/'+filename
                        results['ecount'] += 1
                        continue
            else:
                results['ecount'] += 1
                continue


        out=' %s to json stdout\nsome stuff\nlast line ..' % infformat
        if (err is not None ): self.logger.error('[ERROR] ' + err)

        self.logger.info(
                '   \t|- %-10s |@ %-10s |\n\t| Provided | Mapped | Failed |\n\t| %8d | %6d | %6d |' 
                % ( 'Finished',time.strftime("%H:%M:%S"),
                    results['tcount'],
                    fcount,
                    results['ecount']
                ))

        # search in output for result statistics
        last_line = out.split('\n')[-2]
        if ('INFO  Main - ' in last_line):
            string = last_line.split('INFO  Main ')[1]
            [results['count'], results['ecount']] = re.findall(r"\d{1,}", string)
            results['count'] = int(results['count']); results['ecount'] = int(results['ecount'])
        
    
        return results




def main():
    global TimeStart
    TimeStart = time.time()

    # check the version from svn:
    global ManagerVersion
    ManagerVersion = '2.0'

    # parse command line options and arguments:
    modes=['h','harvest','c','convert','m','map','v','validate','o','oaiconvert','u','upload','h-c','c-u','h-u', 'h-d', 'd','delete']
    p = options_parser(modes)
    global options
    options,arguments = p.parse_args()
    
    # check option 'mode' and generate process list:
    (mode, pstat) = pstat_init(p,modes,options.mode,options.source,options.iphost)
    
    # make jobdir
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    jid = os.getpid()
    # create logger and OUT output handler and initialise it:
##HEW-DDD    OUT = B2FIND.OUTPUT(pstat,now,jid,options)
##HEW-DDD    global logger 
##HEW-DDD    logger = logging.getLogger()
    
    # print out general info:
    print '\nVersion:  \t%s' % ManagerVersion
    print 'Run mode:   \t%s' % pstat['short'][mode]
    logging.debug('Process ID:\t%s' % str(jid))
    logging.debug('Processing status:\t')
    for key in pstat['status']:
        logging.debug(" %s\t%s" % (key, pstat['status'][key]))
    # check options:

    if ( pstat['status']['u'] == 'tbd'):
    
        # checking given options:
        if (options.iphost):
          if (not options.auth):
             from os.path import expanduser
             home = expanduser("~")
             if(not os.path.isfile(home+'/.netrc')):
                logger.critical('[CRITICAL] Can not access job host authentification file %s/.netrc ' % home )
                exit()
             f = open(home+'/.netrc','r')
             lines=f.read().splitlines()
             f.close()

             l = 0
             for host in lines:
                if(options.iphost == host.split()[0]):
                   options.auth = host.split()[1]
                   break
             logger.debug(
                'NOTE : For upload mode write access to %s by API key must be allowed' % options.iphost
             )
             if (not options.auth):
                logger.critical('[CRITICAL] API key is neither given by option --auth nor can retrieved from %s/.netrc' % home )
                exit()
        else:
            logger.critical(
                "\033[1m [CRITICAL] " +
                    "For upload mode valid URL of CKAN instance (option -i) and API key (--auth) must be given" + "\033[0;0m"
            )
            sys.exit(-1)
            
    ## START PROCESSING:
    print "Start : \t%s\n" % now
    logging.info("Loop over processes and related requests :\n")
    logging.info('|- <Process> started : %s' % "<Time>")
    logging.info(' |- Joblist: %s' % "<Filename of request list>")
    logging.info('   |# %-15s : %-30s \n\t|- %-10s |@ %-10s |' % ('<ReqNo.>','<Request description>','<Status>','<Time>'))



    try:
        # start the process:
        process(options,pstat)
        exit()
    except Exception, e:
        logging.critical("[CRITICAL] Program is aborted because of a critical error! Description:")
        logging.critical("%s" % traceback.format_exc())
        exit()
    finally:
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        logging.info("\nEnd :\t\t%s" % now)


def process(options,pstat):
    ## process (options,pstat,OUT) - function
    # Starts the specific process routines for harvesting, converting, mapping, validating, oai-converting and/or uploading
    #
    # Parameters:
    # -----------
    # 1. (OptionsParser object)    - 
    #
    # Return Values:
    # --------------
    # return values

    ##HEW-D logging.basicConfig(format='process:')
    
    # set single or multi mode:
    mode = None
    if(options.source):
        mode = 'single'
        
        if ( not(
            options.community and
            options.source and
            options.verb and
            options.mdprefix
        )):
            logging.critical("\033[1m [CRITICAL] " + "When single mode is used following options are required:\n\t%s" % (
                '\n\t'.join(['community','source','verb','mdprefix'])) + "\033[0;0m" 
            )
            exit()
        
    elif(options.list):
        mode = 'multi'
    else:
        logging.critical("[CRITICAL] Either option source (option -s) or list of sources (option -l) is required")
        exit()
    
    ## HARVESTING mode:    
    if (pstat['status']['h'] == 'tbd'):
        # start the process harvesting:
        print '\n|- Harvesting started : %s' % time.strftime("%Y-%m-%d %H:%M:%S")
        HV = HARVESTER(pstat,options.outdir,options.fromdate)
        
        if mode is 'multi':
            logging.info(' |- Joblistx:  \t%s' % options.list)
            logging.debug(' |- Community:\t%s' % options.community)
            logging.debug(' |- OAI subset:\t%s' % options.mdsubset)
            logging.debug(' |- Source MD format:\t%s' % options.mdprefix)

            process_harvest(HV,parse_list_file('harvest',options.list, options.community,options.mdsubset,options.mdprefix))
        else:
            process_harvest(HV,[[
                options.community,
                options.source,
                options.verb,
                options.mdprefix,
                options.mdsubset
            ]])

    if (pstat['status']['h'] == 'no'):
        ## MAPPINING - Mode:  
        if (pstat['status']['m'] == 'tbd'):
            # start the process mapping:
            logging.info('\n|- Mapping started : %s' % time.strftime("%Y-%m-%d %H:%M:%S"))
            MP = MAPPER()
        
            # start the process mapping:
            if mode is 'multi':
                convert_list='harvest_list'
                logging.info(' |- Joblist:  \t%s' % convert_list )
                if (options.community != '') : logging.debug(' |- Community:\t%s' % options.community)
                if (options.mdsubset != None) : logging.debug(' - OAI subset:\t%s' % options.mdsubset)
                process_map(MP, parse_list_file('convert', convert_list or options.list, options.community,options.mdsubset, options.mdprefix, options.target_mdschema))
            else:
                process_map(MP,[[
                    options.community,
                    options.source,
                    options.mdprefix,
                    options.outdir + '/' + options.mdprefix,
                    options.mdsubset,
                    options.target_mdschema
                ]])
        ## VALIDATOR - Mode:  
        if (pstat['status']['v'] == 'tbd'):
            MP = MAPPER()
            logging.info('\n|- Validating started : %s' % time.strftime("%Y-%m-%d %H:%M:%S"))
        
            # start the process converting:
            if mode is 'multi':
                logging.info(' |- Joblist:  \t%s' % options.list)
                process_validate(MP, parse_list_file('validate', convert_list or options.list, options.community,options.mdsubset))
            else:
                process_validate(MP,[[
                    options.community,
                    options.source,
                    options.mdprefix,
                    options.outdir + '/' + options.mdprefix,
                    options.mdsubset
                ]])
        ## UPLOADING - Mode:  
        if (pstat['status']['u'] == 'tbd'):
            # create CKAN object                       
            CKAN = B2FIND.CKAN_CLIENT(options.iphost,options.auth)
            UP = B2FIND.UPLOADER(CKAN, OUT)
            logging.info('\n|- Uploading started : %s' % time.strftime("%Y-%m-%d %H:%M:%S"))
            logging.info(' |- Host:  \t%s' % CKAN.ip_host )

            # start the process uploading:
            if mode is 'multi':
                logging.info(' |- Joblist:  \t%s' % convert_list )
                process_upload(UP, parse_list_file('upload', convert_list or options.list, options.community, options.mdsubset), options)
            else:
                process_upload(UP,[[
                    options.community,
                    options.source,
                    options.mdprefix,
                    options.outdir + '/' + options.mdprefix,
                    options.mdsubset
                ]],options)

def process_harvest(HV, rlist):
    ## process_harvest (HARVESTER object, rlist) - function
    # Harvests per request.
    #
    # Parameters:
    # -----------
    # (object)  HARVESTER - object from the class HARVESTER
    # (list)    rlist - list of request lists 
    #
    # Return Values:
    # --------------
    # None
    ir=0
    for request in rlist:
        ir+=1
        harveststart = time.time()
        logging.info('   |# %-4d : %-30s \n\t|- %-10s |@ %-10s |' % (ir,request,'Started',time.strftime("%H:%M:%S")))
        results = HV.harvest(ir,request)
    
        if (results == -1):
            logging.error("Couldn't harvest from %s" % request)

        harvesttime=time.time()-harveststart
        #results['time'] = harvesttime

def process_map(MP, rlist):
    ## process_map (MAPPER object, rlist) - function
    # Maps per request.
    #
    # Parameters:
    # -----------
    # (object)  MAPPER - object from the class MAPPER
    # (list)    rlist - list of requests 
    #
    # Return Values:
    # --------------
    # None
    ir=0
    for request in rlist:
        ir+=1
        if (len (request) > 5):            
            mapfile='%s/%s-%s.xml' % ('mapfiles',request[0],request[5])
            target=request[5]
        else:
            mapfile='%s/%s/%s-%s.xml' % (os.getcwd(),'mapfiles',request[0],request[3])
            if not os.path.isfile(mapfile):
                mapfile='%s/%s/%s.xml' % (os.getcwd(),'mapfiles',request[3])
                if not os.path.isfile(mapfile):
                    logging.error('[ERROR] Mapfile %s does not exist !' % mapfile)
            target=None
        logging.info('   |# %-4d : %-10s\t%-20s : %-20s \n\t|- %-10s |@ %-10s |' % (ir,request[0],request[2:5],os.path.basename(mapfile),'Started',time.strftime("%H:%M:%S")))
        
        cstart = time.time()
        
        path=os.path.abspath('oaidata/'+request[0]+'-'+request[3]+'/'+request[4])
        results = MP.map(ir,request[0],request[3],path,target)

        ctime=time.time()-cstart
        results['time'] = ctime
        
def process_validate(MP, rlist):
    ## process_validate (MAPPER object, rlist) - function
    # Validates per request.
    #
    # Parameters:
    # -----------
    # (object)  VALIDATOR - object from the class MAPPER
    # (list)    rlist - list of request lists 
    #
    # Return Values:
    # --------------
    # None
    ir=0
    for request in rlist:
        ir+=1
        outfile='%s/%s/%s' % (request[2],request[4],'validation.stat')
        logging.info('   |# %-4d : %-10s\t%-20s\t--> %-30s \n\t|- %-10s |@ %-10s |' % (ir,request[0],request[3:5],outfile,'Started',time.strftime("%H:%M:%S")))
        cstart = time.time()
        
        results = MP.validate(request[0],request[3],os.path.abspath(request[2]+'/'+request[4]))

        ctime=time.time()-cstart
        results['time'] = ctime
        
        # save stats:
        MP.OUT.save_stats(request[0]+'-' + request[3],request[4],'v',results)
        
def process_oaiconvert(MP, rlist):

    ir=0
    for request in rlist:
        ir+=1
        logging.info('   |# %-4d : %-10s\t%-20s --> %-10s\n\t|- %-10s |@ %-10s |' % (ir,request[0],request[2:5],request[5],'Started',time.strftime("%H:%M:%S")))
        rcstart = time.time()
        
        results = MP.oaiconvert(request[0],request[3],os.path.abspath(request[2]+'/'+request[4]),request[5])

        rctime=time.time()-rcstart
        results['time'] = rctime
        
        # save stats:
        MP.OUT.save_stats(request[0]+'-' + request[3],request[4],'o',results)


def process_upload(UP, rlist, options):
    ##HEW-D-ec credentials,ec = None,None

    def print_extra(key,jsondata):
        for v in jsondata['extras']:
            if v['key'] == key:
                print ' Key : %s | Value : %s |' % (v['key'],v['value'])
 

    # create credentials and handle cleint if required
    if (options.handle_check):
          try:
              cred = PIDClientCredentials.load_from_JSON('credentials_11098')
          except Exception, err:
              logging.critical("[CRITICAL %s ] : Could not create credentials from credstore %s" % (err,options.handle_check))
              ##p.print_help()
              sys.exit(-1)
          else:
              logging.debug("Create EUDATHandleClient instance")
              client = EUDATHandleClient.instantiate_with_credentials(cred)

    CKAN = UP.CKAN
    last_community = ''
    package_list = dict()

    ir=0
    mdschemas={
        "ddi" : "ddi:codebook:2_5 http://www.ddialliance.org/Specification/DDI-Codebook/2.5/XMLSchema/codebook.xsd",
        "oai_ddi" : "http://www.icpsr.umich.edu/DDI/Version1-2-2.xsd",
        "marcxml" : "http://www.loc.gov/MARC21/slim http://www.loc.gov/standards",
        "iso" : "http://www.isotc211.org/2005/gmd/metadataEntity.xsd",        
        "oai_dc" : "http://www.openarchives.org/OAI/2.0/oai_dc.xsd",
        "oai_qdc" : "http://pandata.org/pmh/oai_qdc.xsd",
        "cmdi" : "http://catalog.clarin.eu/ds/ComponentRegistry/rest/registry/profiles/clarin.eu:cr1:p_1369752611610/xsd",
        "json" : "http://json-schema.org/latest/json-schema-core.html",
        "fgdc" : "No specification for fgdc available",
        "hdcp2" : "No specification for hdcp2 available"
        }
    for request in rlist:
        ir+=1
        log.info('   |# %-4d : %-10s\t%-20s \n\t|- %-10s |@ %-10s |' % (ir,request[0],request[2:5],'Started',time.strftime("%H:%M:%S")))
        community, source, dir = request[0:3]
        mdprefix = request[3]
        subset = request[4]
        dir = dir+'/'+subset
        
        results = {
            'count':0,
            'ecount':0,
            'tcount':0,
            'time':0
        }
        
        if not (CKAN.action('group_show',{"id":community}))['success'] :
          self.log.error("[ERROR]: Community (CKAN group) %s must exist!!!" % community)
          sys.exit()

        if not os.path.exists(dir):
            log.error('[ERROR] The directory "%s" does not exist! No files for uploading are found!\n(Maybe your upload list has old items?)' % (dir))
            
            # save stats:
            UP.OUT.save_stats(community+'-'+mdprefix,subset,'u',results)
            
            continue
        
        log.debug('    |   | %-4s | %-40s |\n    |%s|' % ('#','id',"-" * 53))
        
        if (last_community != community and options.ckan_check == 'True'):
            last_community = community
            UP.get_packages(community)
        
        uploadstart = time.time()
        
        # find all .json files in dir/json:
        files = filter(lambda x: x.endswith('.json'), os.listdir(dir+'/json'))
        
        results['tcount'] = len(files)
        
        scount = 0
        fcount = 0
        oldperc = 0
        for filename in files:
            ## counter and progress bar
            fcount+=1
            if (fcount<scount): continue
            perc=int(fcount*100/int(len(files)))
            bartags=perc/5
            if perc%10 == 0 and perc != oldperc :
                oldperc=perc
                log.info("\t[%-20s] %d / %d%%\r" % ('='*bartags, fcount, perc ))
                sys.stdout.flush()

            jsondata = dict()
            pathfname= dir+'/json/'+filename
            if ( os.path.getsize(pathfname) > 0 ):
                with open(pathfname, 'r') as f:
                    try:
                        jsondata=json.loads(f.read())
                    except:
                        log.error('    | [ERROR] Cannot load the json file %s' % dir+'/json/'+filename)
                        results['ecount'] += 1
                        continue
            else:
                results['ecount'] += 1
                continue

            # get dataset id (CKAN name) from filename (a uuid generated identifier):
            ds_id = os.path.splitext(filename)[0]
            
            log.info('    | u | %-4d | %-40s |' % (fcount,ds_id))

            # get OAI identifier from json data extra field 'oai_identifier':
            oai_id = jsondata['oai_identifier'][0]
            log.debug("        |-> identifier: %s\n" % (oai_id))
            
            ### CHECK JSON DATA for upload
            jsondata=UP.check(jsondata)

            ### ADD SOME EXTRA FIELDS TO JSON DATA:
            #  generate get record request for field MetaDataAccess:
            if (mdprefix == 'json'):
               reqpre = source + '/dataset/'
               mdaccess = reqpre + oai_id
            else:
               reqpre = source + '?verb=GetRecord&metadataPrefix=' + mdprefix
               mdaccess = reqpre + '&identifier=' + oai_id
            index1 = mdaccess

            # exceptions for some communities:
            if (community == 'clarin' and oai_id.startswith('mi_')):
                mdaccess = 'http://www.meertens.knaw.nl/oai/oai_server.php?verb=GetRecord&metadataPrefix=cmdi&identifier=http://hdl.handle.net/10744/' + oai_id
            elif (community == 'sdl'):
                mdaccess =reqpre+'&identifier=oai::record/'+oai_id

            ###HEW!!! if (field.split('.')[0] == 'extras'): # append extras field
            ###HEW!!!        self.add_unique_to_dict_list(newds['extras'], field.split('.')[1], value)

            ## Move all CKAN extra fields to the list jsondata['extras']
            
            jsondata['MetaDataAccess']=mdaccess

            jsondata=UP.json2ckan(jsondata)
            # determine checksum of json record and append
            try:
                checksum=hashlib.md5(unicode(json.dumps(jsondata))).hexdigest()
            except UnicodeEncodeError:
                log.error('        |-> [ERROR] Unicode encoding failed during md checksum determination')
                checksum=None
            else:
                jsondata['version'] = checksum
                
            # Set the tag ManagerVersion:
            jsondata['extras'].append({
                     "key" : "ManagerVersion",
                     "value" : ManagerVersion
                    })

            
            ### CHECK STATE OF DATASET IN CKAN AND HANDLE SERVER:
            # status of data set
            dsstatus="unknown"
     
            # check against handle server
            handlestatus="unknown"
            checksum2=None
            if (options.handle_check):
                try:
                    ##HEW-D pid = "11098/eudat-jmd_" + ds_id ##HEW?? 
                    pid = cred.get_prefix() + '/eudat-jmd_' + ds_id 
                    rec = client.retrieve_handle_record_json(pid)
                    checksum2 = client.get_value_from_handle(pid, "CHECKSUM",rec)
                    ManagerVersion2 = client.get_value_from_handle(pid, "JMDVERSION",rec)
                    B2findHost = client.get_value_from_handle(pid,"B2FINDHOST",rec)
                except Exception, err:
                    log.error("[CRITICAL : %s] in client.get_value_from_handle" % err )
                else:
                    log.debug("Got checksum2 %s, ManagerVersion2 %s and B2findHost %s from PID %s" % (checksum2,ManagerVersion2,B2findHost,pid))
                if (checksum2 == None):
                    log.debug("        |-> Can not access pid %s to get checksum" % pid)
                    handlestatus="new"
                elif ( checksum == checksum2) and ( ManagerVersion2 == ManagerVersion ) and ( B2findHost == options.iphost ) :
                    log.debug("        |-> checksum, ManagerVersion and B2FIND host of pid %s not changed" % (pid))
                    handlestatus="unchanged"
                else:
                    log.debug("        |-> checksum, ManagerVersion or B2FIND host of pid %s changed" % (pid))
                    handlestatus="changed"
                dsstatus=handlestatus

            # check against CKAN database
            ckanstatus = 'unknown'                  
            if (options.ckan_check == 'True'):
                ckanstatus=UP.check_dataset(ds_id,checksum)
                if (dsstatus == 'unknown'):
                    dsstatus = ckanstatus

            upload = 0
            # depending on status of handle upload record to B2FIND 
            log.debug('        |-> Dataset is [%s]' % (dsstatus))
            if ( dsstatus == "unchanged") : # no action required
                log.info('        |-> %s' % ('No upload required'))
            else:
                upload = UP.upload(ds_id,dsstatus,community,jsondata)
                if (upload == 1):
                    log.info('        |-> Creation of %s record succeed' % dsstatus )
                elif (upload == 2):
                    log.info('        |-> Update of %s record succeed' % dsstatus )
                    upload=1
                else:
                    log.error('        |-> Upload of %s record %s failed ' % (dsstatus, ds_id ))
                    log.debug('        |-> JSON data :\n\t %s ' % json.dumps(jsondata, indent=2))

            # update PID in handle server                           
            if (options.handle_check):
                if (handlestatus == "unchanged"):
                    log.info("        |-> No action required for %s" % pid)
                else:
                    if (upload >= 1): # new or changed record
                        ckands='http://b2find.eudat.eu/dataset/'+ds_id
                        if (handlestatus == "new"): # Create new PID
                            log.info("        |-> Create a new handle %s with checksum %s" % (pid,checksum))
                            try:
                                npid = client.register_handle(pid, ckands, checksum, None, True ) ## , additional_URLs=None, overwrite=False, **extratypes)
                            except (HandleAuthenticationError,HandleSyntaxError) as err :
                                log.critical("[CRITICAL : %s] in client.register_handle" % err )
                            except Exception, err:
                                log.critical("[CRITICAL : %s] in client.register_handle" % err )
                                sys.exit()
                            else:
                                log.debug(" New handle %s with checksum %s created" % (pid,checksum))
                        else: # PID changed => update URL and checksum
                            log.info("        |-> Update handle %s with changed checksum %s" % (pid,checksum))
                            try:
                                client.modify_handle_value(pid,URL=ckands) ##HEW-T !!! as long as URLs not all updated !!
                                client.modify_handle_value(pid,CHECKSUM=checksum)
                            except (HandleAuthenticationError,HandleNotFoundException,HandleSyntaxError) as err :
                                log.critical("[CRITICAL : %s] client.modify_handle_value %s" % (err,pid))
                            except Exception, err:
                                log.critical("[CRITICAL : %s]  client.modify_handle_value %s" % (err,pid))
                                sys.exit()
                            else:
                                log.debug(" Modified JMDVERSION, COMMUNITY or B2FINDHOST of handle %s " % pid)

                    try: # update PID entries in all cases (except handle status is 'unchanged'
                        client.modify_handle_value(pid, JMDVERSION=ManagerVersion)
                        client.modify_handle_value(pid, COMMUNITY=community)
                        client.modify_handle_value(pid, SUBSET=subset)
                        client.modify_handle_value(pid, B2FINDHOST=options.iphost)
                        client.modify_handle_value(pid, IS_METADATA=True)
                        client.modify_handle_value(pid, MD_SCHEMA=mdschemas[mdprefix])
                        client.modify_handle_value(pid, MD_STATUS='B2FIND_uploaded')
                    except (HandleAuthenticationError,HandleNotFoundException,HandleSyntaxError) as err :
                        log.critical("[CRITICAL : %s] in client.modify_handle_value of pid %s" % (err,pid))
                    except Exception, err:
                        log.critical("[CRITICAL : %s] in client.modify_handle_value of %s" % (err,pid))
                        sys.exit()
                    else:
                        log.debug(" Modified JMDVERSION, COMMUNITY or B2FINDHOST of handle %s " % pid)

            results['count'] +=  upload
            
        uploadtime=time.time()-uploadstart
        results['time'] = uploadtime
        log.info(
                '   \n\t|- %-10s |@ %-10s |\n\t| Provided | Uploaded | Failed |\n\t| %8d | %6d | %6d |' 
                % ( 'Finished',time.strftime("%H:%M:%S"),
                    results['tcount'],
                    fcount,
                    results['ecount']
                ))
        
        # save stats:
        UP.OUT.save_stats(community+'-'+mdprefix,subset,'u',results)

def process_delete(OUT, dir, options):
    print "###JM# Don't use this function. It is not up to date."
    return False

    # create CKAN object                       
    CKAN = B2FIND.CKAN_CLIENT(options.iphost,options.auth)
    UP = B2FIND.UPLOADER(CKAN, OUT)
    
    ##HEW-D-ec credentials,ec = None,None

    # create credentials
    try:
        cred = b2handle.clientcredentials.PIDClientCredentials.load_from_JSON('credentials_11098')
    except Exception, err:
        log.critical("[CRITICAL] %s Could not create credentials from credstore %s" % (err,options.handle_check))
        p.print_help()
        sys.exit(-1)
    else:
        log.debug("Create handle client instance to add uuid to handle server")

    for delete_file in glob.glob(dir+'/*.del'):
        community, mdprefix = os.path.splitext(os.path.basename(delete_file))[0].split('-')
        
        log.info('\n## Deleting datasets from community "%s" ##' % (community))
        
        # get packages from the group in CKAN:
        UP.get_packages(community)
        
        # open the delete file and loop over its lines:
        file_content = ''
        try:
            f = open(delete_file, 'r')
            file_content = f.read()
            f.close()
        except IOError as (errno, strerror):
            self.log.critical("Cannot read data from '{0}': {1}".format(delete_file, strerror))
            f.close
        else:
            # rename the file in a crash backup file:
            os.rename(delete_file,delete_file+'.crash-backup')
        
        results = {
            'count':0,
            'ecount':0,
            'tcount':0,
            'time':0
        }

        # use a try-except-finally environment to gurantee that no deleted metadata information will be lost:
        try:
            log.info('    |   | %-4s | %-50s | %-50s |\n    |%s|' % ('#','oai identifier','CKAN identifier',"-" * 116))
            
            deletestart = time.time()
     
            for line in file_content.split('\n'):
                # ignore empty lines:
                if not line:
                    continue
                   
                results['tcount'] += 1
                subset, identifier = line.split('\t')
         
                # dataset name uniquely generated from oai identifier
                uid = uuid.uuid5(uuid.NAMESPACE_DNS, identifier.encode('ascii','replace'))
                ds = str(uid)

                # output:
                log.info('    | d | %-4d | %-50s | %-50s |' % (results['tcount'],identifier,ds))

                ### CHECK STATUS OF DATASET IN CKAN AND PID:
                # status of data set
                dsstatus="unknown"
         
                # check against handle server
                handlestatus="unknown"
                ##HEW-D-ec???  pid = credentials.prefix + "/eudat-jmd_" + ds
                pid = "11098/eudat-jmd_" + ds_id
                checksum2 = client.get_value_from_handle(pid, "CHECKSUM")

                if (checksum2 == None):
                  log.debug("        |-> Can not access pid %s to get checksum" % (pid))
                  handlestatus="new"
                else:
                  log.debug("        |-> pid %s exists" % (pid))
                  handlestatus="exist"

                # check against CKAN database
                ckanstatus = 'unknown'                  
                ckanstatus=UP.check_dataset(ds,None)

                delete = 0
                # depending on handle status delete record from B2FIND
                if ( handlestatus == "new" and ckanstatus == "new") : # no action required
                    log.info('        |-> %s' % ('No deletion required'))
                else:
                    delete = UP.delete(ds,ckanstatus)
                    if (delete == 1):
                        log.info('        |-> %s' % ('Deletion was successful'))
                        results['count'] +=  1
                        
                        # delete handle (to keep the symmetry between handle and B2FIND server)
                        if (handlestatus == "exist"):
                           log.info("        |-> Delete handle %s with checksum %s" % (pid,checksum2))
                           try:
                               client.delete_handle(pid)
                           except GenericHandleError as err:
                               log.error('[ERROR] Unexpected Error: %s' % err)
                           except Exception, e:
                               log.error('[ERROR] Unexpected Error: %s' % e)

                        else:
                           log.info("        |-> No action (deletion) required for handle %s" % pid)
                    else:
                        log.info('        |-> %s' % ('Deletion failed'))
                        results['ecount'] += 1
        except Exception, e:
            log.error('[ERROR] Unexpected Error: %s' % e)
            log.error('You find the ids of the deleted metadata in "%s"' % (delete_file+'.crash-backup'))
            raise
        else:
            # all worked fine you can remove the crash-backup file:
            os.remove(delete_file+'.crash-backup')
            
        deletetime=time.time()-deletestart
        results['time'] = deletetime
        
        # save stats:
        OUT.save_stats(community+'-'+mdprefix,subset,'d',results)

def parse_list_file(process,filename,community=None,subset=None,mdprefix=None,target_mdschema=None):
    if(not os.path.isfile(filename)):
        log.critical('[CRITICAL] Can not access job list file %s ' % filename)
        exit()
    else:
        file = open(filename, 'r')
        lines=file.read().splitlines()
        file.close

    # processing loop over ingestion requests
    inside_comment = False
    reqlist = []

    logging.debug(' Arguments given to parse_list_file:\n\tcommunity:\t%s\n\tmdprefix:\t%s\n\tsubset:\t%s\n\ttarget_mdschema:\t%s' % (community,mdprefix,subset,target_mdschema))


    l = 0
    for request in lines:
    
        l += 1
        
        # recognize multi-lines-comments (starts with '<#' and ends with '>'):
        if (request.startswith('<#')):
            inside_comment = True
            continue
        if ((request.startswith('>') or request.endswith('>')) and (inside_comment == True)):
            inside_comment = False
            continue
        
        # ignore comments and empty lines
        if(request == '') or ( request.startswith('#')) or (inside_comment == True):
            continue
       
        # sort out lines that don't match given community
        if((community != None) and ( not request.startswith(community))):
            continue

        # sort out lines that don't match given mdprefix
        if (mdprefix != None):
            if ( not request.split()[3] == mdprefix) :
              continue
            
        # sort out lines that don't match given subset
        if (subset != None):
            if len(request.split()) < 5 :
               continue
            elif ( not request.split()[4] == subset ) and (not ( subset.endswith('*') and request.split()[4].startswith(subset.translate(None, '*')))) :
              continue

        if (target_mdschema != None):
            request+=' '+target_mdschema  

        reqlist.append(request.split())
        
    if len(reqlist) == 0:
        log.error(' No matching request found in %s' % filename)
        exit()
 
    return reqlist

def options_parser(modes):
    
    p = optparse.OptionParser(
        description = '''Description                                                              
===========                                                                           
 Management of metadata, comprising                                      
      - Harvesting of XML files from an OAI-PMH endpoint \n\t

              - Mapping : Convert XML to JSON and perform semantic mapping on metadata \n\t

\n              - Validation : Performe checks on the mapped JSON records and create coverage statistics\n\t
              - Uploading of JSON records as datasets to a metadata catalogue\n\t
''',
        formatter = optparse.TitledHelpFormatter(),
        prog = 'mdmanager.py',
        epilog='For any further information and documentation please look at the README.md file or send an email to widmann@dkrz.de.'
    )
        
    p.add_option('-v', '--verbose', action="count",
                        help="increase output verbosity (e.g., -vv is more than -v)", default=False)
    p.add_option('--mode', '-m', metavar='PROCESSINGMODE', help='\nThis specifies the processing mode. Supported modes are (h)arvesting, (m)apping, (v)alidating, and (u)ploading.')
    p.add_option('--community', '-c', help="community where data harvested from and uploaded to", default='', metavar='STRING')
    p.add_option('--fromdate', help="Filter harvested files by date (Format: YYYY-MM-DD).", default=None, metavar='DATE')
    p.add_option('--outdir', '-d', help="The relative root dir in which all harvested files will be saved. The converting and the uploading processes work with the files from this dir. (default is 'oaidata')",default='oaidata', metavar='PATH')
    
         
    group_multi = optparse.OptionGroup(p, "Multi Mode Options",
        "Use these options if you want to ingest from a list in a file.")
    group_multi.add_option('--list', '-l', help="list of OAI harvest sources (default is ./harvest_list)", default='harvest_list',metavar='FILE')
         
    group_single = optparse.OptionGroup(p, "Single Mode Options",
        "Use these options if you want to ingest from only ONE source.")
    group_single.add_option('--source', '-s', help="A URL to .xml files which you want to harvest",default=None,metavar='PATH')
    group_single.add_option('--verb', help="Verbs or requests defined in OAI-PMH, can be ListRecords (default) or ListIdentifers here",default='ListRecords', metavar='STRING')
    group_single.add_option('--mdsubset', help="Subset of harvested meta data",default=None, metavar='STRING')
    group_single.add_option('--mdprefix', help="Prefix of harvested meta data",default=None, metavar='STRING')
    group_single.add_option('--target_mdschema', help="Meta data schema of the target",default=None, metavar='STRING')
    
    group_upload = optparse.OptionGroup(p, "Upload Options",
        "These options will be required to upload an dataset to a CKAN database.")
    group_upload.add_option('--iphost', '-i', help="IP adress of B2FIND portal (CKAN instance)", metavar='IP')
    group_upload.add_option('--auth', help="Authentification for CKAN APIs (API key, by default taken from file $HOME/.netrc)",metavar='STRING')
    
    p.add_option_group(group_multi)
    p.add_option_group(group_single)
    p.add_option_group(group_upload)
    
    return p

def pstat_init (p,modes,mode,source,iphost):
    if (mode):
        if not(mode in modes):
           print("[ERROR] Mode " + mode + " is not supported")
           sys.exit(-1)
    else: # all processes (default)
        mode = 'h-u'
 
    # initialize status, count and timing of processes
    plist=['a','h','m','v','u','c','o','d']
    pstat = {
        'status' : {},
        'text' : {},
        'short' : {},
     }

    for proc in plist :
        pstat['status'][proc]='no'
        if ( proc in mode):
            pstat['status'][proc]='tbd'
        if (len(mode) == 3) and ( mode[1] == '-'): # multiple mode
            ind=plist.index(mode[0])
            last=plist.index(mode[2])
            while ( ind <= last ):
                pstat['status'][plist[ind]]='tbd'
                ind+=1
        
    if ( mode == 'h-u'):
        pstat['status']['a']='tbd'
        
    if source:
       stext='provider '+source
    else:
       stext='a list of MD providers'
       
    pstat['text']['h']='Harvest community XML files from ' + stext 
    pstat['text']['c']='Convert community XML to B2FIND JSON and do semantic mapping'  
    pstat['text']['m']='Map community XML to B2FIND JSON and do semantic mapping'  
    pstat['text']['v']='Validate JSON records against B2FIND schema'  
    pstat['text']['o']='OAI-Convert B2FIND JSON to B2FIND XML'  
    pstat['text']['u']='Upload JSON records as datasets into B2FIND %s' % iphost
    pstat['text']['d']='Delete B2FIND datasets from %s' % iphost
    
    pstat['short']['h-u']='TotalIngestion'
    pstat['short']['h']='Harvesting'
    pstat['short']['c']='Converting'
    pstat['short']['m']='Mapping'
    pstat['short']['v']='Validating'
    pstat['short']['o']='OAIconverting'
    pstat['short']['u']='Uploading'
    pstat['short']['d']='Deletion'
    
    return (mode, pstat)

def exit_program (OUT, message=''):
    # stop the total time:
    OUT.save_stats('#Start','subset','TotalTime',time.time()-TimeStart)

    # print results with OUT.HTML_print_end() in a .html file:
    OUT.HTML_print_end()

    if (OUT.options.verbose != False):
      ##HEW? try:
      ##HEW  os.system('firefox '+OUT.jobdir+'/overview.html')
      ##HEW except Exception, err:
      ##HEW   print("[ERROR] %s : Can not open result html in browser" % err)
      ##HEWos.system('cat '+OUT.jobdir+'/overview.html')
      log.debug('For more info open HTML file %s' % OUT.jobdir+'/overview.html')

if __name__ == "__main__":
    main()
