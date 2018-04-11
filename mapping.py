"""mapping.py - class for B2FIND mapping : 
  - Mapper    maps harvested nad specific MD records onto B2FIND schema

Copyright (c) 2013 Heinrich Widmann (DKRZ)
Further contributions by
     2017 Claudia Martens
     2014 Mikael Karlsson
     2013 John Mrziglod (DKRZ)

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

# needed for MAPPER :
import codecs
import xml.etree.ElementTree as ET
import simplejson as json
import io
from pyparsing import *
import Levenshtein as lvs
import iso639
from collections import OrderedDict, Iterable, Counter

PY2 = sys.version_info[0] == 2
if PY2:
    from urllib2 import urlopen
    from urllib2 import HTTPError,URLError
else:
    from urllib.request import urlopen
    from urllib.error import HTTPError,URLError

class Mapper(object):
    """
    ### MAPPER - class
    # Parameters:
    # -----------
    # Public Methods:
    # ---------------
    # map(request)  - maps records according to request on B21FIND schema
    #     using mapfiles in md-mapping and stores resulting files in subdirectory '../json'
    #
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
            disctab = []
            discipl_file =  '%s/mapfiles/b2find_disciplines.json' % (os.getcwd())
            with open(discipl_file) as f:
                disctab = json.load(f)['disciplines']
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
            if isinstance(el, Iterable) and not isinstance(el, str):
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
            seplist=[re.split(r"[;&\xe2]",i) for i in invalue]
            swlist=[re.findall(r"[\w']+",i) for i in invalue]
            inlist=swlist ## +seplist
            inlist=[item for sublist in inlist for item in sublist] ##???
        for indisc in inlist :
            self.logger.info(' Next input discipline value %s of type %s' % (indisc,type(indisc)))
            if PY2:
                indisc=indisc.encode('utf8').replace('\n',' ').replace('\r',' ').strip().title()
            else:
                indisc=indisc.replace('\n',' ').replace('\r',' ').strip().title()
            maxr=0.0
            maxdisc=''
            for line in disctab :
                line=re.split(r'#', line) 
                try:
                    if len(line) < 3:
                        self.logger.critical('Missing base element in dicipline array %s' % line)
                        sys.exit(-2)
                    else:
                        disc=line[2].strip()
                        r=lvs.ratio(indisc,disc)
                except Exception as e :
                    self.logger.error('%s : %s of type %s can not compared to %s of type %s' % (e,indisc,type(indisc),disc,type(disc)))
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
        Invalue is expected as list (if is not, it is splitted). 
        Loop over invalue and for each elem : 
           - If pattern is None truncate characters specified by nfield (e.g. ':4' first 4 char, '-2:' last 2 char, ...)
           - else if pattern is in invalue, split according to pattern and return field nfield (if 0 return the first found pattern),
           - else return invalue.

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
                logging.error("%s in cut() with invalue %s" % (e,invalue))

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

        # community-mdschema root path
        cmpath='%s/%s-%s' % (self.base_outdir,community,mdprefix)
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
                                elif facet in ['Format']: 
                                    jsondata[facet] = self.uniq(jsondata[facet])
                                elif facet == 'PublicationYear':
                                    publdate=self.date2UTC(jsondata[facet])
                                    if publdate:
                                        jsondata[facet] = self.cut([publdate],'\d\d\d\d',0)
                                elif facet == 'fulltext':
                                    encoding='utf-8'
                                    jsondata[facet] = ' '.join([x.strip(' \n\t') for x in filter(None,jsondata[facet].strip())]).split().encode(encoding)[:32000]
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
                            results['ecount'] += 1
                            continue
                        else:
                            self.logger.debug(' Succesfully written to json file %s' % outpath+'/'+filename)

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
        cmpath='%s/%s-%s' % (self.base_outdir,community,mdprefix)
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
                    counter=Counter(totstats[facet]['vstat'])
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
