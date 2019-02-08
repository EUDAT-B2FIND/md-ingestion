# -*- coding: utf-8 -*-
"""validating.py - class for B2FIND validation : 
  - Validator    validates mapped JSON records 

Copyright (c) 2018 Heinrich Widmann (DKRZ)
Further contributions by

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

# needed for VALIDATOR :
import simplejson as json
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

class Validator(object):
    """
    ### Validator - class
    # Parameters:
    # -----------
    # Public Methods:
    # ---------------
    # vaildate(request)  - validates records according to B21FIND schema
    #     and generates coverage and check statistics
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
        This class represents the closed vocabulary used for the mapping of B2FIND discipline mapping
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
        This class represents the closed vocabulary used for the mapping of B2FIND spatial coverage to coordinates
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
            if str(err.reason).startswith('[SSL: CERTIFICATE_VERIFY_FAILED]') :
                return Warning
            else :
                return False
        else:
            return True
 

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

    def check_spatial(self,invalue,geotab):
        """
        Check spatial coverage and map to representiable form
        Copyright (C) 2018 Heinrich Widmann
        Licensed under AGPLv3.
        """

        self.logger.debug('invalue %s' % (invalue,))

        if not any(invalue) :
            self.logger.critical('Coordinate list has only None entries : %s' % (invalue,))
        desc=''
        ## check coordinates
        if len(invalue) > 1 :
            for lat in [invalue[1],invalue[3]]:
                if float(lat) < -90. or float(lat) > 90. :
                    self.logger.critical('Latitude %s is not in range [-90,90]' % lat)
            for lon in [invalue[2],invalue[4]]:
                if float(lon) < 0. or float(lon) > 360. :
                    self.logger.warning('Longitude %s is not in range [0,360]' % lon)
                    if float(lon) < -180. or float(lon) > 180 :
                        self.logger.critical('Longitude %s is not in range [-180,180] nor in [0,360]' % lon)

            if invalue[1]==invalue[3] and invalue[2]==invalue[4] :
                self.logger.info('[%s,%s] seems to be a point' % (invalue[1],invalue[2]))
                if float(invalue[1]) > 0 : # northern latitude
                    desc+='(%-2.0fN,' % float(invalue[1])
                else : # southern lat
                    desc+='(%-2.0fS,' % (float(invalue[1]) * -1.0)
                if float(invalue[2]) >= 0 : # eastern longitude
                    desc+='%-2.0fE)' % float(invalue[2]) ## (float(invalue[2]) -180.)
                else : # western longitude
                    desc+='%-2.0fW)' % (float(invalue[2]) * -1.0)
            else:
                self.logger.info('[%s,%s,%s,%s] seems to be a box' % (invalue[1],invalue[2],invalue[3],invalue[4]))
                if float(invalue[1]) > 0 : # northern min latitude
                    desc+='(%-2.0fN-' % float(invalue[1])
                else : # southern min lat
                    desc+='(%-2.0fS-' % (float(invalue[1]) * -1.0)
                if float(invalue[3]) > 0 : # northern max latitude
                    desc+='%-2.0fN,' % float(invalue[3])
                else :  # southern max lat
                    desc+='%-2.0fS,' % (float(invalue[3]) * -1.0)
                if float(invalue[2]) >= 0 : # eastern min longitude
                    desc+='%-2.0fE-' % float(invalue[2])
                else : # western min longitude
                    desc+='%-2.0fW-' % (float(invalue[2]) * -1.0)
                if float(invalue[4]) > 0 : # eastern max longitude
                    desc+='%-2.0fE)' % float(invalue[4])
                else : # western max longitude
                    desc+='%-2.0fW)' % (float(invalue[4]) * -1.0)              

        self.logger.info('Spatial description %s' % desc)
        return (desc,invalue[1],invalue[2],invalue[3],invalue[4])
 
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
            self.logger.debug('\t\t Next input discipline value %s of type %s' % (indisc,type(indisc)))
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
                if r > maxr :
                    maxdisc=disc
                    maxr=r
                    self.logger.debug('--- %s \n|%s|%s| %f | %f' % (line,indisc,disc,r,maxr))
                    rethier=line
            if maxr == 1 and indisc == maxdisc :
                self.logger.info('  | Perfect match of >%s< : nothing to do, DiscHier %s' % (indisc,line))
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
            return (';'.join(retval),rethier)
        else:
            return ('Not stated',list()) 

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
    
    def keynormalize(self,iterable):
        """normalize all keys in iterable"""
        ##for k, v in iterable.items():
        ##    print(k, v)

        if type(iterable) is dict:
            for key in iterable.keys():
                for namesp in ['aip.dc.','aip.meta.']:
                    if key.startswith(namesp):
                        newKey = key[len(namesp):]
                        iterable[newKey] = iterable.pop(key)
        return iterable

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
            obj=self.keynormalize(obj)
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
    

    def evalxpath(self, obj, expr, ns):
        # returns list of selected entries from xml obj using xpath expr
        flist=re.split(r'[\(\),]',expr.strip())
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
                if self.map_discipl(value,self.cv_disciplines().discipl_list)[0] is None :
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
        mdsubset=request[4] if (len(request)>4 and request[4] ) else ''

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
            self.logger.warning('\t |- Check next subdirectory %s for required processing (mdsubset is %s)' % (subdir,mdsubset))
            if self.fromdate :
                datematch = re.search(mdsubset+r'_f(\d{4}-\d{2}-\d{2})_\d+$',subdir) ##, subdir[:-2])
                if datematch :
                    subdirdate = datetime.datetime.strptime(datematch.group(1), '%Y-%m-%d').date()
                    fromdate = datetime.datetime.strptime(self.fromdate, '%Y-%m-%d').date()
                    if (fromdate > subdirdate) :
                        self.logger.warning('\t |- Subdirectory %s has timestamp older than fromdate %s - no processing required' % (subdir,self.fromdate))
                        continue
                    else :
                        self.logger.warning('\t |- Subdirectory %s with timestamp newer than fromdate %s is processed' % (subdir,self.fromdate))
                else :
                    continue
            elif not ( mdsubset == subdir or re.search(re.escape(mdsubset)+r'_\d+$', subdir)) :               
                self.logger.error('\t |- Subdirectory %s does not match %s[_NN] - no processing required' % (subdir,mdsubset))
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

            outfile='%s/%s/%s' % (cmpath,subdir,'validation.stat')
            print('\t|- Validationfile\t--> %s' % outfile)

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
                        printstats+="\n |-> {!s:<16s} <-- {!s:<20s}\n  |-- {:>5d} | {:>4.0f} | {:>5d} | {:>4.0f}\n".format(key,totstats[facet]['xpath'],totstats[facet]['mapped'],totstats[facet]['mapped']*100/float(fcount),totstats[facet]['valid'],totstats[facet]['valid']*100/float(fcount))
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
                                    printstats+="      |- {:<5d} : {!s:<30s}{!s:<5s} |\n".format(tuple[1],ucvalue[:80],contt)
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
                ##HEW-T print('JJJJJJJJ %s' % jsondata['oai_identifier'])
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
