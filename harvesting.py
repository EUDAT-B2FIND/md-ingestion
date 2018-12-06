"""harvesting.py - class for B2FIND harvesting : 
  - Harvester    harvests from a data provider server

Copyright (c) 2014 Heinrich Widmann (DKRZ)

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
import traceback
import re

__author__ = "Heinrich Widmann"

PY2 = sys.version_info[0] == 2

# needed for class Harvester:
from sickle import Sickle
from sickle.oaiexceptions import NoRecordsMatch,CannotDisseminateFormat
from owslib.csw import CatalogueServiceWeb
from owslib.namespaces import Namespaces
from SPARQLWrapper import SPARQLWrapper, JSON
import requests
from requests.exceptions import ConnectionError
import uuid
import lxml.etree as etree
import xml.etree.ElementTree as ET
import simplejson as json
from itertools import tee 
import collections
if PY2:
    from urllib2 import urlopen, Request
    from urllib2 import HTTPError,URLError
else:
    from urllib.request import urlopen, Request
    from urllib.error import HTTPError,URLError

class Harvester(object):
    
    """
    ### Harvester - class
    # Provides methods to harvest metadata from external data providers
    #
    # create HARVESTER object                       
    HV = Harvester(OUT object, outdir,fromdate)
    """
    
    def __init__ (self, OUT, pstat, base_outdir, fromdate):
        self.logger = logging.getLogger('root')
        self.pstat = pstat
        self.OUT = OUT
        self.base_outdir = base_outdir
        self.fromdate = fromdate
        
    
    def harvest(self, request):
        ## harvest (Harvester object, request = [community, source, verb, mdprefix, mdsubset])
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
                    chunk = harvestreq(**{'action':haction,'offset':choffset,'chunklen':chunklen,'key':None})
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
            maxrecords=20
            try:
                src = CatalogueServiceWeb(req['url'])
                NS = Namespaces()
                namespaces=NS.get_namespaces()
                if req['mdprefix'] == 'iso19139' or req['mdprefix'] == 'own' : 
                    nsp = namespaces['gmd']
                else :
                    nsp = namespaces['csw']

                harvestreq=getattr(src,'getrecords2')
                chunk = harvestreq(**{'esn':'full','startposition':choffset,'maxrecords':maxrecords,'outputschema':nsp})
                chunklist=list(src.records.items())
                while(len(chunklist) > 0) :
                    records.extend(chunklist)
                    choffset+=maxrecords
                    chunk = harvestreq(**{'esn':'full','startposition':choffset,'maxrecords':maxrecords,'outputschema':nsp})
                    chunklist=list(src.records.items())
                    self.logger.debug(" Got next %s records [%d,%d] from chunk " % (nsp,choffset,choffset+chunklen))
            except (HTTPError,ConnectionError) as err:
                self.logger.critical("%s during connecting to %s\n" % (err,req['url']))
                return -1
            except (ImportError,CannotDisseminateFormat,Exception) as err:
                self.logger.error("%s : During harvest request %s\n" % (err,req))
                ##return -1

        # Restful API POST request
        elif req["lverb"].startswith('POST'):
            outtypedir='hjson'
            outtypeext='json'
            startposition=0
            maxrecords=1000
            try:
                url=req['url']
                data={ "text" : "mnhn", "searchTextInMetadata" : True, "searchTextInAdditionalData" : True, "page" : 1, "size" : 1000, "highlight" : { "preTag" : "<b>", "postTag" : "</b>", "fragmentSize" : 500, "fragmentsCount" : 1 } }
                headers = {'content-type': 'application/json'}
                response = requests.post(url, data=json.dumps(data), headers=headers, verify=False )##, stream=True ) ##HEW-D auth=('myusername', 'mybasicpass'))
                records=response.json()['result']
            except (HTTPError,ConnectionError) as err:
                self.logger.critical("%s during connecting to %s\n" % (err,req['url']))
                return -1
            except (ImportError,CannotDisseminateFormat,Exception) as err:
                self.logger.critical("%s during harvest request \n" % err)
                return -1
            
        # CKAN-API request
        elif req["lverb"].startswith('ckan_api'):
            outtypedir='hjson'
            outtypeext='json'
            startposition=0
            maxrecords=1000
            try:
                url=req['url']
                action_url = '{url}/{action}'.format(url=url,action='package_list')
                data_string=json.dumps({}).encode('utf8')
                request = Request(action_url,data_string)
                self.logger.debug('request %s' % request)            
                response = urlopen(request)
                self.logger.debug('response %s' % response)            
                records= json.loads(response.read())['result']
                self.logger.debug('records %s' % records)
            except (HTTPError,ConnectionError) as err:
                self.logger.critical("%s during connecting to %s\n" % (err,req['url']))
                return -1
            except (ImportError,CannotDisseminateFormat,Exception) as err:
                self.logger.critical("%s during harvest request \n" % err)
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
                statement='''prefix cpmeta: <http://meta.icos-cp.eu/ontologies/cpmeta/>
prefix prov: <http://www.w3.org/ns/prov#>
prefix dcterms: <http://purl.org/dc/terms/>
select
?url ?doi
(concat("11676/", substr(str(?url), strlen(str(?url)) - 23)) AS ?pid)
(if(bound(?theTitle), ?theTitle, ?fileName) as ?title)
(if(bound(?theDescription), ?theDescription, ?spec) as ?description)
?submissionTime ?tempCoverageFrom ?tempCoverageTo
?dataLevel ?format ?sha256sum ?latitude ?longitude ?spatialCoverage
where{
   ?url cpmeta:wasSubmittedBy [
     prov:endedAtTime ?submissionTime ;
     prov:wasAssociatedWith [a ?submitterClass]
    ] .
 ?url cpmeta:hasObjectSpec [rdfs:label ?spec ; cpmeta:hasDataLevel ?dataLevel; cpmeta:hasFormat/rdfs:label ?format ] .
  FILTER(?submitterClass = cpmeta:ThematicCenter || ?submitterClass = cpmeta:ES || ?dataLevel = "3"^^xsd:integer)
   ?url cpmeta:hasName ?fileName .
   ?url cpmeta:hasSha256sum ?sha256sum .
   OPTIONAL{?url dcterms:title ?theTitle ; dcterms:description ?theDescription}
   OPTIONAL{?coll dcterms:hasPart ?url . ?coll cpmeta:hasDoi ?doi }
   {
     {
         ?url cpmeta:wasAcquiredBy ?acq .
         ?acq prov:startedAtTime ?tempCoverageFrom; prov:endedAtTime ?tempCoverageTo; prov:wasAssociatedWith ?station .
         {
           {
                ?station cpmeta:hasLatitude ?latitude .
                ?station cpmeta:hasLongitude ?longitude .
           }UNION{
                ?url cpmeta:hasSpatialCoverage/cpmeta:asGeoJSON ?spatialCoverage .
           }
         }
     }UNION{
         ?url cpmeta:hasStartTime ?tempCoverageFrom .
         ?url cpmeta:hasEndTime ?tempCoverageTo .
         ?url cpmeta:hasSpatialCoverage/cpmeta:asGeoJSON ?spatialCoverage .
     }
   }
}
limit 10'''



                '''
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

        self.logger.debug(" Harvest method used %s" % req["lverb"])
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
                elif(record):
                    oai_id = record[0]
                else:
                    self.logger.critical('Record %s has no attrribute identifier %s' % record) 
            
            elif req["lverb"] == 'ListIdentifiers' : ## OAI-PMH harvesting of XML records
                if (record.deleted):
                    stats['totdcount'] += 1
                    delete_flag=True
                    ##HEW-D continue
                else:
                    oai_id = record.identifier
                    try:
                        record = sickle.GetRecord(**{'metadataPrefix':req['mdprefix'],'identifier':record.identifier})
                    except (CannotDisseminateFormat,Exception) as err:
                        self.logger.error('%s during GetRecord of %s' % (err,record.identifier))
                        stats['ecount'] += 1
                        continue
            elif req["lverb"] == 'ListRecords' :
                if (record.header.deleted):
                    stats['totdcount'] += 1
                    continue
                else:
                    oai_id = record.header.identifier
            elif req["lverb"].startswith('Sparql'):
                if 'fileName' in record:
                    oai_id=record['fileName']['value']
                elif 'title' in record:
                    oai_id=record['title']['value']

            elif req["lverb"].startswith('POST'):
                if 'depositIdentifier' in record:
                    oai_id=record['depositIdentifier']

            elif req["lverb"].startswith('ckan_api'):
                try:
                    oai_id=record
                    ##HEW-D action_url = '{url}/{action}?id={record}'.format(url=url,action='package_show',record=record)
                    action_url = '{url}/{action}'.format(url=url,action='package_show')
                    self.logger.debug('action_url %s' % action_url)            
                    self.logger.debug('data_string %s' % data_string)
                    data_string=json.dumps({"id": record }).encode('utf8')
                    request = Request(action_url,data_string)
                    self.logger.debug('request %s' % request)            
                    response = urlopen(request)
                    self.logger.debug('response %s' % response)            
                    record= json.loads(response.read())['result']
                    self.logger.debug('records %s' % records)
                except (HTTPError,ConnectionError) as err:
                    self.logger.critical("%s during connecting to %s\n" % (err,req['url']))
                    return -1
                except (ImportError,CannotDisseminateFormat,Exception) as err:
                    self.logger.critical("%s during harvest request \n" % err)
                    return -1

            # generate a uniquely identifier and a filename for this dataset:
            uid = str(uuid.uuid5(uuid.NAMESPACE_DNS, oai_id))
            outfile = '%s/%s/%s.%s' % (subsetdir,outtypedir,os.path.basename(uid),outtypeext)

            if delete_flag : # record marked as deleted on provider site 
                jsonfile = '%s/%s/%s.%s' % (subsetdir,'json',os.path.basename(uid),'json')
                # remove xml and json file:
                os.remove(xmlfile)
                os.remove(jsonfile)
                delete_ids.append(uid)

            # write record on disc
            try:
                self.logger.debug('    | h | %-4d | %-45s | %-45s |' % (stats['count']+1,oai_id,uid))
                self.logger.debug('Try to write the harvested JSON record to %s' % outfile)
     
                if outtypeext == 'xml':   # get and write the XML content:
                    if req["lverb"] == 'csw':
                        metadata = etree.fromstring(record[1].xml)
                    elif hasattr(record,'raw'):
                        metadata = etree.fromstring(record.raw)
                    elif hasattr(record,'xml'):
                        metadata = etree.fromstring(record.xml)

                    if (metadata is not None):
                        try:
                            metadata = etree.tostring(metadata, pretty_print = True).decode('utf-8')
                        except (Exception,UnicodeEncodeError) as e:
                            self.logger.critical('%s : Metadata: %s ...' % (e,metadata[:20]))
                        ##if PY2 :
                        ##    try:
                        ##        metadata = metadata.encode('utf-8')
                        ##    except (Exception,UnicodeEncodeError) as e :
                        ##        self.logger.debug('%s : Metadata : %s ...' % (e,metadata[20]))

                        try:
                            f = open(outfile, 'w')
                            f.write(metadata)
                            f.close
                        except (Exception,IOError) as err :
                            self.logger.critical("%s : Cannot write metadata in xml file %s" % (err,outfile))
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
            with open(delete_file, 'a+') as file:
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
