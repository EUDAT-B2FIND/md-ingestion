# -*- coding: utf-8 -*-
"""converting.py - class for B2FIND converting : 
  - Converter    maps harvested and specific MD records onto B2FIND schema

Copyright (c) 2013 Heinrich Widmann (DKRZ)
Further contributions by
     2013 John Mrziglod (DKRZ)
     2014 Mikael Karlsson (CSC)
     2017 Claudia Martens (DKRZ)

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""

# from future


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
from collections import OrderedDict, Iterable
from urllib.request import urlopen
from urllib.error import HTTPError,URLError

class Converter(object):
    """
    ### CONVERTER - class
    # Parameters:
    # -----------
    # Public Methods:
    # ---------------
    # convert(request)  - converts records according to request on target schema (e.g. CERA) XMLs
    #     using templates and stores resulting files in subdirectory ' ...-<targetschema>/xml'
    #
    """

    def __init__ (self, OUT, base_outdir,fromdate):
        ##HEW-D logging = logging.getLogger()
        self.base_outdir = base_outdir
        self.OUT = OUT
        self.fromdate = fromdate
        self.logger = OUT.logger
        # Read in B2FIND metadata schema and fields
        b2fschemafile =  '%s/mapfiles/b2find_schema.json' % (os.getcwd())
        with open(b2fschemafile, 'r') as f:
            self.b2findfields=json.loads(f.read(), object_pairs_hook=OrderedDict)
       # Read in DataCite metadata schema and fields
        dataciteschemafile =  '%s/mapfiles/datacite_schema.json' % (os.getcwd())
        with open(dataciteschemafile, 'r') as f:
            self.datacitefields=json.loads(f.read(), object_pairs_hook=OrderedDict)

    def json2xml(self,json_obj, line_padding, mdftag, mapdict): ###="b2findfields"):

        result_list = list()
        json_obj_type = type(json_obj)


        if json_obj_type is list:
            for sub_elem in json_obj:
                result_list.append(self.json2xml(sub_elem, line_padding, mdftag, mapdict))

            return "\n".join(result_list)

        if json_obj_type is dict:
            for tag_name in json_obj:
                sub_obj = json_obj[tag_name]
                if tag_name in mapdict or ('extras' in tag_name and tag_name['extras'] in mapdict): 
                    tag_name=mapdict[tag_name]
                    ##HEW-T 
                    print('TTTTT tag_name[name]',tag_name['name'])
                    if tag_name['name'] in ['creators'] :
                        sub_obj = sub_obj.split(';')
                        ##HEW-T
                    print('- sub_obj %s' % sub_obj)
                    key=tag_name
                    print('-- key-n >%s<' % key['name'])
                    result_list.append("%s<%s>" % (line_padding, key['name']))
                    if type(sub_obj) is list:
                        for nv in sub_obj:
                            if tag_name == 'tags' or tag_name == 'KEY_CONNECT.GENERAL_KEY':
                                result_list.append("%s%s" % (line_padding, nv["name"].strip()))
                            if tag_name['name'] in ['subjects','creators'] :
                                if tag_name['name'] == 'subjects' :
                                    nv=nv['display_name']
                                print('- nv %s' % nv)
                                result_list.append("%s%s<%s>%s</%s>" % (line_padding,line_padding,key['name'][:-1],nv,key['name'][:-1]))

                            elif tag_name['name'] == 'DOI' :
                                print('SSSS %s' % sub_obj)
                                for nv in sub_obj :
                                    if nv['key']=='DOI':
                                        result_list.append("%s%s<%s>%s</%s>" % (line_padding,line_padding,key['name'],nv['value'],key['name'][:-1]))
                        ##result_list.append(self.json2xml(sub_obj['name'], "\t" + line_padding, mdftag, mapdict))
                    else:
                        result_list.append(self.json2xml(sub_obj, "\t" + line_padding, mdftag, mapdict))

                    result_list.append("%s</%s>" % (line_padding, key['name']))
                else:
                        self.logger.debug ('[WARNING] : Field %s can not mapped to B2FIND schema' % tag_name)
                        continue
            
            return "\n".join(result_list)

        return "%s%s" % (line_padding, json_obj)

    def convert(self,request): 
    # Converts JSON files to XML files formatted in target format, e.g. 'CERA' (exp_ and ds2_ files)
    
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
            self.logger.error('[ERROR] Can not access input data path %s' % (inpath+'/json') )
            return results
        elif not os.path.exists(outpath) :
            self.logger.warning('[ERROR] Create not existing output data path %s' % (outpath) )
            os.makedirs(outpath)
    
        # run oai-converting
        # find all .json files in inpath/json:
        files = list([x for x in os.listdir(inpath+'/json') if x.endswith('.json')])
        results['tcount'] = len(files)

        ##oaiset=path.split(target_mdschema)[0].split('_')[0].strip('/')
        ##oaiset=os.path.basename(path)
        ## outpath=path.split(community)[0]+'/b2find-oai_b2find/'+community+'/'+mdprefix +'/'+path.split(mdprefix)[1].split('_')[0]+'/xml'
        ##HEW-D outpath=path.split(community)[0]+'b2find-oai_b2find/'+community+'/'+mdprefix +'/xml'

        self.logger.debug(' %s Converting of files in %s' % (time.strftime("%H:%M:%S"),inpath))
        self.logger.debug('    | %-4s | %-40s | %-40s |\n   |%s|' % ('#','infile','outfile',"-" * 53))

        fcount = 0
        oldperc = 0
        start = time.time()

        #HEW-D # Read in B2FIND metadata schema and fields
        #HEW-D schemafile =  '%s/mapfiles/b2find_schema.json' % (os.getcwd())
        #HEW-D with open(schemafile, 'r') as f:
        #HEW-D     b2findfields=json.loads(f.read())

        for filename in files:
            ## counter and progress bar
            fcount+=1
            self.logger.debug('Next file to convert : %s' % inpath+'/json/'+filename)
            perc=int(fcount*100/int(len(files)))
            bartags=int(perc/5)
            if perc%10 == 0 and perc != oldperc :
                oldperc=perc
                print ("\r\t[%-20s] %d / %d%% in %d sec" % ('='*bartags, fcount, perc, time.time()-start ))
                sys.stdout.flush()

            createdate = str(datetime.datetime.utcnow())
            jsondata = dict()
            self.logger.debug(' |- %s     INFO  JSON2XML - Processing: %s/%s' % (time.strftime("%H:%M:%S"),os.path.basename(inpath),filename))

            if ( os.path.getsize(inpath+'/json/'+filename) > 0 ):
                self.logger.debug('Read json file %s' % inpath+'/json/'+filename)
                with open(inpath+'/json/'+filename, 'r') as f:
                    try:
                        jsondata=json.loads(f.read())
                    except:
                        self.logger.error('    | [ERROR] Can not access json file %s' % inpath+'/json/'+filename)
                        results['ecount'] += 1
                        continue
            else:
                self.logger.critical('Can not access json file %s' % inpath+'/json/'+filename)
                results['ecount'] += 1
                continue
            
            ### oai-convert !!
            if target_mdschema == 'cera':
                if 'oai_identifier' in jsondata :
                    identifier=jsondata['oai_identifier'][0]
                else:
                    identifier=os.path.splitext(filename)[0]
                convertfile='%s/mapfiles/%s%s.%s' % (os.getcwd(),'json2',target_mdschema,'json')
                with open(convertfile, 'r') as f:
                    try:
                        mapdict=json.loads(f.read())
                    except:
                        self.logger.error('    | [ERROR] Cannot load the convert file %s' % convertfile)
                        sys.exit()

                    for filetype in ['ds2','exp','dc']:
                        outfile=outpath+'/'+filetype+'_'+community+'_'+identifier+'.xml'             
	                ### load xml template
                        templatefile='%s/mapfiles/%s_%s_%s.%s' % (os.getcwd(),target_mdschema,filetype,'template','xml')
                        with open(templatefile, 'r') as f:
                            try:
                                dsdata= f.read() ##HEW-D ET.parse(templatefile).getroot()
                            except Exception :
                                self.logger.error('    | Cannot load tempalte file %s' % (templatefile))

                        data=dict()
                        jsondata['community']=community
                        ##HEW-D dsdata = Template(dsdata)
                        for facetdict in list(self.b2findfields.values()) :
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
                            self.logger.error("[ERROR] %s\n" % err )
                            pass

                        outfile=outpath+'/'+filetype+'_'+identifier+'.xml'
                        try :
                            f = open(outfile, 'w')
                            f.write(outdata.encode('utf-8'))
                            f.write("\n")
                            f.close
                        except IOError :
                            self.logger.error("[ERROR] Cannot write data in xml file '%s': %s\n" % (outfile))
                            return(False, outfile , outpath, fcount)
	
            if target_mdschema == 'openaire':
                for extradict in jsondata['extras']:
                    jsondata[ extradict['key'] ] = extradict['value']
                
                if 'DOI' in jsondata :
                    print( 'DOI is provided, take it !')
                else:
                    print( 'No DOI available ! => skip record' )
                    continue

                if 'id' in jsondata :
                    identifier=jsondata['id']
                else:
                    identifier=os.path.splitext(filename)[0]
                convertfile='%s/mapfiles/%s%s.%s' % (os.getcwd(),'json2',target_mdschema,'json')
                with open(convertfile, 'r') as f:
                    try:
                        mapdict=json.loads(f.read())
                    except:
                        self.logger.error('    | [ERROR] Cannot load the convert file %s' % convertfile)
                        sys.exit()

                    for filetype in ['dublincore']:
                        outfile=outpath+'/'+os.path.splitext(filename)[0]+'.xml'             
	                ### load xml template
                        templatefile='%s/mapfiles/%s_%s_%s.%s' % (os.getcwd(),target_mdschema,filetype,'template','xml')
                        with open(templatefile, 'r') as f:
                            try:
                                dsdata= f.read() ##HEW-D ET.parse(templatefile).getroot()
                            except Exception :
                                self.logger.error('    | Cannot load tempalte file %s' % (templatefile))

                        data=dict()
                        jsondata['community']=community
                        ##HEW-D dsdata = Template(dsdata)
                        for facetdict in list(self.b2findfields.values()) :
                            facet=facetdict["ckanName"]
                            ##HEW-T                            print ('facet %s ' % facet)
                            if facet in jsondata:
                                if facet == 'author':
                                    creators=jsondata['author'].split(';')
                                    for creator in creators :
                                        namearr= creator.split(',')
                                        data['familyName']=namearr[0]
                                        data['givenName']=namearr[1]
                                value=jsondata[facet]
                            else:
                                value=''


                            if isinstance(value,list) and len(value)>0 :
                                    if facet == 'tags':
                                        data[facet]=''
                                        for tagndict in value:
                                            data[facet]+=tagndict['name']
                                    else:
                                        data[facet]=' '.join(value).strip('\n ')
                            else :
                                    data[facet]=value
                                    ## outdata = dsdata.substitute(key=data[key])
                                    ##HEW-T print('KKKK key %s\t data %s' % (key,data[key]))

                        data['identifier']=identifier
                        data['createdate']=createdate
                        data['oaiset']='openaire_b2find'
                        print(' dsdata %s' % dsdata%data)
                        try:
                            outdata=dsdata%data
                        except KeyError as err :
                            self.logger.error("[ERROR] %s\n" % err )
                            pass

                        ##outfile=outpath+'/'+filename+'.xml'
                        try :
                            f = open(outfile, 'w')
                            f.write(outdata.encode('utf-8'))
                            f.write("\n")
                            f.close
                        except IOError :
                            self.logger.error("[ERROR] Cannot write data in xml file '%s': %s\n" % (outfile))
                            return(False, outfile , outpath, fcount)
	
            elif target_mdschema == 'openaire2' :
                identifier=jsondata["id"]
                filetype=''
                outfile=outpath+'/'+filetype+'/'+community+'_'+identifier+'.xml'
                mapdict=self.datacitefields

                header="""<resource xsi:schemaLocation="http://datacite.org/schema/kernel-4 http://schema.datacite.org/meta/kernel-4.3/metadata.xsd">
"""

                footer="""
</resource>"""

                xmlprefix='datacite:'
                xmldata=header+self.json2xml(jsondata,'\t',xmlprefix,mapdict)+footer
                try:
                    f = open(outfile, 'w')
                    f.write(xmldata.encode('utf-8'))
                    f.write("\n")
                    f.close
                except IOError :
                    self.logger.error("[ERROR] Cannot write data in xml file '%s': %s\n" % (outfile))
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
                    self.logger.error("[ERROR] Cannot write data in xml file '%s': %s\n" % (outfile))
                    return(False, outfile , outpath, fcount)

            self.logger.debug('    | o | %-4d | %-45s | %-45s |' % (fcount,os.path.basename(filename),os.path.basename(outfile)))
            

        self.logger.info('%s     INFO  B2FIND : %d records converted; %d records caused error(s).' % (time.strftime("%H:%M:%S"),fcount,results['ecount']))

        # count ... all .xml files in path/b2find
        results['count'] = len(list([x for x in os.listdir(outpath) if x.endswith('.xml')]))
        print ('   \t|- %-10s |@ %-10s |\n\t| Provided | Converted | Failed |\n\t| %8d | %6d | %6d |' % ( 'Finished',time.strftime("%H:%M:%S"),
                    results['tcount'],
                    fcount,
                    results['ecount']
                ))
    
        return results
