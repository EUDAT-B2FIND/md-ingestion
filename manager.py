#!/usr/bin/env python

"""manager.py
  Management of metadata within the EUDAT Metadata Service B2FIND

Copyright (c) 2013 Heinrich Widmann (DKRZ), John Mrziglod (DKRZ)
Licensed under AGPLv3.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

Modified by  c/o DKRZ 2014   Heinrich Widmann
  New mode 'oaiconvert' added 
Modified by  c/o DKRZ 2015   Heinrich Widmann
  Validation mode enhanced, redesign and bug fixes
Modified by  c/o DKRZ 2016   Heinrich Widmann
  Adapt to new B2HANDLE library
"""

##from __future__ import print_function

import B2FIND

##Py3???
from b2handle.clientcredentials import PIDClientCredentials
from b2handle.handleclient import EUDATHandleClient
from b2handle.handleexceptions import HandleAuthenticationError,HandleNotFoundException,HandleSyntaxError,GenericHandleError
import os, optparse, sys, glob, re
PY2 = sys.version_info[0] == 2

from subprocess import call,Popen,PIPE
import time, datetime
import simplejson as json
import copy

import logging
logger = logging.getLogger()
import traceback
import hashlib
import codecs
import pprint

def setup_custom_logger(name,verbose):
    log_format='%(levelname)s :  %(message)s'
    log_level=logging.CRITICAL
    log_format='[ %(levelname)s <%(module)s:%(funcName)s> @\t%(lineno)4s ] %(message)s'
    if verbose == 1 :
        log_level=logging.ERROR
    elif  verbose == 2 :
        log_level=logging.WARNING
    elif verbose == 3 :
        log_level=logging.INFO
    elif verbose > 3 :
        log_level=logging.DEBUG

    formatter = logging.Formatter(fmt=log_format)

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    logger.setLevel(log_level)
    logger.addHandler(handler)
    return logger

def main():
    global TimeStart
    TimeStart = time.time()

    # check the version from svn:
    global ManagerVersion
    ManagerVersion = '2.2.0'

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

    # Output instance
    OUT = B2FIND.OUTPUT(pstat,now,jid,options)

    ## logger
    logger = setup_custom_logger('root',options.verbose)

    ## logger = logging.getLogger('root')
    ##HEW-D logging.basicConfig(format=log_format, level=log_level) ### logging.DEBUG)
    # print out general info:
    logger.info('Version:  \t%s' % ManagerVersion)
    logger.info('Run mode:   \t%s' % pstat['short'][mode])
    logger.info('Process ID:\t%s' % str(jid))
    logger.debug('Processing modes (to be done):\t')

    for key in pstat['status']:
        if  pstat['status'][key] == 'tbd':
            logger.debug(" %s\t%s" % (key, pstat['status'][key]))

    # check options:
    if ( pstat['status']['u'] == 'tbd'):
    
        # checking given options:
        if (options.iphost):
          if (not options.auth):
             from os.path import expanduser
             home = expanduser("~")
             if(not os.path.isfile(home+'/.netrc')):
                logger.critical('Can not access job host authentification file %s/.netrc ' % home )
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
                logger.critical('API key is neither given by option --auth nor can retrieved from %s/.netrc' % home )
                exit()
        else:
            logger.critical(
                "\033[1m [CRITICAL] " +
                    "For upload mode valid URL of CKAN instance (option -i) and API key (--auth) must be given" + "\033[0;0m"
            )
            sys.exit(-1)
            
    # check options:
    if (not(options.handle_check) and pstat['status']['u'] == 'tbd'):
        logger.warning("You are going to upload datasets to %s without checking handles !" % (options.iphost))

    # write in HTML results file:
    OUT.HTML_print_begin()
    
    ## START PROCESSING:
    logger.info("Start : \t%s\n" % now)
    logger.info("Loop over processes and related requests :\n")
    logger.debug('|- <Process> started : %s' % "<Time>")
    logger.debug(' |- Joblist: %s' % "<Filename of request list>")
    logger.debug('\n   |# %-15s : %-30s \n\t|- %-10s |@ %-10s |' % ('<ReqNo.>','<Request description>','<Status>','<Time>'))

    OUT.save_stats('#Start','subset','StartTime',0)
    
    try:
        # start the process:
        process(options,pstat,OUT)
        exit()
    except Exception :
        logging.critical("[CRITICAL] Program is aborted because of a critical error! Description:")
        logging.critical("%s" % traceback.format_exc())
        exit()
    finally:
        # exit the program and open results HTML file:
        exit_program(OUT)
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        logger.info("End of processing :\t\t%s" % now)

def process(options,pstat,OUT):
    ## process (options,pstat,OUT) - function
    # Starts processing as specified in pstat['tbd'] and 
    #  according the request list given bey the options
    #
    # Parameters:
    # -----------
    # 1. options (OptionsParser object)
    # 2. pstat   (process status dict)  
    #
    
    # set list of request lsits for single or multi mode:
    mode = None
    procOptions=['community','source','verb','mdprefix','mdsubset','target_mdschema']
    if(options.source):
        mode = 'single'
        mandParams=['community','verb','mdprefix'] # mandatory processing params
        for param in mandParams :
            if not getattr(options,param) :
                logger.critical("Processing parameter %s is required in single mode" % param)
                sys.exit(-1)
        reqlist=[[
                options.community,
                options.source,
                options.verb,
                options.mdprefix,
                options.mdsubset,
                options.ckan_check,
                options.handle_check,
                options.target_mdschema
            ]]
    elif(options.list):
        mode = 'multi'
        logger.debug(' |- Joblist:  \t%s' % options.list)
        reqlist=parse_list_file(options)

    ## check job request (processing) options
    logger.debug('|- Command line options')
    for opt in procOptions :
        if hasattr(options,opt) : logger.debug(' |- %s:\t%s' % (opt.upper(),getattr(options,opt)))
    
    ## HARVESTING mode:    
    if (pstat['status']['h'] == 'tbd'):
        logger.info('\n|- Harvesting started : %s' % time.strftime("%Y-%m-%d %H:%M:%S"))
        HV = B2FIND.HARVESTER(OUT,pstat,options.outdir,options.fromdate)
        process_harvest(HV,reqlist)

    ## MAPPINING - Mode:  
    if (pstat['status']['m'] == 'tbd'):
        logger.info('\n|- Mapping started : %s' % time.strftime("%Y-%m-%d %H:%M:%S"))
        MP = B2FIND.MAPPER(OUT,options.outdir)
        process_map(MP,reqlist)

    ## VALIDATOR - Mode:
    if (pstat['status']['v'] == 'tbd'):
        logger.info(' |- Validating started : %s' % time.strftime("%Y-%m-%d %H:%M:%S"))
        MP = B2FIND.MAPPER(OUT,options.outdir)
        process_validate(MP,reqlist)

    ## OAI-CONVERTING - Mode:  
    if (pstat['status']['o'] == 'tbd'):
        logger.info('\n|- OAI-Converting started : %s' % time.strftime("%Y-%m-%d %H:%M:%S"))
        MP = B2FIND.MAPPER(OUT,options.outdir)
        process_oaiconvert(MP, reqlist)
    ## UPLOADING - Mode:  
    if (pstat['status']['u'] == 'tbd'):
        logger.info('\n|- Uploading started : %s' % time.strftime("%Y-%m-%d %H:%M:%S"))
        # create CKAN object                       
        CKAN = B2FIND.CKAN_CLIENT(options.iphost,options.auth)
        UP = B2FIND.UPLOADER(CKAN, OUT)
        logger.info(' |- Host:  \t%s' % CKAN.ip_host )
        # start the process uploading:
        process_upload(UP, reqlist)
    ## DELETING - Mode:
    if (pstat['status']['d'] == 'tbd'):
        # start the process deleting:
        logger.info('\n|- Deleting started : %s' % time.strftime("%Y-%m-%d %H:%M:%S"))

        if mode is 'multi':
            dir = options.outdir+'/delete'
            if os.path.exists(dir):
                process_delete(OUT, dir, options)
            else:
                logger.error('[ERROR] The directory "%s" does not exist! No files for deleting are found!' % (dir))
        else:
            logger.critical("[CRITICAL] Deleting mode only supported in 'multi mode' and an explicitly deleting script given !")


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
        print ('   |# %-4d : %-30s \n\t|- %-10s |@ %-10s |' % (ir,request,'Started',time.strftime("%H:%M:%S")))
        results = HV.harvest(request)
    
        if (results == -1):
            logger.error("Couldn't harvest from %s" % request)

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
        if request[3]=='json' :
            mext='conf'
        else:
            mext='xml'

        if (len (request) > 5):            
            mapfile='%s/%s-%s.%s' % ('mapfiles',request[0],request[5],mext)
            target=request[5]
        else:
            mapfile='%s/%s/%s-%s.%s' % (os.getcwd(),'mapfiles',request[0],request[3],mext)
            if not os.path.isfile(mapfile):
                logger.error('Can not access mapfile %s for community %s and mdformat %s ' % (mapfile,request[0],request[3]))
                mapfile='%s/%s/%s.%s' % (os.getcwd(),'mapfiles',request[3],mext)
                if not os.path.isfile(mapfile):
                    logger.critical('Can not access mapfile for mdformat %s ' % request[3])
                    ##sys.exit(-1)
                    continue
            target=None
        print ('   |# %-4d : %-10s\t%-20s : %-20s \n\t|- %-10s |@ %-10s |' % (ir,request[0],request[2:5],os.path.basename(mapfile),'Started',time.strftime("%H:%M:%S")))
        
        cstart = time.time()
        
        results = MP.map(request,target)

        ctime=time.time()-cstart
        results['time'] = ctime
        
        # save stats:
        MP.OUT.save_stats(request[0]+'-' + request[3],request[4] if len(request)> 4 else '','m',results)

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
        if len(request) > 4:
            outfile='oaidata/%s-%s/%s_*/%s' % (request[0],request[3],request[4],'validation.stat')
        else:
            outfile='oaidata/%s-%s/%s/%s' % (request[0],request[3],'SET_*','validation.stat')
        print ('   |# %-4d : %-10s\t%-20s\t--> %-30s \n\t|- %-10s |@ %-10s |' % (ir,request[0],request[3:5],outfile,'Started',time.strftime("%H:%M:%S")))
        cstart = time.time()

        ### HEW!!!
        target=None
        
        results = MP.validate(request,target)

        ctime=time.time()-cstart
        results['time'] = ctime
        
        # save stats:
        if len(request) > 4:
            MP.OUT.save_stats(request[0]+'-' + request[3],request[4],'v',results)
        else:
            MP.OUT.save_stats(request[0]+'-' + request[3],'SET_1','v',results)
        
def process_oaiconvert(MP, rlist):

    ir=0
    for request in rlist:
        ir+=1
        print ('   |# %-4d : %-10s\t%-20s --> %-10s\n\t|- %-10s |@ %-10s |' % (ir,request[0],request[2:5],request[5],'Started',time.strftime("%H:%M:%S")))
        rcstart = time.time()
        
        results = MP.oaiconvert(request[0],request[3],os.path.abspath(request[2]+'/'+request[4]),request[5])

        rctime=time.time()-rcstart
        results['time'] = rctime
        
        # save stats:
        MP.OUT.save_stats(request[0]+'-' + request[3],request[4],'o',results)


def process_upload(UP, rlist):
    ##HEW-D-ec credentials,ec = None,None

    def print_extra(key,jsondata):
        for v in jsondata['extras']:
            if v['key'] == key:
                print (' Key : %s | Value : %s |' % (v['key'],v['value']))
 

    # create credentials and handle client if required
    if (options.handle_check):
          try:
              pidAttrs=["URL","CHECKSUM","JMDVERSION","B2FINDHOST","IS_METADATA","MD_STATUS","MD_SCHEMA","COMMUNITY","SUBSET"]
              cred = PIDClientCredentials.load_from_JSON('credentials_11098')
          except Exception as err:
              logger.critical("%s : Could not create credentials from credstore %s" % (err,options.handle_check))
              ##p.print_help()
              sys.exit(-1)
          else:
              logger.debug("Create EUDATHandleClient instance")
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
        "iso19139" : "http://www.isotc211.org/2005/gmd/gmd.xsd",        
        "oai_dc" : "http://www.openarchives.org/OAI/2.0/oai_dc.xsd",
        "oai_qdc" : "http://pandata.org/pmh/oai_qdc.xsd",
        "cmdi" : "http://catalog.clarin.eu/ds/ComponentRegistry/rest/registry/profiles/clarin.eu:cr1:p_1369752611610/xsd",
        "json" : "http://json-schema.org/latest/json-schema-core.html",
        "fgdc" : "No specification for fgdc available",
        "hdcp2" : "No specification for hdcp2 available"
        }

    for request in rlist:
        ir+=1
        print ('   |# %-4d : %-10s\t%-20s \n\t|- %-10s |@ %-10s |' % (ir,request[0],request[2:5],'Started',time.strftime("%H:%M:%S")))
        community, source, dir = request[0:3]
        mdprefix = request[3]
        if len(request) > 4:
            subset = request[4]
        else:
            subset = 'SET_1'
        dir = dir+'/'+subset
        
        results = {
            'count':0,
            'ecount':0,
            'ncount':0,
            'tcount':0,
            'time':0
        }

        try:
            ckangroup=CKAN.action('group_list') ## ,{"id":community})
            if community not in ckangroup['result'] :
                logger.critical('Can not found community %s' % community)
                sys.exit(-1)
        except Exception :
            logging.critical("Can not list CKAN groups")
  
        if len(request) > 4:
            m = re.search(r'_\d+$', request[4]) # check if subset ends with '_' + digit
            if m is not None:
                subset=request[4]
            else:
                subset=request[4]+'_1'
        else:
            subset='SET_1'

        path=os.path.abspath('oaidata/'+request[0]+'-'+request[3]+'/'+subset)

        if not os.path.exists(path):
            logging.critical('[ERROR] The directory "%s" does not exist!' % (path))
            
            continue
        
        logger.info('    |   | %-4s | %-40s |\n    |%s|' % ('#','id',"-" * 53))
        
        if (last_community != community and options.ckan_check == 'True'):
            last_community = community
            UP.get_packages(community)
        
        uploadstart = time.time()
        
        # find all .json files in dir/json:
        ##HEW-D files = filter(lambda x: x.endswith('.json'), os.listdir(path+'/json'))
        files = [x for x in os.listdir(path+'/json') if x.endswith('.json')]
        
        results['tcount'] = len(files)
        
        scount = 0
        fcount = 0
        oldperc = 0
        for filename in files:
            ## counter and progress bar
            fcount+=1
            if (fcount<scount): continue
            perc=int(fcount*100/int(len(files)))
            bartags=int(perc/5)
            if perc%10 == 0 and perc != oldperc :
                oldperc=perc
                print ("\t[%-20s] %d / %d%%\r" % ('='*bartags, fcount, perc ))
                sys.stdout.flush()

            jsondata = dict()
            datasetRecord = dict()
            pathfname= path+'/json/'+filename
            if ( os.path.getsize(pathfname) > 0 ):
                with open(pathfname, 'r') as f:
                    try:
                        jsondata=json.loads(f.read(),encoding = 'utf-8')
                    except:
                        logger.error('    | [ERROR] Cannot load the json file %s' % path+'/json/'+filename)
                        results['ecount'] += 1
                        continue
            else:
                results['ecount'] += 1
                continue

            # get dataset id (CKAN name) from filename (a uuid generated identifier):
            ds_id = os.path.splitext(filename)[0]
            
            logger.debug('    | u | %-4d | %-40s |' % (fcount,ds_id))

            # get OAI identifier from json data extra field 'oai_identifier':
            if 'oai_identifier' not in jsondata :
                jsondata['oai_identifier'] = [ds_id]

            oai_id = jsondata['oai_identifier'][0]
            logger.debug("        |-> identifier: %s\n" % (oai_id))
            
            ### CHECK JSON DATA for upload
            jsondata=UP.check(jsondata)
            if jsondata == None :
                logger.critical('File %s failed check and will not been uploaded' % filename)
                continue

            ### ADD SOME EXTRA FIELDS TO JSON DATA:
            #  generate get record request for field MetaDataAccess:
            if (mdprefix == 'json'):
               reqpre = source + '/dataset/'
               mdaccess = reqpre + oai_id
            else:
               reqpre = source + '?verb=GetRecord&metadataPrefix=' + mdprefix
               mdaccess = reqpre + '&identifier=' + oai_id
               urlcheck=UP.check_url(mdaccess)
            index1 = mdaccess

            # exceptions for some communities:
            if (community == 'clarin' and oai_id.startswith('mi_')):
                mdaccess = 'http://www.meertens.knaw.nl/oai/oai_server.php?verb=GetRecord&metadataPrefix=cmdi&identifier=http://hdl.handle.net/10744/' + oai_id
            elif (community == 'sdl'):
                mdaccess =reqpre+'&identifier=oai::record/'+oai_id
            elif (community == 'b2share'):
                if subset.startswith('trng') :
                    mdaccess ='https://trng-b2share.eudat.eu/api/oai2d?verb=GetRecord&metadataPrefix=marcxml&identifier='+oai_id
                else:
                    mdaccess ='https://b2share.eudat.eu/api/oai2d?verb=GetRecord&metadataPrefix=marcxml&identifier='+oai_id

            if UP.check_url(mdaccess) == False :
                logging.critical('URL %s is broken' % (mdaccess))
            else:
                jsondata['MetaDataAccess']=mdaccess

            jsondata['group']=community

            ## Prepare jsondata for upload to CKAN (decode UTF-8, build CKAN extra dict's, ...)
            jsondata=UP.json2ckan(jsondata)

            # Set the tag ManagerVersion:
            jsondata['extras'].append({
                     "key" : "ManagerVersion",
                     "value" : ManagerVersion
                    })
            datasetRecord["JMDVERSION"]=ManagerVersion
            datasetRecord["B2FINDHOST"]=options.iphost

            logger.debug(' JSON dump\n%s' % json.dumps(jsondata, sort_keys=True))
            #HEW-T pp = pprint.PrettyPrinter(indent=4)
            #HEW-T pp.pprint(json.dumps(jsondata, sort_keys=True))
            # determine checksum of json record and append
            try:
                encoding='utf-8' ##HEW-D 'ISO-8859-15' / 'latin-1'
                checksum=hashlib.md5(json.dumps(jsondata, sort_keys=True).encode('latin1')).hexdigest()
            except UnicodeEncodeError as err :
                logger.critical(' %s during md checksum determination' % err)
                checksum=None
            else:
                logger.debug('Checksum of JSON record %s' % checksum)
                jsondata['version'] = checksum
                datasetRecord["CHECKSUM"]=checksum            

            ### check status of dataset (unknown/new/changed/unchanged)
            dsstatus="unknown"

            # check against handle server
            handlestatus="unknown"
            pidRecord=dict()
            ckands='http://'+options.iphost+'/dataset/'+ds_id
            datasetRecord["B2FINDHOST"]=options.iphost
            datasetRecord["IS_METADATA"]='true'
            datasetRecord["MD_STATUS"]="B2FIND_REGISTERED"
            datasetRecord["URL"]=ckands
            datasetRecord["MD_SCHEMA"]=mdschemas[mdprefix]
            datasetRecord["COMMUNITY"]=community
            datasetRecord["SUBSET"]=subset

            if (options.handle_check):

                try:
                    pid = cred.get_prefix() + '/eudat-jmd_' + ds_id 
                    rec = client.retrieve_handle_record_json(pid)
                except Exception as err :
                    logger.error("%s in client.retrieve_handle_record_json(%s)" % (err,pid))
                else:
                    logger.debug("Retrieved PID %s" % pid )

                chargs={}
                
                if rec : ## Handle exists
                    for pidAttr in pidAttrs :##HEW-D ["CHECKSUM","JMDVERSION","B2FINDHOST"] : 
                        try:
                            pidRecord[pidAttr] = client.get_value_from_handle(pid,pidAttr,rec)
                        except Exception:
                            logger.critical("%s in client.get_value_from_handle(%s)" % (err,pidAttr) )
                        else:
                            logger.debug("Got value %s from attribute %s sucessfully" % (pidRecord[pidAttr],pidAttr))

                        if ( pidRecord[pidAttr] == datasetRecord[pidAttr] ) :
                            chmsg="-- not changed --"
                            if pidAttr == 'CHECKSUM' :
                                handlestatus="unchanged"
                            logger.info(" |%-12s\t|%-30s\t|%-30s|" % (pidAttr,pidRecord[pidAttr],chmsg))
                        else:
                            chmsg=datasetRecord[pidAttr]
                            handlestatus="changed"
                            chargs[pidAttr]=datasetRecord[pidAttr] 
                            logger.info(" |%-12s\t|%-30s\t|%-30s|" % (pidAttr,pidRecord[pidAttr],chmsg))
                else:
                    handlestatus="new"
                dsstatus=handlestatus

                if handlestatus == "unchanged" : # no action required :-) !
                    logger.warning(' No action required :-) - next record')
                    results['ncount']+=1
                    continue
                elif handlestatus == "changed" : # update dataset !
                    logger.warning(' Update handle and dataset !')
                    ##??request = urllib2.Request(
                    ##??    'http://'+options.iphost+'/api/action/package_update')
                else : # create new handle !
                    logger.warning(' Create handle and dataset !')
                    chargs=datasetRecord 

            # check against CKAN database
            ckanstatus = 'unknown'                  
            if (options.ckan_check == 'True'):
                ckanstatus=UP.check_dataset(ds_id,checksum)
                if (dsstatus == 'unknown'):
                    dsstatus = ckanstatus

            upload = 0
            # depending on status of handle upload record to B2FIND 
            logger.debug('        |-> Dataset is [%s]' % (dsstatus))
            if ( dsstatus == "unchanged") : # no action required
                logger.warning('        |-> %s' % ('No update required'))
            else:
                try:
                    upload = UP.upload(ds_id,dsstatus,community,jsondata)
                except Exception as err :
                    logger.critical("[CRITICAL : %s] in call of UP.upload" % err )
                else:
                    logger.debug(" Upload of %s returns with upload code %s" % (ds_id,upload))

                if (upload == 1):
                    logger.warning('        |-> Creation of %s record succeed' % dsstatus )
                elif (upload == 2):
                    logger.warning('        |-> Update of %s record succeed' % dsstatus )
                    upload=1
                else:
                    logger.critical('        |-> Failed upload of %s record %s' % (dsstatus, ds_id ))
                    results['ecount'] += 1

            # update PID in handle server                           
            if (options.handle_check):
                if (handlestatus == "unchanged"):
                    logging.warning("        |-> No action required for %s" % pid)
                else:
                    if (upload >= 1): # new or changed record
                        if (handlestatus == "new"): # Create new PID
                            logging.warning("        |-> Create a new handle %s with checksum %s" % (pid,checksum))
                            try:
                                npid = client.register_handle(pid, datasetRecord["URL"], datasetRecord["CHECKSUM"], None, True )
                            except (Exception,HandleAuthenticationError,HandleSyntaxError) as err :
                                logger.critical("%s in client.register_handle" % err )
                                sys.exit()
                            else:
                                logger.debug("New handle %s with checksum %s created" % (pid,datasetRecord["CHECKSUM"]))

                        ## Modify all changed handle attributes
                        if chargs :
                            try:
                                client.modify_handle_value(pid,**chargs) ## ,URL=dataset_dict["URL"]) 
                            except (Exception,HandleAuthenticationError,HandleNotFoundException,HandleSyntaxError) as err :
                                logger.critical("%s in client.modify_handle_value of %s in %s" % (err,chargs,pid))
                            else:
                                logger.debug(" Attributes %s of handle %s changed sucessfully" % (chargs,pid))

            results['count'] +=  upload
            
        uploadtime=time.time()-uploadstart
        results['time'] = uploadtime
        print ('   \n\t|- %-10s |@ %-10s |\n\t| Provided | Uploaded | No action | Failed |\n\t| %8d | %6d |  %8d | %6d |' % ( 'Finished',time.strftime("%H:%M:%S"),
                    results['tcount'],
                    results['count'],
                    results['ncount'],
                    results['ecount']
                ))
        
        # save stats:
        UP.OUT.save_stats(community+'-'+mdprefix,subset,'u',results)

def process_delete(OUT, dir, options):
    print ("###JM# Don't use this function. It is not up to date.")
    return False

    # create CKAN object                       
    CKAN = B2FIND.CKAN_CLIENT(options.iphost,options.auth)
    UP = B2FIND.UPLOADER(CKAN, OUT)
    
    ##HEW-D-ec credentials,ec = None,None

    # create credentials
    try:
        cred = b2handle.clientcredentials.PIDClientCredentials.load_from_JSON('credentials_11098')
    except Exception:
        logging.critical("[CRITICAL] %s Could not create credentials from credstore %s" % (options.handle_check))
        p.print_help()
        sys.exit(-1)
    else:
        logging.debug("Create handle client instance to add uuid to handle server")

    for delete_file in glob.glob(dir+'/*.del'):
        community, mdprefix = os.path.splitext(os.path.basename(delete_file))[0].split('-')
        
        logging.info('\n## Deleting datasets from community "%s" ##' % (community))
        
        # get packages from the group in CKAN:
        UP.get_packages(community)
        
        # open the delete file and loop over its lines:
        file_content = ''
        try:
            f = open(delete_file, 'r')
            file_content = f.read()
            f.close()
        except IOError :
            logging.critical("Cannot read data from '{0}'".format(delete_file))
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
            logging.info('    |   | %-4s | %-50s | %-50s |\n    |%s|' % ('#','oai identifier','CKAN identifier',"-" * 116))
            
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
                logging.info('    | d | %-4d | %-50s | %-50s |' % (results['tcount'],identifier,ds))

                ### CHECK STATUS OF DATASET IN CKAN AND PID:
                # status of data set
                dsstatus="unknown"
         
                # check against handle server
                handlestatus="unknown"
                ##HEW-D-ec???  pid = credentials.prefix + "/eudat-jmd_" + ds
                pid = "11098/eudat-jmd_" + ds_id
                pidRecord["CHECKSUM"] = client.get_value_from_handle(pid, "CHECKSUM")

                if (pidRecord["CHECKSUM"] == None):
                  logging.debug("        |-> Can not access pid %s to get checksum" % (pid))
                  handlestatus="new"
                else:
                  logging.debug("        |-> pid %s exists" % (pid))
                  handlestatus="exist"

                # check against CKAN database
                ckanstatus = 'unknown'                  
                ckanstatus=UP.check_dataset(ds,None)

                delete = 0
                # depending on handle status delete record from B2FIND
                if ( handlestatus == "new" and ckanstatus == "new") : # no action required
                    logging.info('        |-> %s' % ('No deletion required'))
                else:
                    delete = UP.delete(ds,ckanstatus)
                    if (delete == 1):
                        logging.info('        |-> %s' % ('Deletion was successful'))
                        results['count'] +=  1
                        
                        # delete handle (to keep the symmetry between handle and B2FIND server)
                        if (handlestatus == "exist"):
                           logging.info("        |-> Delete handle %s with checksum %s" % (pid,pidRecord["CHECKSUM"]))
                           try:
                               client.delete_handle(pid)
                           except GenericHandleError as err:
                               logging.error('[ERROR] Unexpected Error: %s' % err)
                           except Exception:
                               logging.error('[ERROR] Unexpected Error:')

                        else:
                           logging.info("        |-> No action (deletion) required for handle %s" % pid)
                    else:
                        logging.info('        |-> %s' % ('Deletion failed'))
                        results['ecount'] += 1
        except Exception:
            logging.error('[ERROR] Unexpected Error')
            logging.error('You find the ids of the deleted metadata in "%s"' % (delete_file+'.crash-backup'))
            raise
        else:
            # all worked fine you can remove the crash-backup file:
            os.remove(delete_file+'.crash-backup')
            
        deletetime=time.time()-deletestart
        results['time'] = deletetime
        
        # save stats:
        OUT.save_stats(community+'-'+mdprefix,subset,'d',results)

def parse_list_file(options):
##filename,community=None,subset=None,mdprefix=None,target_mdschema=None):
    filename=options.list
    if(not os.path.isfile(filename)):
        logging.critical('[CRITICAL] Can not access job list file %s ' % filename)
        exit()
    else:
        file = open(filename, 'r')
        lines=file.read().splitlines()
        file.close

    # processing loop over ingestion requests
    inside_comment = False
    reqlist = []

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
        if((options.community != None) and ( not request.startswith(options.community))):
            continue

        # sort out lines that don't match given mdprefix
        if (options.mdprefix != None):
            if ( not request.split()[3] == options.mdprefix) :
              continue

        # sort out lines that don't match given subset
        if (options.mdsubset != None):
            if len(request.split()) < 5 :
                request+=' '+options.mdsubset
            elif ( len(request.split()) > 4 and not request.split()[4] == options.mdsubset ) and (not ( options.mdsubset.endswith('*') and request.split()[4].startswith(options.mdsubset.translate(None, '*')))) :
                continue
                
        if (options.target_mdschema != None):
            request+=' '+options.target_mdschema  

        reqlist.append(request.split())
        
    if len(reqlist) == 0:
        logging.critical(' No matching request found in %s\n\tfor options %s' % (filename,options) )
        exit()
 
    return reqlist

def options_parser(modes):
    
    p = optparse.OptionParser(
        description = '''Description                                                              
===========                                                                           
 Management of metadata within EUDAT B2FIND, comprising                                      
      - Harvesting of XML files from OAI-PMH MD provider(s)\n\t

              - Mapping XML to JSON and semantic mapping of metadata to B2FIND schema\n\t

\n              - Validation of the JSON records and create coverage statistics\n\t
              - Uploading resulting JSON {key:value} dict\'s as datasets to the B2FIND portal\n\t
              - OAI compatible creation of XML records in oai_b2find format\n\t
    
''',
        formatter = optparse.TitledHelpFormatter(),
        prog = 'manager.py',
        epilog='For any further information and documentation please look at the README.md file or at the EUDAT wiki (http://eudat.eu/b2find).',
        version = "%prog " + ManagerVersion,
        usage = "%prog [options]" 
    )
        
    p.add_option('-v', '--verbose', action="count", 
                        help="increase output verbosity (e.g., -vv is more than -v)", default=False)
    p.add_option('--jobdir', help='\ndirectory where log, error and html-result files are stored. By default directory is created as startday/starthour/processid .', default=None)
    p.add_option('--mode', '-m', metavar='PROCESSINGMODE', help='\nThis can be used to do a partial workflow. Supported modes are (h)arvesting, (c)onverting, (m)apping, (v)alidating, (o)aiconverting and (u)ploading or a combination. default is h-u, i.e. a total ingestion', default='h-u')
    p.add_option('--community', '-c', help="community where data harvested from and uploaded to", metavar='STRING')
    p.add_option('--fromdate', help="Filter harvested files by date (Format: YYYY-MM-DD).", default=None, metavar='DATE')
    p.add_option('--outdir', '-d', help="The relative root dir in which all harvested files will be saved. The converting and the uploading processes work with the files from this dir. (default is 'oaidata')",default='oaidata', metavar='PATH')
    
         
    group_multi = optparse.OptionGroup(p, "Multi Mode Options",
        "Use these options if you want to ingest from a list in a file.")
    group_multi.add_option('--list', '-l', help="list of OAI harvest sources (default is ./harvest_list)", default='harvest_list',metavar='FILE')
    group_multi.add_option('--parallel', 
        help="[DEPRECATED]",#performs list of ingest requests in parallel (makes only sense with option [--list|-l] )",
        default='serial')     
         
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
    group_upload.add_option('--auth', help="Authentification for CKAN APIs (API key, iby default taken from file $HOME/.netrc)",metavar='STRING')
    group_upload.add_option('--handle_check', 
         help="check and generate handles of CKAN datasets in handle server and with credentials as specified in given credstore file", default=None,metavar='FILE')
    group_upload.add_option('--ckan_check',
         help="check existence and checksum against existing datasets in CKAN database", default='False', metavar='BOOLEAN')
    
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
      logging.debug('For more info open HTML file %s' % OUT.jobdir+'/overview.html')

if __name__ == "__main__":
    main()
