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

import B2FIND 
from b2handle.clientcredentials import PIDClientCredentials
from b2handle.handleclient import EUDATHandleClient
from b2handle.handleexceptions import HandleAuthenticationError,HandleNotFoundException,HandleSyntaxError,GenericHandleError
import os, optparse, sys, glob
from subprocess import call,Popen,PIPE
import time, datetime
import simplejson as json
import copy

import logging as log
import traceback
import hashlib
import codecs

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
    global logger
    OUT = B2FIND.OUTPUT(pstat,now,jid,options)
    logger = log.getLogger()
    
    # print out general info:
    logger.info('\nVersion:  \t%s' % ManagerVersion)
    logger.info('Run mode:   \t%s' % pstat['short'][mode])
    logger.debug('Process ID:\t%s' % str(jid))
    logger.debug('Processing status:\t')
    for key in pstat['status']:
        logger.debug(" %s\t%s" % (key, pstat['status'][key]))
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
            
    # check options:
    if (not(options.handle_check) and pstat['status']['u'] == 'tbd' and 'b2find.eudat.eu' in options.iphost):
        logger.debug("\n[WARNING] You are going to upload datasets to the host %s with generating PID's!" % (options.iphost))
        answer = 'Y'
        while (not(answer == 'N' or answer == 'n' or answer == 'Y')):
            answer = raw_input("Do you really want to continue? (Y / n) >>> ")
        
        if (answer == 'n' or answer == 'N'):
            exit()
            
        print '\n'
    elif (options.handle_check and pstat['status']['u'] == 'tbd' and not('b2find.eudat.eu' in options.iphost)):
        logger.debug("\n[WARNING] You are going to upload datasets to the host %s with generating handles!" % (options.iphost))
        answer = 'Y'
        while (not(answer == 'N' or answer == 'n' or answer == 'Y')):
            answer = raw_input("Do you really want to continue? (Y / n) >>> ")
        
        if (answer == 'n' or answer == 'N'):
            exit()
            
        print '\n'

    # write in HTML results file:
    OUT.HTML_print_begin()
    
    ## START PROCESSING:
    logger.info("Start : \t%s\n" % now)
    logger.info("Loop over processes and related requests :\n")
    logger.info('|- <Process> started : %s' % "<Time>")
    logger.info(' |- Joblist: %s' % "<Filename of request list>")
    logger.info('   |# %-15s : %-30s \n\t|- %-10s |@ %-10s |' % ('<ReqNo.>','<Request description>','<Status>','<Time>'))



    OUT.save_stats('#Start','subset','StartTime',0)
    
    try:
        # start the process:
        process(options,pstat,OUT)
        exit()
    except Exception, e:
        logger.critical("[CRITICAL] Program is aborted because of a critical error! Description:")
        logger.critical("%s" % traceback.format_exc())
        exit()
    finally:
        # exit the program and open results HTML file:
        exit_program(OUT)
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        logger.info("\nEnd :\t\t%s" % now)


def process(options,pstat,OUT):
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
            logger.critical("\033[1m [CRITICAL] " + "When single mode is used following options are required:\n\t%s" % (
                '\n\t'.join(['community','source','verb','mdprefix'])) + "\033[0;0m" 
            )
            exit()
        
    elif(options.list):
        mode = 'multi'
    else:
        logger.critical("[CRITICAL] Either option source (option -s) or list of sources (option -l) is required")
        exit()
    
    ## HARVESTING mode:    
    if (pstat['status']['h'] == 'tbd'):
        # start the process harvesting:
        logger.info('\n|- Harvesting started : %s' % time.strftime("%Y-%m-%d %H:%M:%S"))
        HV = B2FIND.HARVESTER(OUT,pstat,options.outdir,options.fromdate)
        
        if mode is 'multi':
            logger.info(' |- Joblist:  \t%s' % options.list)
            if (options.community != '') : logger.debug(' |- Community:\t%s' % options.community)
            if (options.mdsubset != None) : logger.debug(' |- OAI subset:\t%s' % options.mdsubset)
            process_harvest(HV,parse_list_file('harvest',options.list, options.community,options.mdsubset))
        else:
            process_harvest(HV,[[
                options.community,
                options.source,
                options.verb,
                options.mdprefix,
                options.mdsubset
            ]])

    if (OUT.convert_list or pstat['status']['h'] == 'no'):
        ## MAPPINING - Mode:  
        if (pstat['status']['m'] == 'tbd'):
            # start the process mapping:
            logger.info('\n|- Mapping started : %s' % time.strftime("%Y-%m-%d %H:%M:%S"))
            MP = B2FIND.MAPPER(OUT)
        
            # start the process mapping:
            if mode is 'multi':
                logger.info(' |- Joblist:  \t%s' % OUT.convert_list )
                if (options.community != '') : logger.debug(' |- Community:\t%s' % options.community)
                if (options.mdsubset != None) : logger.debug(' - OAI subset:\t%s' % options.mdsubset)
                process_map(MP, parse_list_file('convert', OUT.convert_list or options.list, options.community,options.mdsubset))
            else:
                process_map(MP,[[
                    options.community,
                    options.source,
                    options.mdprefix,
                    options.outdir + '/' + options.mdprefix,
                    options.mdsubset
                ]])
        ## VALIDATOR - Mode:  
        if (pstat['status']['v'] == 'tbd'):
            MP = B2FIND.MAPPER(OUT)
            logger.info('\n|- Validating started : %s' % time.strftime("%Y-%m-%d %H:%M:%S"))
        
            # start the process converting:
            if mode is 'multi':
                logger.info(' |- Joblist:  \t%s' % options.list)
                process_validate(MP, parse_list_file('validate', OUT.convert_list or options.list, options.community,options.mdsubset))
            else:
                process_validate(MP,[[
                    options.community,
                    options.source,
                    options.mdprefix,
                    options.outdir + '/' + options.mdprefix,
                    options.mdsubset
                ]])
        ## OAI-CONVERTING - Mode:  
        if (pstat['status']['o'] == 'tbd'):
            MP = B2FIND.MAPPER(OUT)
            logger.info('\n|- Converting started : %s' % time.strftime("%Y-%m-%d %H:%M:%S"))

            # start the process converting:
            if mode is 'multi':
                process_oaiconvert(MP, parse_list_file('oaiconvert', OUT.convert_list or options.list, options.community,options.mdsubset))
            else:
                process_oaiconvert(MP,[[
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
            logger.info('\n|- Uploading started : %s' % time.strftime("%Y-%m-%d %H:%M:%S"))
            logger.info(' |- Host:  \t%s' % CKAN.ip_host )

            # start the process uploading:
            if mode is 'multi':
                logger.info(' |- Joblist:  \t%s' % OUT.convert_list )
                process_upload(UP, parse_list_file('upload', OUT.convert_list or options.list, options.community, options.mdsubset), options)
            else:
                process_upload(UP,[[
                    options.community,
                    options.source,
                    options.mdprefix,
                    options.outdir + '/' + options.mdprefix,
                    options.mdsubset
                ]],options)
    else:
        logger.warning('\n[WARNING] No metadata were harvested! Therefore no data will be mapped and uploaded.')
    
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
        logger.info('   |# %-4d : %-30s \n\t|- %-10s |@ %-10s |' % (ir,request,'Started',time.strftime("%H:%M:%S")))
        results = HV.harvest(ir,request)
    
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
        mapfile='%s/%s-%s.xml' % ('mapfiles',request[0],request[3])
        logger.info('   |# %-4d : %-10s\t%-20s : %-20s \n\t|- %-10s |@ %-10s |' % (ir,request[0],request[2:5],mapfile,'Started',time.strftime("%H:%M:%S")))
        
        cstart = time.time()
        
        results = MP.map(ir,request[0],request[3],os.path.abspath(request[2]+'/'+request[4]))

        ctime=time.time()-cstart
        results['time'] = ctime
        
        # save stats:
        MP.OUT.save_stats(request[0]+'-' + request[3],request[4],'m',results)

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
        logger.info('   |# %-4d : %-10s\t%-20s\t--> %-30s \n\t|- %-10s |@ %-10s |' % (ir,request[0],request[3:5],outfile,'Started',time.strftime("%H:%M:%S")))
        cstart = time.time()
        
        results = MP.validate(request[0],request[3],os.path.abspath(request[2]+'/'+request[4]))

        ctime=time.time()-cstart
        results['time'] = ctime
        
        # save stats:
        MP.OUT.save_stats(request[0]+'-' + request[3],request[4],'v',results)
        
def process_oaiconvert(MP, rlist):

    for request in rlist:
        logger.info('   |# %-4d : %-10s\t%-20s \n\t|- %-10s |@ %-10s |' % (ir,request[0],request[2:5],'Started',time.strftime("%H:%M:%S")))
        rcstart = time.time()
        
        results = MP.oaiconvert(request[0],request[3],os.path.abspath(request[2]+'/'+request[4]))

        print results
        rctime=time.time()-rcstart
        results['time'] = rctime
        
        # save stats:
        MP.OUT.save_stats(request[0]+'-' + request[3],request[4],'o',results)


def process_upload(UP, rlist, options):
    ##HEW-D-ec credentials,ec = None,None

    # create credentials and handle cleint if required
    if (options.handle_check):
          try:
              cred = PIDClientCredentials.load_from_JSON('credentials_11098')
          except Exception, err:
              logger.critical("[CRITICAL %s ] : Could not create credentials from credstore %s" % (err,options.handle_check))
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
        "oai_dc" : "http://www.openarchives.org/OAI/2.0/oai_dc.xsd",
        "oai_qdc" : "http://pandata.org/pmh/oai_qdc.xsd",
        "cmdi" : "http://catalog.clarin.eu/ds/ComponentRegistry/rest/registry/profiles/clarin.eu:cr1:p_1369752611610/xsd",
        "json" : "http://json-schema.org/latest/json-schema-core.html",
        "fgdc" : "No specification for fgdc available",
        "hdcp2" : "No specification for hdcp2 available"
        }
    for request in rlist:
        ir+=1
        logger.info('   |# %-4d : %-10s\t%-20s \n\t|- %-10s |@ %-10s |' % (ir,request[0],request[2:5],'Started',time.strftime("%H:%M:%S")))
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
          self.logger.error("[ERROR]: Community (CKAN group) %s must exist!!!" % community)
          sys.exit()

        if not os.path.exists(dir):
            logger.error('[ERROR] The directory "%s" does not exist! No files for uploading are found!\n(Maybe your upload list has old items?)' % (dir))
            
            # save stats:
            UP.OUT.save_stats(community+'-'+mdprefix,subset,'u',results)
            
            continue
        
        logger.debug('    |   | %-4s | %-40s |\n    |%s|' % ('#','id',"-" * 53))
        
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
                logger.info("\t[%-20s] %d / %d%%\r" % ('='*bartags, fcount, perc ))
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
            
            logger.debug('    | u | %-4d | %-40s |' % (fcount,ds_id))
            
            # get OAI identifier from json data extra field 'oai_identifier':
            oai_id  = None
            for extra in jsondata['extras']:
                if(extra['key'] == 'oai_identifier'):
                    oai_id = extra['value']
                    break
            logger.debug("        |-> identifier: %s\n" % (oai_id))
            
            ### VALIDATE JSON DATA
            if (UP.check(jsondata) < 1):
                logger.info('        |-> Could not upload %s' % pathfname )
                results['ecount'] += 1
                continue

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

            jsondata['extras'].append({
                     "key" : "MetaDataAccess",
                     "value" : mdaccess
                    })
            
            # determine checksum of json record and append
            try:
                # delete the extra field 'MapperVersion' from check_data
                check_data = copy.deepcopy(jsondata)
                extras_counter = 0
                for extra in check_data['extras']:
                    if(extra['key'] == 'MapperVersion'):
                        check_data['extras'].pop(extras_counter)
                        break
                    extras_counter  += 1
                    
                checksum=hashlib.md5(unicode(json.dumps(check_data))).hexdigest()
            except UnicodeEncodeError:
                logger.error('        |-> [ERROR] Unicode encoding failed during md checksum determination')
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
                    logger.critical("[CRITICAL : %s] in client.get_value_from_handle" % err )
                else:
                    logger.debug("Got checksum2 %s, ManagerVersion2 %s and B2findHost %s from PID %s" % (checksum2,ManagerVersion2,B2findHost,pid))
                if (checksum2 == None):
                    logger.debug("        |-> Can not access pid %s to get checksum" % pid)
                    handlestatus="new"
                elif ( checksum == checksum2) and ( ManagerVersion2 == ManagerVersion ) and ( B2findHost == options.iphost ) :
                    logger.debug("        |-> checksum, ManagerVersion and B2FIND host of pid %s not changed" % (pid))
                    handlestatus="unchanged"
                else:
                    logger.debug("        |-> checksum, ManagerVersion or B2FIND host of pid %s changed" % (pid))
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
            logger.debug('        |-> Dataset is [%s]' % (dsstatus))
            if ( dsstatus == "unchanged") : # no action required
                logger.info('        |-> %s' % ('No upload required'))
            else:
                upload = UP.upload(ds_id,dsstatus,community,jsondata)
                if (upload == 1):
                    logger.debug('        |-> Creation of %s record succeed' % dsstatus )
                elif (upload == 2):
                    logger.debug('        |-> Update of %s record succeed' % dsstatus )
                    upload=1
                else:
                    logger.error('        |-> Upload of %s record failed ' % dsstatus )
            
            # update PID in handle server                           
            if (options.handle_check):
                if (handlestatus == "unchanged"):
                    logger.info("        |-> No action required for %s" % pid)
                else:
                    if (upload >= 1): # new or changed record
                        ckands='http://b2find.eudat.eu/dataset/'+ds_id
                        if (handlestatus == "new"): # Create new PID
                            logger.info("        |-> Create a new handle %s with checksum %s" % (pid,checksum))
                            try:
                                npid = client.register_handle(pid, ckands, checksum, None, True ) ## , additional_URLs=None, overwrite=False, **extratypes)
                            except (HandleAuthenticationError,HandleSyntaxError) as err :
                                logger.critical("[CRITICAL : %s] in client.register_handle" % err )
                            except Exception, err:
                                logger.critical("[CRITICAL : %s] in client.register_handle" % err )
                                sys.exit()
                            else:
                                logger.debug(" New handle %s with checksum %s created" % (pid,checksum))
                        else: # PID changed => update URL and checksum
                            try:
                                client.modify_handle_value(pid,URL=ckands) ##HEW-T !!! as long as URLs not all updated !!
                                client.modify_handle_value(pid,CHECKSUM=checksum)
                            except (HandleAuthenticationError,HandleNotFoundException,HandleSyntaxError) as err :
                                logger.critical("[CRITICAL : %s] client.modify_handle_value %s" % (err,pid))
                            except Exception, err:
                                logger.critical("[CRITICAL : %s]  client.modify_handle_value %s" % (err,pid))
                                sys.exit()
                            else:
                                logger.debug(" Modified JMDVERSION, COMMUNITY or B2FINDHOST of handle %s " % pid)

                    try: # update PID entries in all cases (except handle status is 'unchanged'
                        client.modify_handle_value(pid, JMDVERSION=ManagerVersion)
                        client.modify_handle_value(pid, COMMUNITY=community)
                        client.modify_handle_value(pid, SUBSET=subset)
                        client.modify_handle_value(pid, B2FINDHOST=options.iphost)
                        client.modify_handle_value(pid, IS_METADATA=True)
                        client.modify_handle_value(pid, MD_SCHEMA=mdschemas[mdprefix])
                        client.modify_handle_value(pid, MD_STATUS='B2FIND_uploaded')
                    except (HandleAuthenticationError,HandleNotFoundException,HandleSyntaxError) as err :
                        logger.critical("[CRITICAL : %s] in client.modify_handle_value of pid %s" % (err,pid))
                    except Exception, err:
                        logger.critical("[CRITICAL : %s] in client.modify_handle_value of %s" % (err,pid))
                        sys.exit()
                    else:
                        logger.debug(" Modified JMDVERSION, COMMUNITY or B2FINDHOST of handle %s " % pid)

            results['count'] +=  upload
            
        uploadtime=time.time()-uploadstart
        results['time'] = uploadtime
        logger.info(
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
        logger.critical("[CRITICAL] %s Could not create credentials from credstore %s" % (err,options.handle_check))
        p.print_help()
        sys.exit(-1)
    else:
        logger.debug("Create handle client instance to add uuid to handle server")

    for delete_file in glob.glob(dir+'/*.del'):
        community, mdprefix = os.path.splitext(os.path.basename(delete_file))[0].split('-')
        
        logger.info('\n## Deleting datasets from community "%s" ##' % (community))
        
        # get packages from the group in CKAN:
        UP.get_packages(community)
        
        # open the delete file and loop over its lines:
        file_content = ''
        try:
            f = open(delete_file, 'r')
            file_content = f.read()
            f.close()
        except IOError as (errno, strerror):
            self.logger.critical("Cannot read data from '{0}': {1}".format(delete_file, strerror))
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
            logger.info('    |   | %-4s | %-50s | %-50s |\n    |%s|' % ('#','oai identifier','CKAN identifier',"-" * 116))
            
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
                logger.info('    | d | %-4d | %-50s | %-50s |' % (results['tcount'],identifier,ds))

                ### CHECK STATUS OF DATASET IN CKAN AND PID:
                # status of data set
                dsstatus="unknown"
         
                # check against handle server
                handlestatus="unknown"
                ##HEW-D-ec???  pid = credentials.prefix + "/eudat-jmd_" + ds
                pid = "11098/eudat-jmd_" + ds_id
                checksum2 = client.get_value_from_handle(pid, "CHECKSUM")

                if (checksum2 == None):
                  logger.debug("        |-> Can not access pid %s to get checksum" % (pid))
                  handlestatus="new"
                else:
                  logger.debug("        |-> pid %s exists" % (pid))
                  handlestatus="exist"

                # check against CKAN database
                ckanstatus = 'unknown'                  
                ckanstatus=UP.check_dataset(ds,None)

                delete = 0
                # depending on handle status delete record from B2FIND
                if ( handlestatus == "new" and ckanstatus == "new") : # no action required
                    logger.info('        |-> %s' % ('No deletion required'))
                else:
                    delete = UP.delete(ds,ckanstatus)
                    if (delete == 1):
                        logger.info('        |-> %s' % ('Deletion was successful'))
                        results['count'] +=  1
                        
                        # delete handle (to keep the symmetry between handle and B2FIND server)
                        if (handlestatus == "exist"):
                           logger.info("        |-> Delete handle %s with checksum %s" % (pid,checksum2))
                           try:
                               client.delete_handle(pid)
                           except GenericHandleError as err:
                               logger.error('[ERROR] Unexpected Error: %s' % err)
                           except Exception, e:
                               logger.error('[ERROR] Unexpected Error: %s' % e)

                        else:
                           logger.info("        |-> No action (deletion) required for handle %s" % pid)
                    else:
                        logger.info('        |-> %s' % ('Deletion failed'))
                        results['ecount'] += 1
        except Exception, e:
            logger.error('[ERROR] Unexpected Error: %s' % e)
            logger.error('You find the ids of the deleted metadata in "%s"' % (delete_file+'.crash-backup'))
            raise
        else:
            # all worked fine you can remove the crash-backup file:
            os.remove(delete_file+'.crash-backup')
            
        deletetime=time.time()-deletestart
        results['time'] = deletetime
        
        # save stats:
        OUT.save_stats(community+'-'+mdprefix,subset,'d',results)

def parse_list_file(process,filename,community='',subset=''):
    if(not os.path.isfile(filename)):
        logger.critical('[CRITICAL] Can not access job list file %s ' % filename)
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
        if((community != '') and ( not request.startswith(community))):
            continue
            
        # sort out lines that don't match given subset
        if (subset != None):
            if len(request.split()) < 5 :
               continue
            elif ( not request.split()[4] == subset ) and (not ( subset.endswith('*') and request.split()[4].startswith(subset.translate(None, '*')))) :
              continue
            
        reqlist.append(request.split())
        
    if len(reqlist) == 0:
        logger.error(' No matching request found in %s' % filename)
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
        version = "%prog " + ManagerVersion
    )
        
    p.add_option('-v', '--verbose', action="count", 
                        help="increase output verbosity (e.g., -vv is more than -v)", default=False)
    p.add_option('--jobdir', help='\ndirectory where log, error and html-result files are stored. By default directory is created as startday/starthour/processid .', default=None)
    p.add_option('--mode', '-m', metavar='PROCESSINGMODE', help='\nThis can be used to do a partial workflow. Supported modes are (h)arvesting, (c)onverting, (m)apping, (v)alidating, (o)aiconverting and (u)ploading or a combination. default is h-u, i.e. a total ingestion', default='h-u')
    p.add_option('--community', '-c', help="community where data harvested from and uploaded to", default='', metavar='STRING')
    p.add_option('--fromdate', help="Filter harvested files by date (Format: YYYY-MM-DD).", default=None, metavar='DATE')
    p.add_option('--handle_check', 
         help="check and generate handles of CKAN datasets in handle server and with credentials as specified in given credstore file",
         default=None,metavar='FILE')
    p.add_option('--ckan_check',
         help="check existence and checksum against existing datasets in CKAN database",
         default='False', metavar='BOOLEAN')
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
    
    group_upload = optparse.OptionGroup(p, "Upload Options",
        "These options will be required to upload an dataset to a CKAN database.")
    group_upload.add_option('--iphost', '-i', help="IP adress of B2FIND portal (CKAN instance)", metavar='IP')
    group_upload.add_option('--auth', help="Authentification for CKAN APIs (API key, iby default taken from file $HOME/.netrc)",metavar='STRING')
    
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
      try:
        os.system('firefox '+OUT.jobdir+'/overview.html')
      except Exception, err:
        print("[ERROR] %s : Can not open result html in browser" % err)
        os.system('cat '+OUT.jobdir+'/overview.html')

if __name__ == "__main__":
    main()
