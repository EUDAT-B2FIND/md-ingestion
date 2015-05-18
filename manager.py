#!/usr/bin/env python

"""manager.py
  Management of metadata within the EUDAT Joint Metadata Domain (B2FIND)
  MD Ingestion : Harvest from OAI provider, convert/map XML (to JSON), semantic mapping of MD schema, remap to B2FIND xml, and upload to B2FIND portal

Copyright (c) 2013 Heinrich Widmann (DKRZ), John Mrziglod (DKRZ)
Licensed under AGPLv3.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.


~~~~~~~
For some functions (processes) you haved to install additional libraries

Process                Libraries      Install commands

harvest                pyoai, lxml    easy_install pyoai, lxml or pip install pyoai, lxml 

Modified by  c/o DKRZ 2013   Heinrich Widmann
"""

import B2FIND 
from epicclient import EpicClient,Credentials
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
    ManagerVersion = '1.0'

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
             logger.info(
                'NOTE : For upload mode write access to %s via API key %s must be allowed' % (options.iphost,options.auth)
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
    if (not(options.epic_check) and pstat['status']['u'] == 'tbd' and 'b2find.eudat.eu' in options.iphost):
        print "\n[WARNING] You want to upload datasets to the productive host %s without EPIC handling!" % (options.iphost)
        answer = 'Y'
        while (not(answer == 'N' or answer == 'n' or answer == 'Y')):
            answer = raw_input("Do you really want to continue? (Y / n) >>> ")
        
        if (answer == 'n' or answer == 'N'):
            exit()
            
        print '\n'
    elif (options.epic_check and pstat['status']['u'] == 'tbd' and not('b2find.eudat.eu' in options.iphost)):
        print "\n[WARNING] You want to upload datasets to the non-productive host %s with EPIC handling!" % (options.iphost)
        answer = 'Y'
        while (not(answer == 'N' or answer == 'n' or answer == 'Y')):
            answer = raw_input("Do you really want to continue? (Y / n) >>> ")
        
        if (answer == 'n' or answer == 'N'):
            exit()
            
        print '\n'

    # write in HTML results file:
    OUT.HTML_print_begin()
    
    ## START PROCESSING:
    logger.info("\nStart :\t%s" % now)
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
    
    ## HARVESTING - Mode:    
    if (pstat['status']['h'] == 'tbd'):
        HV = B2FIND.HARVESTER(OUT,pstat,options.outdir,options.fromdate)
        
        # start the process harvesting:
        if mode is 'multi':
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
        ## CONVERTING - Mode:  
        if (pstat['status']['c'] == 'tbd'):
            CV = B2FIND.CONVERTER(OUT)
        
            # start the process converting:
            if mode is 'multi':
                process_convert(CV, parse_list_file('convert', OUT.convert_list or options.list, options.community,options.mdsubset))
            else:
                process_convert(CV,[[
                    options.community,
                    options.source,
                    options.mdprefix,
                    options.outdir + '/' + options.mdprefix,
                    options.mdsubset
                ]])
        ## MAPPINING - Mode:  
        if (pstat['status']['m'] == 'tbd'):
            CV = B2FIND.CONVERTER(OUT)
        
            # start the process mapping:
            if mode is 'multi':
                process_map(CV, parse_list_file('convert', OUT.convert_list or options.list, options.community,options.mdsubset))
            else:
                process_map(CV,[[
                    options.community,
                    options.source,
                    options.mdprefix,
                    options.outdir + '/' + options.mdprefix,
                    options.mdsubset
                ]])
        ## VALIDATOR - Mode:  
        if (pstat['status']['v'] == 'tbd'):
            CV = B2FIND.CONVERTER(OUT)
        
            # start the process converting:
            if mode is 'multi':
                process_validate(CV, parse_list_file('validate', OUT.convert_list or options.list, options.community,options.mdsubset))
            else:
                process_validate(CV,[[
                    options.community,
                    options.source,
                    options.mdprefix,
                    options.outdir + '/' + options.mdprefix,
                    options.mdsubset
                ]])
        ## OAI-CONVERTING - Mode:  
        if (pstat['status']['o'] == 'tbd'):
            CV = B2FIND.CONVERTER(OUT)
        
            # start the process converting:
            if mode is 'multi':
                process_oaiconvert(CV, parse_list_file('oaiconvert', OUT.convert_list or options.list, options.community,options.mdsubset))
            else:
                process_oaiconvert(CV,[[
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

            # start the process uploading:
            if mode is 'multi':
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
    for request in rlist:
        logger.info('\n## Harvesting request %s##' % request)
        
        harveststart = time.time()
        results = HV.harvest(request)
    
        if (results == -1):
            logger.error("Couldn't harvest from %s" % request)

        harvesttime=time.time()-harveststart
        #results['time'] = harvesttime

def process_convert(CV, rlist):
    ## process_convert (CONVERTER object, rlist) - function
    # Maps per request.
    #
    # Parameters:
    # -----------
    # (object)  CONVERTER - object from the class CONVERTER
    # (list)    rlist - list of requests 
    #
    # Return Values:
    # --------------
    # None
    for request in rlist:
        logger.info('\n## Converting request %s##' % request)
        
        cstart = time.time()
        
        results = CV.convert(request[0],request[3],os.path.abspath(request[2]+'/'+request[4]))

        ctime=time.time()-cstart
        results['time'] = ctime
        
        # save stats:
        CV.OUT.save_stats(request[0]+'-' + request[3],request[4],'c',results)
    
def process_map(CV, rlist):
    ## process_map (CONVERTER object, rlist) - function
    # Maps per request.
    #
    # Parameters:
    # -----------
    # (object)  CONVERTER - object from the class CONVERTER
    # (list)    rlist - list of requests 
    #
    # Return Values:
    # --------------
    # None
    for request in rlist:
        logger.info('\n## Mapping request %s##' % request)
        
        cstart = time.time()
        
        results = CV.map(request[0],request[3],os.path.abspath(request[2]+'/'+request[4]))

        ctime=time.time()-cstart
        results['time'] = ctime
        
        # save stats:
        CV.OUT.save_stats(request[0]+'-' + request[3],request[4],'m',results)
        
def process_validate(CV, rlist):
    ## process_validate (CONVERTER object, rlist) - function
    # Validates per request.
    #
    # Parameters:
    # -----------
    # (object)  VALIDATOR - object from the class CONVERTER
    # (list)    rlist - list of request lists 
    #
    # Return Values:
    # --------------
    # None
    for request in rlist:
        logger.info('\n## Validating request %s##' % request)
        
        cstart = time.time()
        
        results = CV.validate(request[0],request[3],os.path.abspath(request[2]+'/'+request[4]))

        ctime=time.time()-cstart
        results['time'] = ctime
        
        # save stats:
        CV.OUT.save_stats(request[0]+'-' + request[3],request[4],'v',results)
        
def process_oaiconvert(CV, rlist):

    for request in rlist:
        logger.info('\n## OAI-Mapping request %s##' % request)
        
        rcstart = time.time()
        
        results = CV.oaiconvert(request[0],request[3],os.path.abspath(request[2]+'/'+request[4]))

        print results
        rctime=time.time()-rcstart
        results['time'] = rctime
        
        # save stats:
        CV.OUT.save_stats(request[0]+'-' + request[3],request[4],'o',results)


def process_upload(UP, rlist, options):
    credentials,ec = None,None

    # create credentials if required
    if (options.epic_check):
          try:
              credentials = Credentials('os',options.epic_check)
              credentials.parse()
          except Exception, err:
              logger.critical("[CRITICAL] %s Could not create credentials from credstore %s" % (err,options.epic_check))
              p.print_help()
              sys.exit(-1)
          else:
              logger.debug("Create EPIC client instance to add uuid to handle server")
              ec = EpicClient(credentials)

    CKAN = UP.CKAN
    last_community = ''
    package_list = dict()

    for request in rlist:
        logger.info('\n## Uploading request %s##' % request)
        
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
        
        logger.info('    |   | %-4s | %-40s |\n    |%s|' % ('#','id',"-" * 53))
        
        if (last_community != community and options.ckan_check == 'True'):
            last_community = community
            UP.get_packages(community)
        
        uploadstart = time.time()
        
        # find all .json files in dir/json:
        files = filter(lambda x: x.endswith('.json'), os.listdir(dir+'/json'))
        
        results['tcount'] = len(files)
        
        scount = 0
        fcount = 1
        for filename in files:
            if (fcount<scount):
              fcount += 1
              continue

            jsondata = dict()
        
            if ( os.path.getsize(dir+'/json/'+filename) > 0 ):
                with open(dir+'/json/'+filename, 'r') as f:
                    try:
                        jsondata=json.loads(f.read())
                    except:
                        log.error('    | [ERROR] Cannot load the json file %s' % dir+'/json/'+filename)
                        results['ecount'] += 1
                        continue
            else:
                results['ecount'] += 1
                continue

            # get dataset name from filename (a uuid generated identifier):
            ds_id = os.path.splitext(filename)[0]
            
            logger.info('    | u | %-4d | %-40s |' % (fcount,ds_id))
            
            # get OAI identifier from json data extra field 'oai_identifier':
            oai_id  = None
            for extra in jsondata['extras']:
                if(extra['key'] == 'oai_identifier'):
                    oai_id = extra['value']
                    break
            logger.debug("        |-> identifier: %s\n" % (oai_id))
            
            ### VALIDATE JSON DATA
            if (UP.validate(jsondata) < 1):
                logger.info('        |-> Upload is aborted')
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

            
            ### CHECK STATE OF DATASET IN CKAN AND EPIC:
            # status of data set
            dsstatus="unknown"
     
            # check against handle server EPIC
            epicstatus="unknown"
            if (options.epic_check):
                pid = credentials.prefix + "/eudat-jmd_" + ds_id
                checksum2 = ec.getValueFromHandle(pid,"CHECKSUM")
                ManagerVersion2 = ec.getValueFromHandle(pid,"JMDVERSION")
                B2findHost = ec.getValueFromHandle(pid,"B2FINDHOST")

                if (checksum2 == None):
                    logger.debug("        |-> Can not access pid %s to get checksum" % (pid))
                    epicstatus="new"
                elif ( checksum == checksum2) and ( ManagerVersion2 == ManagerVersion ) and ( B2findHost == options.iphost ) :
                    logger.debug("        |-> checksum, ManagerVersion and B2FIND host of pid %s not changed" % (pid))
                    epicstatus="unchanged"
                else:
                    logger.debug("        |-> checksum, ManagerVersion or B2FIND host of pid %s changed" % (pid))
                    epicstatus="changed"
                dsstatus=epicstatus

            # check against CKAN database
            ckanstatus = 'unknown'                  
            if (options.ckan_check == 'True'):
                ckanstatus=UP.check_dataset(ds_id,checksum)
                if (dsstatus == 'unknown'):
                    dsstatus = ckanstatus

            upload = 0
            # depending on status from epic handle upload record to B2FIND 
            logger.info('        |-> Dataset is [%s]' % (dsstatus))
            if ( dsstatus == "unchanged") : # no action required
                logger.info('        |-> %s' % ('No upload required'))
            else:
                upload = UP.upload(ds_id,dsstatus,community,jsondata)
                if (upload == 1):
                    logger.info('        |-> Creation of %s record succeed' % dsstatus )
                elif (upload == 2):
                    logger.info('        |-> Update of %s record succeed' % dsstatus )
                    upload=1
                else:
                    logger.info('        |-> Upload of %s record failed ' % dsstatus )
            
            # update handle in EPIC server                                                                                  
            if (options.epic_check and upload == 1):
##HEW-T (create EPIC handle as well if upload/date failed !!! :
##HEW-T            if (options.epic_check): ##HEW and upload == 1):
                if (epicstatus == "new"):
                    logger.info("        |-> Create a new handle %s with checksum %s" % (pid,checksum))
                    npid=ec.createHandle(pid,index1,checksum)
                    ec.modifyHandle(pid,'JMDVERSION',ManagerVersion)
                    ec.modifyHandle(pid,'COMMUNITY',community)
                    ec.modifyHandle(pid,'B2FINDHOST',options.iphost)
                elif (epicstatus == "unchanged"):
                    logger.info("        |-> No action required for %s" % pid)
                else:
                    logger.info("        |-> Update checksum of pid %s to %s" % (pid,checksum))
                    ec.modifyHandle(pid,'CHECKSUM',checksum)
                    ec.modifyHandle(pid,'JMDVERSION',ManagerVersion)
                    ec.modifyHandle(pid,'COMMUNITY',community)
                    ec.modifyHandle(pid,'B2FINDHOST',options.iphost)

            results['count'] +=  upload
            
            fcount += 1
            
        uploadtime=time.time()-uploadstart
        results['time'] = uploadtime
        
        # save stats:
        UP.OUT.save_stats(community+'-'+mdprefix,subset,'u',results)

def process_delete(OUT, dir, options):
    print "###JM# Don't use this function. It is not up to date."
    return False

    # create CKAN object                       
    CKAN = B2FIND.CKAN_CLIENT(options.iphost,options.auth)
    UP = B2FIND.UPLOADER(CKAN, OUT)
    
    credentials,ec = None,None

    # create credentials
    try:
        credentials = Credentials('os','credentials_11098')
        credentials.parse()
    except Exception, err:
        logger.critical("[CRITICAL] %s Could not create credentials from credstore %s" % (err,options.epic_check))
        p.print_help()
        sys.exit(-1)
    else:
        logger.debug("Create EPIC client instance to add uuid to handle server")
        ec = EpicClient(credentials)

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

                ### CHECK STATUS OF DATASET IN CKAN AND EPIC:
                # status of data set
                dsstatus="unknown"
         
                # check against handle server EPIC
                epicstatus="unknown"
                pid = credentials.prefix + "/eudat-jmd_" + ds
                checksum2 = ec.getValueFromHandle(pid,"CHECKSUM")

                if (checksum2 == None):
                  logger.debug("        |-> Can not access pid %s to get checksum" % (pid))
                  epicstatus="new"
                else:
                  logger.debug("        |-> pid %s exists" % (pid))
                  epicstatus="exist"

                # check against CKAN database
                ckanstatus = 'unknown'                  
                ckanstatus=UP.check_dataset(ds,None)

                delete = 0
                # depending on status from epic handle delete record from B2FIND
                if ( epicstatus == "new" and ckanstatus == "new") : # no action required
                    logger.info('        |-> %s' % ('No deletion required'))
                else:
                    delete = UP.delete(ds,ckanstatus)
                    if (delete == 1):
                        logger.info('        |-> %s' % ('Deletion was successful'))
                        results['count'] +=  1
                        
                        # delete handle in EPIC server (to keep the symmetry between both servers)
                        if (epicstatus == "exist"):
                           logger.info("        |-> Delete handle %s with checksum %s" % (pid,checksum2))
                           ec.deleteHandle(pid)
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
        logger.info('Joblist:\t%s' % filename)
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
        if (subset != None) and ((len(request.split()) < 5) or  ( not request.split()[4] == subset )): ###.startswith(subset))) :
            continue
            
        reqlist.append(request.split())
        
        # check the requests out for syntax errors:
        if(process == 'harvest'):
            if not (len(reqlist[-1]) == 4 or len(reqlist[-1]) == 5):
                logger.critical('[CRITICAL] The list file "%s" has wrong number of columns in line no. %d! Either 4 or 5 columns are allowed but %d columns are found!' %(filename, l, len(reqlist[-1])))
                exit_program()
        else:
            if len(reqlist[-1]) != 5:
                logger.critical('[CRITICAL] The list file "%s" has wrong number of columns in line no. %d! Only 5 columns are allowed but %d columns are found!' %(filename, l, len(reqlist[-1])))
                exit_program()
    
    return reqlist

def options_parser(modes):
    
    descI="""           I.  Ingestion of metadata comprising                                           
              - 1. Harvesting of XML files from OAI-PMH MD provider(s)\n\t
              - 2. Converting/Mapping XML to JSON and semantic mapping of metadata to CKAN schema
              - 3. Uploading resulting JSON {key:value} dict\'s as datasets to JMD portal
"""
    p = optparse.OptionParser(
        description = """Description :                                                    
           Management of metadata within EUDAT B2FIND, i.e.    
""" + descI,
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
    p.add_option('--epic_check', 
         help="check and generate handles of CKAN datasets in handle server EPIC and with credentials as specified in given credstore file",
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
    plist=['a','h','c','m','v','u','o','d']
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
