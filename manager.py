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
Modified by  c/o DKRZ 2018   Heinrich Widmann
  Further modularization and redesign
"""

##from __future__ import print_function

##import classes from B2FIND modules
from generating import Generator
from harvesting import Harvester
from mapping import Mapper
from converting import Converter
from validating import Validator
from uploading import Uploader, CKAN_CLIENT
from output import Output
import settings

##Py3???
from b2handle.clientcredentials import PIDClientCredentials
from b2handle.handleclient import EUDATHandleClient
from b2handle.handleexceptions import GenericHandleError
import os, optparse, sys, glob, re
PY2 = sys.version_info[0] == 2

from subprocess import call,Popen,PIPE
import time, datetime
import simplejson as json
import copy

import logging
import traceback
import codecs
import pprint

def main():
    # initialize global settings
    settings.init()

    # parse command line options and arguments:
    modes=['a','g','generate','h','harvest','m','map','v','validate','c','convert','u','upload','h-m','m-u','h-u', 'h-d', 'd','delete']
    p = options_parser(modes)
    global options
    options,arguments = p.parse_args()
    
    # check option 'mode' and generate process list:
    (mode, pstat) = pstat_init(p,modes,options.mode,options.source,options.iphost)
    
    # set now time and process id
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    jid = os.getpid()

    # Output instance
    OUT = Output(pstat,now,jid,options)

    ## logger
    global logger 
    logger = OUT.setup_custom_logger('root',options.verbose)

    ## logger = logging.getLogger('root')
    ##HEW-D logging.basicConfig(format=log_format, level=log_level) ### logging.DEBUG)
    # print out general info:
    logger.info('B2FIND Version:  \t%s' % settings.B2FINDVersion)
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
        sys.exit()
    except Exception as err :
        logging.critical("%s" % err)
        logging.error("%s" % traceback.format_exc())
        sys.exit()
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

    logger.debug(' |- Requestlist:  \t%s' % reqlist)

    ## check job request (processing) options
    logger.debug('|- Command line options')
    for opt in procOptions :
        if hasattr(options,opt) : logger.debug(' |- %s:\t%s' % (opt.upper(),getattr(options,opt)))
    
    ## GENERATION mode:    
    if (pstat['status']['g'] == 'tbd'):
        logger.info('\n|- Generation started : %s' % time.strftime("%Y-%m-%d %H:%M:%S"))
        GEN = Generator(OUT,pstat,options.outdir)
        process_generate(GEN,reqlist)

    ## HARVESTING mode:    
    if (pstat['status']['h'] == 'tbd'):
        logger.info('\n|- Harvesting started : %s' % time.strftime("%Y-%m-%d %H:%M:%S"))
        HV = Harvester(OUT,pstat,options.outdir,options.fromdate,options.verify_ssl)
        process_harvest(HV,reqlist)

    ## MAPPINING - Mode:  
    if (pstat['status']['m'] == 'tbd'):
        logger.info('\n|- Mapping started : %s' % time.strftime("%Y-%m-%d %H:%M:%S"))
        MP = Mapper(OUT,options.outdir,options.fromdate)
        process_map(MP,reqlist)

    ## VALIDATING - Mode:
    if (pstat['status']['v'] == 'tbd'):
        logger.info(' |- Validating started : %s' % time.strftime("%Y-%m-%d %H:%M:%S"))
        VD = Validator(OUT,options.outdir,options.fromdate)
        process_validate(VD,reqlist)

    ## CONVERTING - Mode:  
    if (pstat['status']['c'] == 'tbd'):
        logger.info('\n|- Converting started : %s' % time.strftime("%Y-%m-%d %H:%M:%S"))
        CV = Converter(OUT,options.outdir,options.fromdate)
        process_convert(CV, reqlist)

    ## UPLOADING - Mode:  
    if (pstat['status']['u'] == 'tbd'):
        logger.info('\n|- Uploading started : %s' % time.strftime("%Y-%m-%d %H:%M:%S"))
        # create CKAN object                       
        CKAN = CKAN_CLIENT(options.iphost,options.auth)
        # create credentials and handle client if required
        if (options.handle_check):
          try:
              cred = PIDClientCredentials.load_from_JSON('credentials_11098')
          except Exception as err:
              logger.critical("%s : Could not create credentials from credstore %s" % (err,options.handle_check))
              ##p.print_help()
              sys.exit(-1)
          else:
              logger.debug("Create EUDATHandleClient instance")
              HandleClient = EUDATHandleClient.instantiate_with_credentials(cred)
        else:
            cred=None
            HandleClient=None

        UP = Uploader(CKAN,options.ckan_check,HandleClient,cred,OUT,options.outdir,options.fromdate,options.iphost)
        logger.info(' |- Host:  \t%s' % CKAN.ip_host )
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

def process_generate(GEN, rlist):
    ## process_generate (GENERATOR object, rlist) - function
    # Generates per request.
    #
    # Parameters:
    # -----------
    # (object)  GENERATOR - object from the class GENERATEER
    # (list)    rlist - list of request lists 
    #
    # Return Values:
    # --------------
    # None
    ir=0
    for request in rlist:
        ir+=1
        generatestart = time.time()
        print ('   |# %-4d : %-30s %-10s \n\t|- %-10s |@ %-10s |' % (ir,request,'Started',request[2],time.strftime("%H:%M:%S")))
        results = GEN.generate(request)
    
        if (results == -1):
            logger.error("Couldn't generate from %s" % request)

        generatetime=time.time()-generatestart

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

        if len(request)>4 :
            request[4] = request[4] if (not request[4].startswith('#')) else None
        else :
            request.append(None)

        print ('   |# %-4d : %-30s %-10s \n\t|- %-10s |@ %-10s |' % (ir,request,HV.fromdate,'Started',time.strftime("%H:%M:%S")))
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
        
        cstart = time.time()

        print ('   |# %-4d : %-10s\t%-20s \n\t|- %-10s |@ %-10s |' % (ir,request[0],request[3:5],'Started',time.strftime("%H:%M:%S")))
        results = MP.map(request)

        ctime=time.time()-cstart
        results['time'] = ctime
        
        # save stats:
        if len(request) > 4:
            MP.OUT.save_stats(request[0]+'-' + request[3], request[4],'m',results)
        else:
            MP.OUT.save_stats(request[0]+'-' + request[3],'SET_1','v',results)


def process_validate(VD, rlist):
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
        cstart = time.time()

        target=None

        print ('   |# %-4d : %-10s\t%-20s \n\t|- %-10s |@ %-10s |' % (ir,request[0],request[2:5],'Started',time.strftime("%H:%M:%S")))
        
        results = VD.validate(request,target)

        ctime=time.time()-cstart
        results['time'] = ctime
        
        # save stats:
        if len(request) > 4:
            VD.OUT.save_stats(request[0]+'-' + request[3],request[4],'v',results)
        else:
            VD.OUT.save_stats(request[0]+'-' + request[3],'SET_1','v',results)
        
def process_upload(UP, rlist):

    CKAN = UP.CKAN
    last_community = ''
    package_list = dict()

    ir=0
    for request in rlist:
        ir+=1
        print ('   |# %-4d : %-10s\t%-20s \n\t|- %-10s |@ %-10s |' % (ir,request[0],request[2:5],'Started',time.strftime("%H:%M:%S")))
        community=request[0]
        mdprefix = request[3]
        
        try:
            ckangroup=CKAN.action('group_list')
            if community not in ckangroup['result'] :
                logger.critical('Can not find community %s' % community)
                sys.exit(-1)
        except Exception :
            logging.critical("Can not list communities (CKAN groups)")
            sys.exit(-1)
  

        if (last_community != community) :
            last_community = community
            if options.ckan_check == 'True' :
                UP.get_packages(community)
            if options.clean == 'True' :
                delete_file = '/'.join([UP.base_outdir,'delete',community+'-'+mdprefix+'.del'])
                if os.path.exists(delete_file) :
                    logging.warning("All datasets listed in %s will be removed" % delte_file)
                    with open (delete_file,'r') as df :
                        for id in df.readlines() :
                            UP.delete(id,'to_delete')


        ##HEW-D-Test sys.exit(0)

        uploadstart = time.time()


        cstart = time.time()
        
        results = UP.upload(request)

        ctime=time.time()-cstart
        results['time'] = ctime
        
        # save stats:
        if len(request) > 4:
            UP.OUT.save_stats(request[0]+'-'+request[3],request[4],'u',results)
        else:
            UP.OUT.save_stats(request[0]+'-'+request[3],'SET_1','u',results)
        

def process_convert(CV, rlist):

    ir=0
    for request in rlist:
        ir+=1
        print ('   |# %-4d : %-10s\t%-20s --> %-10s\n\t|- %-10s |@ %-10s |' % (ir,request[0],request[2:5],request[5],'Started',time.strftime("%H:%M:%S")))
        rcstart = time.time()
        
        results = CV.convert(request)

        rctime=time.time()-rcstart
        results['time'] = rctime
        
        # save stats:
        CV.OUT.save_stats(request[0]+'-'+ request[3],request[4],'c',results)


def process_delete(OUT, dir, options):
    print ("###JM# Don't use this function. It is not up to date.")
    return False

    # create CKAN object                       
    CKAN = CKAN_CLIENT(options.iphost,options.auth)
    UP = Uploader(CKAN,OUT,options.outdir)
    
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
        if((options.community != None) and (not request.split()[0] == options.community)):
            continue

        reqarr=request.split()
        # sort out lines that don't match given mdprefix
        if (options.mdprefix != None):
            if ( not reqarr[3] == options.mdprefix) :
              continue

        # sort out lines that don't match given subset
        if (options.mdsubset != None):
            if len(reqarr) < 5 :
                reqarr.append(options.mdsubset)
            elif ( reqarr[4] == options.mdsubset.split('_')[0] ) :
                reqarr[4] = options.mdsubset
            elif not ( options.mdsubset.endswith('*') and reqarr[4].startswith(options.mdsubset.translate(None, '*'))) :
                continue
                
        if (options.target_mdschema != None and not options.target_mdschema.startswith('#')):
            if len(reqarr) < 6 :
                print('reqarr %s' % reqarr)
                reqarr.append(options.target_mdschema)
        elif len(reqarr) > 5 and reqarr[5].startswith('#') :
            del reqarr[5:]

        logging.debug('Next request : %s' % reqarr)
        reqlist.append(reqarr)
        
    if len(reqlist) == 0:
        logging.critical(' No matching request found in %s\n\tfor options %s' % (filename,options) )
        exit()
 
    return reqlist

def options_parser(modes):
    
    p = optparse.OptionParser(
        description = '''Description                                                              
===========                                                                           
 Management of metadata within EUDAT B2FIND, comprising                               - Generation of formated XML records from raw metadata sets \n\t                       
      - Harvesting of XML files from OAI-PMH MD provider(s)\n\t

              - Mapping XML to JSON and semantic mapping of metadata to B2FIND schema\n\t

\n            - Validation of the JSON records and create coverage statistics\n\t
              - Uploading resulting JSON {key:value} dict\'s as datasets to the B2FIND portal\n\t
              - OAI compatible creation of XML records in oai_b2find format\n\t
    
''',
        formatter = optparse.TitledHelpFormatter(),
        prog = 'manager.py',
        epilog='For any further information and documentation please look at the README.md file or at the EUDAT wiki (http://eudat.eu/b2find).',
        version = "%prog " + settings.B2FINDVersion,
        usage = "%prog [options]" 
    )

    p.add_option('-v', '--verbose', action="count", 
                        help="increase output verbosity (e.g., -vv is more than -v)", default=False)
    p.add_option('--outdir', '-o', help="The relative root dir in which all harvested files will be saved. The converting and the uploading processes work with the files from this dir. (default is 'oaidata')",default='oaidata', metavar='PATH')
    p.add_option('--mode', '-m', metavar='PROCESSINGMODE', help='\nThis can be used to do a partial workflow. Supported modes are (g)enerating, (h)arvesting, (c)onverting, (m)apping, (v)alidating, (o)aiconverting and (u)ploading or a combination. default is h-u, i.e. a total ingestion', default='h-u')
    p.add_option('--community', '-c', help="community where data harvested from and uploaded to", metavar='STRING')
    p.add_option('--fromdate', help="Filter harvested files by date (Format: YYYY-MM-DD).", default=None, metavar='DATE')
    p.add_option("--no-ssl-verify", action="store_false", dest="verify_ssl", default=True, help="Do not verify SSL certificates")
         
    group_multi = optparse.OptionGroup(p, "Multi Mode Options",
        "Use these options if you want to ingest from a list in a file.")
    group_multi.add_option('--list', '-l', help="list of OAI harvest sources (default is ./ingestion_list)", default='ingestion_list',metavar='FILE')
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
    group_upload.add_option('--auth', help="Authentification for CKAN APIs (API key, by default taken from file $HOME/.netrc)",metavar='STRING')
    group_upload.add_option('--handle_check', 
         help="check and generate handles of CKAN datasets in handle server and with credentials as specified in given credstore file", default=None,metavar='FILE')
    group_upload.add_option('--ckan_check',
         help="check existence and checksum against existing datasets in CKAN database", default='False', metavar='BOOLEAN')
    group_upload.add_option('--clean',
         help="Clean CKAN from datasets listed in delete file", default='False', metavar='BOOLEAN')
    
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
    plist=['g','h','m','v','u','c','d']
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
       
    pstat['text']['g']='Generate XML files from ' + stext 
    pstat['text']['h']='Harvest XML files from ' + stext 
    pstat['text']['m']='Map community XML to B2FIND JSON and do semantic mapping'  
    pstat['text']['v']='Validate JSON records against B2FIND schema'  
    pstat['text']['c']='Convert JSON to (e.g. CERA) XML'  
    pstat['text']['u']='Upload JSON records as datasets into B2FIND %s' % iphost
    pstat['text']['d']='Delete B2FIND datasets from %s' % iphost
    
    pstat['short']['h-u']='TotalIngestion'
    pstat['short']['g']='Generating'
    pstat['short']['h']='Harvesting'
    pstat['short']['m']='Mapping'
    pstat['short']['v']='Validating'
    pstat['short']['c']='Converting'
    pstat['short']['u']='Uploading'
    pstat['short']['d']='Deletion'
    
    return (mode, pstat)

def exit_program (OUT, message=''):
    # stop the total time:
    OUT.save_stats('#Start','subset','TotalTime',time.time()-settings.TimeStart)

    # print results with OUT.HTML_print_end() in a .html file:
    OUT.HTML_print_end()

    if (OUT.options.verbose != False):
      logging.debug('For more info open HTML file %s' % OUT.jobdir+'/overview.html')

if __name__ == "__main__":
    main()
