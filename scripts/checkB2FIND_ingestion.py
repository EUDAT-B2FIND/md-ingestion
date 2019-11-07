#!/usr/bin/env python

"""checkB2FIND_ingestion.py  checks ingestion status.

Copyright (c) 2016 Heinrich Widmann (DKRZ)

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""
import logging
import traceback
import os, sys, io, time
import argparse
import timeout_decorator
import socket
import json, pprint
import xlsxwriter
import time
import re

PY2 = sys.version_info[0] == 2
if PY2:
    from urllib import quote
    from urllib2 import urlopen, Request
    from urllib2 import HTTPError,URLError
else:
    from urllib import parse
    from urllib.request import urlopen, Request
    from urllib.error import HTTPError,URLError

def check_url(url):
    # Checks and validates a url via urllib module
    #
    # Parameters:
    # -----------
    # (url)  url - Url to check
    #
    # Return Values:
    # --------------
    # 1. (boolean)  result

    rta=0
    resplen='--'
    try:
        start=time.time()
        request = Request(url)
        if (urlopen(request, timeout=1).getcode() < 501):
            msg='[OK]'
            retcode=0
        rta=time.time()-start
        
    except URLError as e:
        ## msg="    %s and %s" % (e,traceback.format_exc())
        msg="    [URLError] %s" % e
        retcode = 2    #catched
    except socket.timeout as e:
        msg="    [Socket Timeout] %s" % e
        retcode = 2    #catched
    except IOError as e:
        msg="    [IOError] %s" % e
        retcode = 1
    except ValueError as e:
        msg="    [ValueError] %s" % e
        retcode = 1    #catched
    except Exception as e:
        msg="    [Unknown Error] %s" % e
        retcode = 3    #catched
    else:
        msg = '[OK]'
        retcode = 0

    return (retcode,msg,resplen,rta)

def check_ckan_action(actionreq,data,rows):
    # Checks and validates a request or action submitted to CKAN
    #
    # Parameters:
    # -----------
    # (url)  url - Url to check
    #
    # Return Values:
    # --------------
    # 1. (boolean)  result

    resplen=0
    rta=0
    try:
        start=time.time()
##HEW-D        request = Request(actionreq)
        if data :
            if PY2 :
                data_string = quote(json.dumps(data))
            else :
                data_string = parse.quote(json.dumps(data)).encode('utf-8')
            request = Request(actionreq,data_string)
            response = urlopen(request)
        else :
            request = Request(actionreq)
            response = urlopen(request)
        rta=time.time()-start


    except URLError as e:
        msg = " [URLERROR] %s " % e
        retcode = 2
    except socket.timeout as e:
        msg = " [TIMEOUT] %s " % e
        retcode = 2
    except IOError as e:
        msg = " [IOError] %s " % e
        retcode = 1
    except ValueError as e:
        msg = " [ValueError] %s " % e
        retcode = 1    #catched
    except Exception as e:
        msg = "  [Error] %s " % e
        retcode = 3    #catched
    else:
        msg = '[OK]'
        retcode = 0
        assert response.code == 200
        ###print('response %s' % response.read()) 
        if PY2 :
            response_dict = json.loads(response.read())
        else:
            response_dict = response.read()
        print('ttt response_dict %s' % type(response_dict))        

        # Check the contents of the response.
        if type(response_dict) == 'dict' and 'success' in response_dict :
            assert response_dict['success'] is True
        ## print('response.code %s' % response.code)
        if type(response_dict) == 'dict' and 'result' in response_dict :
            result = response_dict['result']
            if actionreq.endswith('group_show') :
                resplen=result['package_count']
            else:
                resplen=len(result)

    return (retcode,msg,resplen,rta)

def main():
    B2FIND_version='2.2'
    CKAN_version='2.2'

    ## Get options and arguments
    args = get_args()

    if args.version :
        print ('B2FIND %s :: CKAN %s' % (B2FIND_version,CKAN_version))
        sys.exit(0)

    sys.exit(checkStatus(args))

## @timeout_decorator.timeout(args.timeout)
def checkStatus(args):

    now = time.strftime("%y%m%d")
    ## Harvested records 
    community = args.community
    mdprefix = args.mdprefix
    subset = args.mdsubset
    base_outdir = args.outdir

    # Read harvest file and get harvest endpoint, mdprefixes and (OAI) sets  
    harvestfile='%s' % 'harvest_list'
    hdict=dict()
    oldset=''
    setlist=list()
    with open(harvestfile, 'r') as f:
        for line in f:
            if re.match(community+"(.*)", line):
                splline=line.split()
                if splline[4] != oldset :
                    setlist.append(splline[4])
                if splline[1] not in hdict :
                    mdflist=[{ splline[3] : setlist}]
                    hdict[splline[1]]=mdflist
                else :
                    if splline[3] != oldmdf :
                        mdflist.append(splline[3])
                    hdict[oldcomm]=mdflist
                oldset=splline[4]
                oldmdf=splline[3]
                oldcomm=splline[1]
                print(splline,)

    ## print(hdict)

    # Create an new Excel file and add a worksheet.
    workbook = xlsxwriter.Workbook('communities/' + community+'-b2find.xlsx')


    hsheet = workbook.add_worksheet('Harvesting ('+now+')')

    # Write HarestInfo Info.
    hsheet.write('A2', 'URL endpoints')
    hsheet.write('B2', '1')
    hsheet.write('A3', 'Metadata Schemas')
    hsheet.write('B3', '2' )
    hsheet.write('A4', 'Subset')
    hsheet.write('B4', '236' )



    if len(hdict) == 1 : # only one endpoint
        print('only one endpoint')
    else:
        print('%d endpoints ' % len(hdict))


    for endp in hdict:
        if len(hdict[endp][0]) == 1 : # only one mdprefix
            print('only one mdrefix %s' % list(list(hdict[endp])[0])[0])
            tmdprefix=list(list(hdict[endp])[0])[0]
            fsubset = hdict[endp][0][tmdprefix][0]+'_1'

    commmdprpath='/'.join([base_outdir,community+'-'+mdprefix])

    if args.mdsubset :
        subsets = [args.mdsubset]
    else :
        subsets = next(os.walk(commmdprpath))[1]
    hcount=0 ; mcount=0 ;
    htot=0 ; mtot=0 ;
    fieldCount=9
    print ('| %-20s | %5s | %10s | %8s | %9s | %6s |' % ('Subset','Description','Issues','Harvested','Mapped','Uploaded'))
    for subset in subsets :
        fieldCount+=1
        nextfield='A'+str(fieldCount)
        hsheet.write(nextfield,subset)
        inpath = '/'.join([base_outdir,community+'-'+mdprefix,subset])
        if os.path.isdir(inpath+'/xml'):
            hfiles = filter(lambda x: x.endswith('.xml'), os.listdir(inpath+'/xml'))
            hcount=len(list(hfiles))
            htot+=hcount
            nextfield='F'+str(fieldCount)
            hsheet.write(nextfield,hcount)
        if os.path.isdir(inpath+'/json'):
            mfiles = filter(lambda x: x.endswith('.json'), os.listdir(inpath+'/json'))
            mcount=len(list(mfiles))
            mtot+=mcount
            nextfield='G'+str(fieldCount)
            hsheet.write(nextfield,mcount)
        print ('| %20s | %5s | %10s | %8d | %9d | %6d |' % ( subset,'blabla...','Errors :...',hcount,mcount,10000) )

    print ('| %20s | %5s | %10s | %8d | %9d | %6d |' % ('Sum','','',htot,mtot,10000) )


    msheet = workbook.add_worksheet('Map '+mdprefix+' ('+now+')')

    # Write Community Info.
    msheet.write('A1', 'Community')
    msheet.write('A2', community)
    msheet.write('B1', 'Subset')
    msheet.write('B2', subset )
    msheet.write('C1', 'MDFormat')
    msheet.write('C2', mdprefix )


    # Read stat file and write XPATH rules and Coverage in xlsx file  
    statfile='%s/%s/%s/%s' % (base_outdir,community+'-'+mdprefix,subset,'validation.stat')
    if not os.path.exists(statfile):
        print('Can not access %s' % statfile)
        ##sys.exit(-1)

    with open(statfile, 'r') as f:
        fieldCount=13
        for line in f.readlines() :
            linearr=line.split()
            if len(linearr) > 1:
                if linearr[0] == '|->' :
                    fieldCount+=1
                    nextfield='E'+str(fieldCount)
                    xpath=linearr[3] if len(linearr) > 3 else ''
                    msheet.write(nextfield,xpath)
                    nextfield='B'+str(fieldCount)
                    fieldname=linearr[1] if len(linearr) > 1 else ''
                    msheet.write(nextfield,fieldname)
                elif linearr[0] == '|--' :
                    ##fieldCount+=1
                    nextfield='G'+str(fieldCount)
                    perc=linearr[3] if len(linearr) > 3 else ''
                    msheet.write(nextfield,perc)

    workbook.close()

    return 0

def checkProbes(args):

    ## Settings for CKAN client and API
    ## print 'args %s' % args

    b2find_url='http://'+args.url
    if args.port :
         b2find_url+=':'+args.port
    print (' Check the service endpoint %s' % b2find_url)
    ckanapi3=b2find_url+'/api/3'
    ckanapi3act=b2find_url+'/api/3/action/'
    ckan_limit=100

    start=time.time()

    print ('| %-15s | %-7s | %-20s | %-7s | %-6s |' % ('Probe','RetCode','Message','ResLength','RTA'))
    print ('-----------------------------------------------')
    suppProbes=['URLcheck','ListDatasets','ListCommunities','ShowGroupENES']
    if args.action == 'all' :
        probes=suppProbes
    else:
        if args.action in suppProbes :
            probes=[args.action]
        else:
            print ('Action %s is not supported' % args.action)
            sys.exit(-1)

    totretcode=0
    for probe in probes :
        data_dict={}
        if probe == 'URLcheck':
            answer=check_url(b2find_url)
            ###print ('| %s | %s | %s' % (probe,answer[0],answer[1]))

        else:
            if probe == 'ListDatasets' :
                action='dataset_list'
            elif probe == 'ListCommunities' :
                action='group_list'
            elif probe == 'ShowGroupENES' :
                action='group_show'
                data_dict={'id':'enes'}
            actionreq=ckanapi3act+action

            answer = check_ckan_action(actionreq,data_dict,ckan_limit)

        print ('| %-15s | %-7s | %-20s | %-7s | %-7.2f | ' % (probe,answer[0],answer[1],answer[2],answer[3]))
        if answer[0] > totretcode : totretcode = answer[0]

    return totretcode

def get_args():
    p = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description = "Description: Performs checks according different probes and returns the appropriate messages and codes."
    )
   
    p.add_argument('--version', '-v', help="prints the B2FIND and CKAN version and exits", action='store_true')
    p.add_argument('--timeout', '-t', help="time out : After given number of seconds excecution terminates.", default=1000, metavar='INT')
    p.add_argument('--outdir', '-o', help="The relative root dir in which all harvested files will be saved. The converting and the uploading processes work with the files from this dir. (default is 'oaidata')",default='oaidata', metavar='PATH')
    p.add_argument('--community', '-c', help="community where data harvested from and uploaded to", metavar='STRING')
    p.add_argument('--mdsubset', help="Subset of harvested meta data",default=None, metavar='STRING')
    p.add_argument('--mdprefix', help="Prefix of harvested meta data",default=None, metavar='STRING')
    p.add_argument('--action', '-a', help="Action which has to be excecuted and checked. Supported actions are URLcheck, ListDatasets, ListCommunities, ShowGroupENES or all (default)", default='all', metavar='STRING')
    p.add_argument('--url', '-u',  help='URL of the B2FIND service, to which probes are submitted (default is b2find.eudat.eu)', default='b2find.eudat.eu', metavar='URL')
    p.add_argument('--port', '-p',  help='(Optional) Port of the B2FIND service, to which probes are submitted (default is None)', default=None, metavar='URL')
##    p.add_argument('pattern',  help='CKAN search pattern, i.e. by logical conjunctions joined field:value terms.', default='*:*', metavar='PATTERN', nargs='*')
    
    args = p.parse_args()
    
    return args
               
if __name__ == "__main__":
    main()
