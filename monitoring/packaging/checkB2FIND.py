#!/usr/bin/env python

"""checkB2FIND.py  performs checks according different probes and 
returns the appropriate messages and codes.

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
PY2 = sys.version_info[0] == 2
if PY2:
    from urllib.parse import quote
    from urllib.request import urlopen, Request
    from urllib.error import HTTPError, URLError
else:
    from urllib import parse
    from urllib.request import urlopen, Request
    from urllib.error import HTTPError,URLError


import socket
import simplejson as json


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
    resplen=''
    try:
        start=time.time()
        if (urlopen(url, timeout=1).getcode() < 501):
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
        encoding='utf8'
        ## print('actionreq %s' % actionreq)            
        ## print('data %s' % data)
        data_string = json.dumps(data).encode(encoding)
        ## print('data_string %s' % type(data_string))            
        request = Request(actionreq,data_string)
        ## print('Request %s' % request)         
        response = urlopen(request)
        ## print('response %s' % response)            
        rta=time.time()-start
        
        ### assert response.code == 200
        result = json.loads(response.read())['result']
        ## print('result %s' % result)

        # Check the contents of the response.
        ##assert response_dict['success'] is True
        if actionreq.endswith('group_show') :
            resplen=result['package_count']
        else:
            resplen=len(result)

    except (URLError,socket.timeout,IOError,ValueError,Exception) as e:
        msg = "%s : %s - %s" % (e,actionreq,data_string)
        retcode = 2
    else:
        msg = '[OK]'
        retcode = 0

    return (retcode,msg,resplen,rta)

def main():
    B2FIND_version='2.2'
    CKAN_version='2.2'

    ## Get options and arguments
    args = get_args()

    if args.version :
        print(('B2FIND %s :: CKAN %s' % (B2FIND_version,CKAN_version)))
        sys.exit(0)

    sys.exit(checkProbes(args))

## @timeout_decorator.timeout(args.timeout)
def checkProbes(args):

    ## Settings for CKAN client and API
    ## print 'args %s' % args

    b2find_url='http://'+args.hostname
    if args.port :
         b2find_url+=':'+args.port
    print((' Check the service endpoint %s' % b2find_url))
    ckanapi3=b2find_url+'/api/3'
    ckanapi3act=b2find_url+'/api/3/action/'
    ckan_limit=100

    start=time.time()

##    print ('| %-15s | %-7s | %-20s | %-7s | %-6s |' % ('Probe','RetCode','Message','ResLength','RTA'))
##    print ('-----------------------------------------------')
    suppProbes=['URLcheck','ListDatasets','ListCommunities','ShowGroupENES']
    if args.action == 'all' :
        probes=suppProbes
    else:
        if args.action in suppProbes :
            probes=[args.action]
        else:
            print(('Action %s is not supported' % args.action))
            sys.exit(-1)

    totretcode=0
    for probe in probes :
        data_dict={}
        if probe == 'URLcheck':
            answer=check_url(b2find_url)
            ###print ('| %s | %s | %s' % (probe,answer[0],answer[1]))

        else:
            if probe == 'ListDatasets' :
                action='package_list'
            elif probe == 'ListCommunities' :
                action='group_list'
            elif probe == 'ShowGroupENES' :
                action='group_show'
                data_dict={'id':'enes'}
            actionreq=ckanapi3act+action

            answer = check_ckan_action(actionreq,data_dict,ckan_limit)

        print((' %-15s - %-7s - %-20s - %-7s - %-7.2f ' % (probe,answer[0],answer[1],answer[2],answer[3])))
        if answer[0] > totretcode : totretcode = answer[0]

    return totretcode

def get_args():
    p = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description = "Description: Performs checks according different probes and returns the appropriate messages and codes."
    )
   
    p.add_argument('--version', '-v', help="prints the B2FIND and CKAN version and exits", action='store_true')
    p.add_argument('--timeout', '-t', help="time out : After given number of seconds excecution terminates.", default=1000, metavar='INT')
    p.add_argument('--action', '-a', help="Action which has to be excecuted and checked. Supported actions are URLcheck, ListDatasets, ListCommunities, ShowGroupENES or all (default)", default='all', metavar='STRING')
    p.add_argument('--hostname', '-H',  help='Hostname or IP address of the B2FIND service, to which probes are submitted (default is b2find.eudat.eu)', default='b2find.eudat.eu', metavar='URL')
    p.add_argument('--port', '-p',  help='(Optional) Port of the B2FIND service, to which probes are submitted (default is None)', default=None, metavar='URL')
##    p.add_argument('pattern',  help='CKAN search pattern, i.e. by logical conjunctions joined field:value terms.', default='*:*', metavar='PATTERN', nargs='*')
    
    args = p.parse_args()
    
    return args
               
if __name__ == "__main__":
    main()
