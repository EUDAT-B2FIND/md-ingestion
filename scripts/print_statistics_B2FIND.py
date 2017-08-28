#!/usr/bin/env python

"""print_statistics_B2FIND.py  prints and sends statistics of B2FIND and.

Copyright (c) 2017 Heinrich Widmann (DKRZ)

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
import smtplib
import argparse
import urllib2, urllib, socket
import json, pprint


def ckan_action(actionreq,data,rows):
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
        request = urllib2.Request(actionreq)
        if data :
            data_string = urllib.quote(json.dumps(data))
            response = urllib2.urlopen(request,data_string)
        else :
            response = urllib2.urlopen(request)
        rta=time.time()-start
        
        assert response.code == 200
        response_dict = json.loads(response.read())
        

        # Check the contents of the response.
        assert response_dict['success'] is True
        result = response_dict['result']
        if actionreq.endswith('group_show') :
            resplen=result['package_count']
        else:
            resplen=len(result)

    except urllib2.URLError as e:
        msg = "   [URLERROR] %s " % e
        retcode = 2
    except socket.timeout as e:
        msg = "   [IOERROR] %s " % e
        retcode = 2
    except IOError as e:
        msg = "   [IOERROR] %s " % e
        retcode = 1
    except ValueError as e:
        msg = "   [IOERROR] %s " % e
        retcode = 1    #catched
    except Exception as e:
        msg = "   [IOERROR] %s " % e
        retcode = 3    #catched
    else:
        msg = '[OK]'
        retcode = 0

    return (retcode,msg,result,resplen,rta)

def main():
    B2FIND_version='2.3.1'
    CKAN_version='2.2'

    ## Get options and arguments
    args = get_args()

    if args.version :
        print ('B2FIND %s :: CKAN %s' % (B2FIND_version,CKAN_version))
        sys.exit(0)

    sys.exit(checkProbes(args))

## @timeout_decorator.timeout(args.timeout)
def checkProbes(args):

    ## Settings for CKAN client and API
    ## print 'args %s' % args

    b2find_url='http://'+args.url
    if args.port :
         b2find_url+=':'+args.port
    now = time.strftime("%Y-%m-%d") ##  %H:%M:%S")
    subject='B2FIND (%s) : General Statistics on %s\n\n' % (b2find_url,now)
    msgtxt="""From: B2FIND Maintenance <do_not_reply@dkrz.de>
To: EUDAT-B2FIND Interest Group <widmann@dkrz.de>
Subject: %s
""" % subject

    msgtxt+=subject
    ckanapi3=b2find_url+'/api/3'
    ckanapi3act=b2find_url+'/api/3/action/'
    ckan_limit=100

    start=time.time()

    suppProbes=['ListDatasets','ListCommunities','ShowGroupENES']
    if args.action == 'all' :
        probes=suppProbes
    else:
        if args.action in suppProbes :
            probes=[args.action]
        else:
            print ('Action %s is not supported' % args.action)
            sys.exit(-1)

    totretcode=0

    data_dict={}
    ## Number and list of communities
    actionreq=ckanapi3act+'group_list'
    answer = ckan_action(actionreq,data_dict,ckan_limit)
    ## print answer
    communities=answer[2]
    noOfCommunities=answer[3]
    msgtxt+='Number of Communities:\t%d\n' % noOfCommunities
    ## Number of datasets
    actionreq=ckanapi3act+'dataset_list'
    actionreq=ckanapi3act+'package_search'
    answer = ckan_action(actionreq,data_dict,ckan_limit)
    noOfDatasets=answer[2]['count']
    msgtxt+='Number of Datsets (all Communities):\t%d\n\n' % noOfDatasets


    ##sys.exit(0)
    msgtxt+=('| %-15s | %18s |\n' % ('Community','Number of Datasets'))
    msgtxt+=('|%-16s +%20s|\n' % ('----------------','--------------------'))

    for community in communities :
        data_dict={'id':community}
        actionreq=ckanapi3act+'group_show'
        data_dict={'q': 'groups:'+community}
        actionreq=ckanapi3act+'package_search'

        answer = ckan_action(actionreq,data_dict,ckan_limit)
        msgtxt+=('| %-15s | %18s |\n' % (community,answer[2]['count']))
        if answer[0] > totretcode : totretcode = answer[0]
    
    print msgtxt

    try:
        smtpObj = smtplib.SMTP('localhost')
        smtpObj.sendmail('widmann@dkrz.de', ['widmann@dkrz.de','martens@dkrz.de','thiemann@dkrz.de'], msgtxt)         
        print "Successfully sent email"
    except SMTPException:
        print "Error: unable to send email"

    outfile='/tmp/B2FIND_stat_%s' % now
    with open(outfile, 'w') as f:
        f.write(msgtxt)

    return totretcode

def get_args():
    p = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description = "Description: Performs checks according different probes and returns the appropriate messages and codes."
    )
   
    p.add_argument('--version', '-v', help="prints the B2FIND and CKAN version and exits", action='store_true')
    p.add_argument('--timeout', '-t', help="time out : After given number of seconds excecution terminates.", default=1000, metavar='INT')
    p.add_argument('--action', '-a', help="Action which has to be excecuted and checked. Supported actions are URLcheck, ListDatasets, ListCommunities, ShowGroupENES or all (default)", default='all', metavar='STRING')
    p.add_argument('--url', '-u',  help='URL of the B2FIND service, to which probes are submitted (default is b2find.eudat.eu)', default='b2find.eudat.eu', metavar='URL')
    p.add_argument('--port', '-p',  help='(Optional) Port of the B2FIND service, to which probes are submitted (default is None)', default=None, metavar='URL')
##    p.add_argument('pattern',  help='CKAN search pattern, i.e. by logical conjunctions joined field:value terms.', default='*:*', metavar='PATTERN', nargs='*')
    
    args = p.parse_args()
    
    return args
               
if __name__ == "__main__":
    main()
