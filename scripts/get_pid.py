#!/usr/bin/env python

"""get_pid.py  Select pid's by given search criteria

Copyright (c) 2014 Heinrich Widmann (DKRZ)

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""

import os, sys
import argparse
import simplejson as json
import urllib.request, urllib.parse, urllib.error, urllib.request, urllib.error, urllib.parse

def main():
    args = get_args()
    list = []
    
    print("\n%s" %('-'*100))
    # create CKAN search pattern :
    ckan_pattern = ''
    sand=''
    pattern=' AND '.join(args.pattern)
    if (args.community):
        ckan_pattern += "groups:%s" % args.community
        sand=" AND "
    if (args.pattern):
        ckan_pattern += sand + pattern   

    print('Search in\t%s\nfor pattern\t%s\n.....' % (args.ckan,ckan_pattern))
    answer = action(args.ckan, {"q":ckan_pattern,"rows":args.ckan_limit,"start":0})
    tcount=answer['result']['count']
    print("=> %d datasets found" % tcount)
    if (tcount>args.ckan_limit): print("=> but maximal %d rows are returned " % args.ckan_limit)
    ## print '    | %-4s | %-40s |\n    |%s|' % ('#','Dataset ID',"-" * 53)
    sf = open('source.file', 'w')
    pidf = open('pid.file', 'w')
    idf = open('id.file', 'w')
    countpid=0
    counter=0
    cstart=0

    while (cstart < tcount) :
       if (cstart > 0): answer = action(args.ckan, {"q":ckan_pattern,"rows":args.ckan_limit,"start":cstart})
       for ds in answer['result']['results']:
            counter +=1
            ## print'    | %-4d | %-40s |' % (counter,ds['name'])
            idf.write(ds['name']+'\n')
            sf.write(ds['url']+'\n')
            for extra in ds['extras']:
                if (extra['key'] == 'PID'):
                   pidf.write(extra['value']+'\n')
                   countpid+=1
       cstart+=len(answer['result']['results']) 

    pidf.close()
    print("Found %d records (ID's written to id.file) and %d associated PIDs (written to pid.file)" % (counter, countpid))

def action(host, data={}):
    ## action (action, jsondata) - method
    # Call the api action <action> with the <jsondata> on the CKAN instance which was defined by iphost
    # parameter of CKAN_CLIENT.
    #
    # Parameters:
    # -----------
    # (string)  action  - Action name of the API v3 of CKAN
    # (dict)    data    - Dictionary with json data
    #
    # Return Values:
    # --------------
    # (dict)    response dictionary of CKAN
	    
    return __action_api(host,'package_search', data)
	
def __action_api (host,action, data_dict):
    # Make the HTTP request for data set generation.
    response=''
    rvalue = 0
    action_url = "http://{host}/api/3/action/{action}".format(host=host,action=action)

    # make json data in conformity with URL standards
    data_string = urllib.parse.quote(json.dumps(data_dict))

    ##print('\t|-- Action %s\n\t|-- Calling %s\n\t|-- Object %s ' % (action,action_url,data_dict))	
    try:
       request = urllib.request.Request(action_url)
       response = urllib.request.urlopen(request,data_string)
    except urllib.error.HTTPError as e:
       print('\t\tError code %s : The server %s couldn\'t fulfill the action %s.' % (e.code,host,action))
       if ( e.code == 403 ):
                print('\t\tAccess forbidden, maybe the API key is not valid?')
                exit(e.code)
       elif ( e.code == 409 and action == 'package_create'):
                print('\t\tMaybe the dataset already exists or you have a parameter error?')
                action('package_update',data_dict)
                return {"success" : False}
       elif ( e.code == 409):
                print('\t\tMaybe you have a parameter error?')
                return {"success" : False}
       elif ( e.code == 500):
                print('\t\tInternal server error')
                exit(e.code)
    except urllib.error.URLError as e:
       exit('%s' % e.reason)
    else :
       out = json.loads(response.read())
       assert response.code >= 200
       return out

def get_args():
    p = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description = "Description: Get PID's of datasets that fulfill the search criteria",
        epilog =  '''Examples:
           1. > ./get_pid.py -c aleph tags:LEP
             searchs for all datasets of community ALEPH with tag "LEP".
           2. >./get_pid.py tags:PUBLICATIONOTHER author:'"Ahn, Changhyun"'
             searchs for all datasets tagged with PUBLICATIONOTHER and with author "Ahn, Changhyan"'''
    )
   
    p.add_argument('--ckan',  help='CKAN portal address, to which search requests are submitted (default is b2find.eudat.eu)', default='b2find.eudat.eu', metavar='IP/URL')
    p.add_argument('--community', '-c', help="Community where you want to search in", default='', metavar='STRING')
    p.add_argument('--ckan_limit',  help='Limit of listed datasets (default is 1000)', default=1000, type=int, metavar='INTEGER')
    p.add_argument('pattern',  help='CKAN search pattern, i.e. (a list of) field:value terms.', metavar='PATTERN', nargs='*')
    
    args = p.parse_args()
    
    if (not args.pattern) and (not args.community) :
        print("[ERROR] Need at least a community given via option -c or a search pattern as an argument!")
        exit()
    
    return args
               
if __name__ == "__main__":
    main()
