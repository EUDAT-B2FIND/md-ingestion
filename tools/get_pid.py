#!/usr/bin/env python

"""get_pid.py  Select pid's (and irods links) by given search criteria

Copyright (c) 2014 Heinrich Widmann (DKRZ)

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""

import os, sys, re
import optparse
from lxml import etree

# add parent directory to python library searching paths
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import B2FIND

def main():
    options, args = get_options()
    list = []
    
    print("\n%s" %('-'*100))
        
    CKAN = B2FIND.CKAN_CLIENT(options.ckan,None)
        
    # create pattern for CKAN:
    ckan_pattern = ''
    sand=''
    pattern=' '.join(args)
    if (options.community):
        ckan_pattern += "groups:%s" % options.community
        sand=" AND "
    if (pattern):
        ckan_pattern += sand + pattern   

    print('Search in\t%s\nfor pattern\t%s\n.....' % (options.ckan,ckan_pattern))
    answer = CKAN.action('package_search', {"q":ckan_pattern,"rows":options.ckan_limit,"start":0})
    tcount=answer['result']['count']
    print("=> %d datasets found" % tcount)
    if (options.ckan_limit>tcount): print("=> but maximal %d rows are returned " % options.ckan_limit)
    print("=> search_facets " % (answer['result']['search_facets']))
    print('    | %-4s | %-40s |\n    |%s|' % ('#','Dataset ID',"-" * 53))
    urlf = open('url.file', 'w')
    pidf = open('pid.file', 'w')
    countpid=0
    counter=0
    cstart=0
    ##print 'ss %s' % answer['result']
    while (cstart < tcount) :
       if (cstart >0): answer = CKAN.action('package_search', {"q":ckan_pattern,"rows":options.ckan_limit,"start":cstart})
       # print results:
       ## print 'len %d' % len(answer['result']['results'])
       for ds in answer['result']['results']:
            counter +=1
            ## print'    | %-4d | %-40s |' % (counter,ds['name'])
            ##print '    title: %s' % ds['title']
            ##print '    Source: %s' % ds['url']

            urlf.write(ds['url']+'\n')

            for extra in ds['extras']:
                if (extra['key'] == 'PID'):
                   ##print '    PID : %s' % extra['value']
                   pidf.write(extra['value']+'\n')
                   countpid+=1
                ##else:
                ##   print 'Can\'t access field PID'
       cstart+=len(answer['result']['results']) 
    urlf.close()
    pidf.close()
    print("Found %d records and %d PIDs" % (counter, countpid))



def get_options():
    p = optparse.OptionParser(
        description = "Description: Get PID's or/and URL's that fulfill search criteria",
        prog = 'get_pid.py',
        usage = '%prog [ OPTIONS ] PATTERN\n\tPATTERN is the CKAN search pattern, i.e. (a list of) field:value terms.'
    )
   
    p.add_option('--ckan',  help='CKAN portal address, to which search requests are submitted (default is eudat6b.dkrz.de)', default='eudat6b.dkrz.de', metavar='IP/URL')
    p.add_option('--community', '-c', help="Community where you want to search in", default='', metavar='STRING')
    p.add_option('--ckan_limit',  help='Limit of listed datasets (default is 1000)', default=1000, type='int', metavar='INTEGER')
    
    options, args = p.parse_args()
    
    if (len(args) != 1) and (not options.community) :
        print("[ERROR] Need at least a community given via option -c or a search pattern as an argument!")
        exit()
    
    return options, args
               
if __name__ == "__main__":
    main()
