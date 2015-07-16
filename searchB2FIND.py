#!/usr/bin/env python

"""searchB2FIND.py  performs search request in the B2FIND metadata catalogue

Copyright (c) 2015 Heinrich Widmann (DKRZ)

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
import urllib, urllib2
from tables import *

def main():
    args = get_args()
    list = []
    
    print "\n%s" %('-'*100)
    # create CKAN search pattern :
    ckan_pattern = ''
    sand=''
    pattern=' '.join(args.pattern)

    if (args.community):
        ckan_pattern += "groups:%s" % args.community
        sand=" AND "
    if (args.pattern):
        ckan_pattern += sand + pattern   

    print 'Search\n\tin\t\t%s\n\tfor pattern\t%s\n' % (args.ckan,ckan_pattern)

    ckan_limit=10000
    answer = action(args.ckan, {"q":ckan_pattern,"rows":ckan_limit,"start":0})
    tcount=answer['result']['count']
    print " => %d datasets found" % tcount
    aids=args.ids
    ## print '    | %-4s | %-40s |\n    |%s|' % ('#','Dataset ID',"-" * 53)
    suppid={'id':'id','Source':'url','PID':'PID','DOI':'DOI','Group':'groups'}

    class Record(IsDescription):
        id      = StringCol(64)      # 64-character String
        Source  = StringCol(64)      # 64-character String
        PID     = StringCol(64)      # 64-character String
        DOI     = StringCol(64)      # 64-character String

    extension=os.path.splitext(args.output)[1][1:]
    if extension == 'hd5': 
      h5file = open_file(args.output, mode = "w", title = "Search results")
      print "Results written to %s" % args.output
      group = h5file.create_group("/", 'identifiers', 'Identification information')
      table = h5file.create_table(group, 'readout', Record, "Readout example")
      record = table.row
    elif extension == 'txt':
      print " => %s : %s written to %s" % ('id',aids,args.output)
      fh = open(args.output, "w")
      record={} 
    else:
      print 'Output type %s is not supported' % extension
      exit()
            
  
    totlist=[]
    for outt in aids:
       if outt not in suppid :
           print 'Output identifier %s is not supported' % outt
           exit()
    countpid=0
    countdoi=0
    counter=0
    cstart=0

    while (cstart < tcount) :
       if (cstart > 0):
           ## print 'processing %d to %d record ...' % (cstart,cstart+ckan_limit)
           answer = action(args.ckan, {"q":ckan_pattern,"rows":ckan_limit,"start":cstart})
       for ds in answer['result']['results']:
            counter +=1
            ## print'    | %-4d | %-40s |' % (counter,ds['name'])

            record['id']  = '%s' % (ds['name'])
            if 'Group' in aids :
                record['Group']  = '%s' % (ds['groups'][0]['display_name'])
            if 'PID' in aids :
                xpid=[e for e in ds['extras'] if e['key'] == 'PID']
                if xpid:
                    record['PID']  = '%s' % (xpid[0]['value'])
                    countpid+=1
                else:
                    record['PID']  = '%s' % 'N/A'
            if 'DOI' in aids :
                xdoi=[e for e in ds['extras'] if e['key'] == 'DOI']
                if xdoi:
                    record['DOI']  = '%s' % (xdoi[0]['value'])
                    countdoi+=1
                else:
                    record['DOI']  = '%s' % 'N/A'
            if extension == 'hd5':
                record.append()
            elif extension == 'txt':
                outline=record['id']
                for aid in aids:
                    if aid != 'id':
                        outline+='\t'+record[aid]
                fh.write(outline+'\n')
       cstart+=len(answer['result']['results']) 

    print "Found\n\t%d\trecords\n\t%d\tPIDs\n\t%d\tDOIs" % (counter, countpid, countdoi)
    if extension == 'hd5':
      table.flush()
      h5file.close()
    elif extension == 'txt':
      fh.close()

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
    data_string = urllib.quote(json.dumps(data_dict))

    ##print('\t|-- Action %s\n\t|-- Calling %s\n\t|-- Object %s ' % (action,action_url,data_dict))	
    try:
       request = urllib2.Request(action_url)
       response = urllib2.urlopen(request,data_string)
    except urllib2.HTTPError as e:
       print '\t\tError code %s : The server %s couldn\'t fulfill the action %s.' % (e.code,host,action)
       if ( e.code == 403 ):
                print '\t\tAccess forbidden, maybe the API key is not valid?'
                exit(e.code)
       elif ( e.code == 409 and action == 'package_create'):
                print '\t\tMaybe the dataset already exists or you have a parameter error?'
                action('package_update',data_dict)
                return {"success" : False}
       elif ( e.code == 409):
                print '\t\tMaybe you have a parameter error?'
                return {"success" : False}
       elif ( e.code == 500):
                print '\t\tInternal server error'
                exit(e.code)
    except urllib2.URLError as e:
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
           1. >./searchB2FIND.py -c aleph tags:LEP
             searchs for all datasets of community ALEPH with tag "LEP" in b2find.eudat.eu.
           2. >./searchB2FIND.py author:"Jones*" AND Discipline:"Crystal?Structure" --ckan eudat-b1.dkrz.de
             searchs in eudat-b1.dkrz.de for all datasets having an author satrting with "Jones" and belongs to the discipline "Crystal Structure"
           3. >./searchB2FIND.py -c narcis DOI:'*' --ids DOI
             returns the list of id's and DOI's for all records in community "NARCIS" that have a DOI 
'''
    )
   
    p.add_argument('--ckan',  help='CKAN portal address, to which search requests are submitted (default is b2find.eudat.eu)', default='b2find.eudat.eu', metavar='IP/URL')
    p.add_argument('--output', '-o', help="Output file name and format. Format is given by the extention, supported are 'txt' (plain ascii file) or 'hd5' file, ...", default='results.txt', metavar='STRING')
    p.add_argument('--community', '-c', help="Community where you want to search in", default='', metavar='STRING')
    p.add_argument('--ids', '-i', help="Identifiers of found records outputed. Default is 'id'. Additional 'Source','PID' and 'DOI' are supported.", default=['id'], nargs='*')
    p.parse_args('--ids'.split())
    p.add_argument('pattern',  help='CKAN search pattern, i.e. (a list of) field:value terms.', metavar='PATTERN', nargs='*')
    
    args = p.parse_args()
    
    if (not args.pattern) and (not args.community) :
        print "[ERROR] Need at least a community given via option -c or a search pattern as an argument!"
        exit()
    
    return args
               
if __name__ == "__main__":
    main()
