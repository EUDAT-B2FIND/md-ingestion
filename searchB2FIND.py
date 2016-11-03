#!/usr/bin/env python

"""searchB2FIND.py  performs search request in a CKAN catalogue (by default in the B2FIND metadata catalogue)

Copyright (c) 2015 Heinrich Widmann (DKRZ)
Modified for B2FIND Training
              2016 Heinrich Widmann (DKRZ)

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""

import os, sys, io, time
import argparse
import simplejson as json
import urllib, urllib2
import ckanclient
from collections import OrderedDict,Counter


def main():

    ckanlistrequests=['package_list','group_list','tag_list']

    ## Get options and arguments
    args = get_args(ckanlistrequests)
    
    ## Settings for CKAN client and API
    ckanapi3='http://'+args.ckan+'/api/3'
    ckan = ckanclient.CkanClient(ckanapi3)
    ckan_limit=500000

    start=time.time()

    if args.request.endswith('list'):
        try:
            if args.request == 'community_list' :
                action='group_list'
            else:
                action=args.request
            answer = ckan.action(action, rows=ckan_limit)
        except ckanclient.CkanApiError as e :
            print '\t\tError %s Supported list requests are %s.' % (e,ckanlistrequests)
            sys.exit(1)
        ## print '|- The list of %ss :\n\t%s' % (args.request.split('_')[0],'\n\t'.join(answer).encode('utf8'))
        print '\n\t%s' % '\n\t'.join(answer).encode('utf8')
        sys.exit(0)

    # create CKAN search pattern :
    ckan_pattern = ''
    sand=''
    pattern=' '.join(args.pattern)

    if (args.community):
        ckan_pattern += "groups:%s" % args.community
        sand=" AND "
    if (args.pattern):
        ckan_pattern += sand + pattern   

    print ' | - Search\n\t|- in\t%s\n\t|- for\t%s\n' % (args.ckan,ckan_pattern)

    if args.request == 'package_search' :
        answer = ckan.action('package_search', q=ckan_pattern, rows=ckan_limit)
    tcount=answer['count']
    print " | - Results:\n\t|- %d records found in %d sec" % (tcount,time.time()-start)

    # Read in B2FIND metadata schema and fields
    schemafile =  '%s/mapfiles/b2find_schema.json' % (os.getcwd())
    with open(schemafile, 'r') as f:
        b2findfields=json.loads(f.read(), object_pairs_hook=OrderedDict)   


    if tcount>0 and args.keys is not None :
        if len(args.keys) == 0 :
            akeys=[]
        else:
            if args.keys[0] == 'B2FIND.*' :
                akeys=OrderedDict(sorted(b2findfields.keys()))
            else:
                akeys=args.keys

        suppid=b2findfields.keys()

        fh = io.open(args.output, "w", encoding='utf8')
        record={} 
  
        totlist=[]
        count={}
        count['id']=0
        statc={}
        for outt in akeys:
                if outt not in suppid :
                    print ' [WARNING] Not supported key %s is removed' % outt
                    akeys.remove(outt)
                else:
                    count[outt]=0
                    statc[outt] = Counter()

        printfacets=''
        if (len(akeys) > 0):
            printfacets="and related facets %s " % ", ".join(akeys)

            print "\t|- ID's %sare written to %s ..." % (printfacets,args.output)

        counter=0
        cstart=0
        oldperc=0
        start2=time.time()

        while (cstart < tcount) :
	       if (cstart > 0):
	           answer = ckan.action('package_search', q=ckan_pattern, rows=ckan_limit, start=cstart)
	       if len(answer['results']) == 0 :
	           break
	
	       # loop over found records
	       for ds in answer['results']:
	            counter +=1
	            ##HEW-T print'    | %-4d | %-40s |' % (counter,ds['name'])
	            perc=int(counter*100/tcount)
	            bartags=perc/5
	            if perc%10 == 0 and perc != oldperc :
	                oldperc=perc
	                print "\r\t[%-20s] %5d (%3d%%) in %d sec" % ('='*bartags, counter, perc, time.time()-start2 )
	                sys.stdout.flush()
	
	            
	            record['id']  = '%s' % (ds['name'])
	            outline=record['id']
	
	            # loop over facets
	            for facet in akeys:
                        ##HEW-T print 'facet : %s' % facet
                        ckanFacet=b2findfields[facet]["ckanName"]
	                if ckanFacet in ds: ## CKAN default field
	                    if facet == 'Group':
	                        record[facet]  = ds[ckanFacet][0]['display_name']
	                    else:
	                        record[facet]  = ds[ckanFacet]
	
	                else: ## CKAN extra field
                            ##HEW-T print 'ds extras %s' % ds['extras']
	                    efacet=[e for e in ds['extras'] if e['key'] == facet]
	                    if efacet:
                                ##HEW-T print 'rrrr %s effff %s' % (record[facet],efacet[0]['value'])
	                        record[facet]  = efacet[0]['value']
	                    else:
	                        record[facet]  = 'N/A'
	                if record[facet] is None :
	                    record[facet]='None'
	                    statc[facet][record[facet]]+=1
	                else:
	                    if not isinstance(record[facet],list):
	                        words=record[facet].split(';')
	                    else:
	                        words=record[facet]
	                    for word in words:
	                        if isinstance(word,dict): word=word['name']
	                        statc[facet][word]+=1
	                if not ( record[facet] == 'N/A' or record[facet] == 'Not Stated') and len(record[facet])>0 : 
	                    count[facet]+=1
	
		        outline+='\t | %-30s' % record[facet][:30]
	                ##outline+='\n   \t| %-20s' % (statc[aid].keys(),statc[aid].values())
	            fh.write(outline+'\n')
	       cstart+=len(answer['results']) 
        fh.close()
	
        if len(akeys) > 0 :
	        statfh = io.open('stat_'+args.output, "w", encoding='utf8')
	        ##print "\n|- Statistics :\n\t| %-16s | %-10s | %6s |\n\t%s " % ('Facet','Occurence','%',"-" * 50)
	        print '|- Statistics written to file %s' % 'stat_'+args.output
	
	        statline=unicode("")
	        for outt in akeys:
	            statline+= "| %-16s\n\t| %-15s | %-6d | %3d |\n" % (outt,'-Total-',count[outt],int(count[outt]*100/tcount))
	            for word in statc[outt].most_common(10):
	                statline+= '\t| %-15s | %-6d | %3d |\n' % (word[0][:100], word[1], int(word[1]*100/tcount))
	
	        statfh.write(statline)
	
	        statfh.close()
	
def get_args(ckanlistrequests):
    p = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description = "Description: Lists identifers of datasets that fulfill the given search criteria",
        epilog =  '''Examples:
           1. >./searchB2FIND.py -c aleph tags:LEP
             searchs in b2find.eudat.eu within the community ALEPH 
             for all datasets with tag "LEP".
           2. >./searchB2FIND.py author:"Jones*" AND Discipline:"Crystallography"
             searchs in  b2find.eudat.eu for all datasets having an 
             author starting with "Jones" and belonging to the 
             discipline "Crystallography"
           3. >./searchB2FIND.py -c narcis DOI:'*' --keys DOI
             returns the list of id's and the related DOI's of all records in 
             community "NARCIS" that have a DOI. Additonally statistical 
             information about the coverage and distribution of the facet 
             DOI is written to an extra file. 
'''
    )
   
    p.add_argument('--request', '-r', help="Request command. Supported are list requests ('package_list','group_list','tag_list') and 'package_search'. The latter is the default and expects a search pattern as argument." % ckanlistrequests, default='package_search', metavar='STRING')
    p.add_argument('--community', '-c', help="Community where you want to search in", default='', metavar='STRING')
    p.add_argument('--keys', '-k', help=" B2FIND fields additionally outputed for the found records. Additionally statistical information is printed into an extra output file.", default=[], nargs='*')
    p.parse_args('--keys'.split())
    p.add_argument('--ckan',  help='CKAN portal address, to which search requests are submitted (default is b2find.eudat.eu)', default='b2find.eudat.eu:8080', metavar='URL')
    p.add_argument('--output', '-o', help="Output file name (default is results.txt)", default='results.txt', metavar='FILE')
    p.add_argument('pattern',  help='CKAN search pattern, i.e. by logical conjunctions joined field:value terms.', default='*:*', metavar='PATTERN', nargs='*')
    
    args = p.parse_args()
    
    if (not args.pattern) and (not args.community) :
        print "[ERROR] Need at least a community given via option -c or a search pattern as an argument!"
        exit()
    
    return args
               
if __name__ == "__main__":
    main()
