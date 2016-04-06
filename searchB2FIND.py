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

import os, sys, io, time
import argparse
import simplejson as json
import urllib, urllib2
import ckanclient
from collections import OrderedDict,Counter

def main():
    args = get_args()
    
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

    print ' | - Search\n\t|- in\t%s\n\t|- for\t%s\n' % (args.ckan,ckan_pattern)

    ckanapi3='http://'+args.ckan+'/api/3'
    ckan = ckanclient.CkanClient(ckanapi3)
    ckan_limit=100000
    start=time.time()
    answer = ckan.action('package_search', q=ckan_pattern, rows=ckan_limit)
    tcount=answer['count']
    print " | - Results:\n\t|- %d records found in %d sec" % (tcount,time.time()-start)

    b2findorder=['Title','Description','Tags','Source','DOI','PID','Checksum','Rights','Discipline','Creator','Publisher','PublicationYear','Language','TemporalCoverage','SpatialCoverage','Format','Contact','MetadataAccess']

    b2findfacets={
        'Title':'title',
        'Description':'notes',
        'Tags':'tags',
        'Source':'url',
        'DOI':'DOI',
        'PID':'PID',
        'Checksum':'version',
        'Rights':'Rights',
        'Discipline':'Discipline',
        'Creator':'author',
        'Publisher':'Publisher',
        'PublicationYear':'PublicationYear',
        'Language':'Language',
        'TemporalCoverage':'TemporalCoverage',
        'SpatialCoverage':'SpatialCoverage',
        'Format':'Format',
        'Contact':'Contact',
        'MetadataAccess':'MetadataAccess'
}

    admin_fields={
        'ManagerVersion':'ManagerVersion',
        'modified':'metadata_modified',
        'created':'metadata_created',
        'oai_set':'oai_set'
}

    if tcount>0 and args.keys is not None :
        if len(args.keys) == 0 :
            akeys=[]
        else:
            if args.keys[0] == 'B2FIND.*' :
                akeys=OrderedDict(sorted(b2findfacets.items(), key=lambda i:b2findorder.index(i[0])))
            else:
                akeys=args.keys

        suppid=b2findfacets
        suppid.update(admin_fields)

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
	
	       # loop over records
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
	
	            # loop over facets
	            for facet in akeys:
	                if suppid[facet] in ds: ## CKAN default field
	                    if facet == 'Group':
	                        record[facet]  = ds[suppid[facet]][0]['display_name']
	                    else:
	                        record[facet]  = ds[suppid[facet]]
	
	                else: ## CKAN extra field
	                    efacet=[e for e in ds['extras'] if e['key'] == facet]
	                    if efacet:
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
	
	
	            outline=record['id']
	            for aid in akeys:
	                outline+='\t | %-30s' % record[aid][:30]
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
	            statline+= "| %-16s | %-10d | %6d |\n" % (outt,count[outt],int(count[outt]*100/tcount))
	            for word in statc[outt].most_common(10):
	                statline+= '\t| %-10d | %-100s\n' % (word[1], word[0][:100])
	
	        statfh.write(statline)
	
	        statfh.close()
	
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
    ##except urllib2.BadStatusLine as e:
    ##   exit('%s' % e.reason)
    except Exception, e:
       exit('%s' % e.reason)
    else :
       out = json.loads(response.read())
       assert response.code >= 200
       return out

def get_args():
    p = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description = "Description: Lists identifers of datasets that fulfill the given search criteria",
        epilog =  '''Examples:
           1. >./searchB2FIND.py -c aleph tags:LEP
             searchs for all datasets of community ALEPH with tag "LEP" in b2find.eudat.eu.
           2. >./searchB2FIND.py author:"Jones*" AND Discipline:"Crystal?Structure" --ckan eudat-b1.dkrz.de
             searchs in eudat-b1.dkrz.de for all datasets having an author starting with "Jones" and belongs to the discipline "Crystal Structure"
           3. >./searchB2FIND.py -c narcis DOI:'*' --keys DOI
             returns the list of id's and DOI's for all records in community "NARCIS" that have a DOI 
'''
    )
   
    p.add_argument('--ckan',  help='CKAN portal address, to which search requests are submitted (default is b2find.eudat.eu)', default='b2find.eudat.eu', metavar='IP/URL')
    p.add_argument('--output', '-o', help="Output file name and format. Format is determined by the extention, supported are 'txt' (plain ascii file) or 'hd5' file. Default is the ascii file results.txt.", default='results.txt', metavar='STRING')
    p.add_argument('--community', '-c', help="Community where you want to search in", default='', metavar='STRING')
    p.add_argument('--keys', '-k', help=" Keys to be outputed for the found records. Default is to print out only the B2FIND identifier . If you want only know the total numbers of records found, but no id's should be listed, enter None. Additionally supported fields are all fields of the B2FIND schema and some additional admin properties. If you want statistical information about all B2FIND fields enter 'B2FIND.*' ", default=[], nargs='*')
    p.parse_args('--keys'.split())
    p.add_argument('pattern',  help='CKAN search pattern, i.e. by logical conjunctions joined field:value terms.', default='*:*', metavar='PATTERN', nargs='*')
    
    args = p.parse_args()
    
    if (not args.pattern) and (not args.community) :
        print "[ERROR] Need at least a community given via option -c or a search pattern as an argument!"
        exit()
    
    return args
               
if __name__ == "__main__":
    main()
