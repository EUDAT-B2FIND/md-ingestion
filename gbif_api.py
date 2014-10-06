#!/usr/bin/env python

"""gbif_api.py
..........

"""

### import B2FIND
import sys
import urllib, urllib2
import simplejson as json
import uuid

class GBIF_CLIENT(object):

    # call action api:
    ## GBIF.action('package_list',{})

    def __init__ (self, ip_host): ##, api_key):
	    self.ip_host = ip_host



    def action(self, action, offset):
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
	    
	    return self.__action_api(action, offset)
		


    def __action_api (self, action, offset):
        # Make the HTTP request for get datasets from GBIF portal
        response=''
        rvalue = 0
        ## offset = 0
        limit=100
        api_url = "http://api.gbif.org/v1"
        action_url = "{apiurl}/dataset?offset={offset}&limit={limit}".format(apiurl=api_url,offset=str(offset),limit=str(limit))	# default for get 'dataset'
        # normal case:
        ###  action_url = "http://{host}/api/3/action/{action}".format(host=self.ip_host,action=action)

        # make json data in conformity with URL standards
        ## data_string = urllib.quote(json.dumps(data_dict))

        ## print '\t|-- Action %s\n\t|-- Calling %s\n\t|-- Offset %d ' % (action,action_url,offset)
        try:
            request = urllib2.Request(action_url)
            ##if (self.api_key): request.add_header('Authorization', self.api_key)
            response = urllib2.urlopen(request)
        except urllib2.HTTPError as e:
            print '\t\tError code %s : The server %s couldn\'t fulfill the action %s.' % (e.code,self.ip_host,action)
            if ( e.code == 403 ):
                print '\t\tAccess forbidden, maybe the API key is not valid?'
                exit(e.code)
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

def main():
    
    # create GBIF object     
    iphost='api.gbif.org'
    auth=''                  
#    GBIF = GBIF_CLIENT(iphost,auth)
    GBIF = GBIF_CLIENT(iphost)

    # call the action:
    ##call_action(GBIF,options,options.action,object)
    

    nj=0    
    noffs=0
    data = GBIF.action('package_list',noffs)
    while(not data['endOfRecords'] and nj<10):
      ## 
      print 'data %s' % data['endOfRecords']
      for record in data['results']:
        nj+=1
        jsondata=dict()
        jsonmapped=dict()
        jsondata=record
        
        ## 
        print ' | %d | %s ' % (nj,jsondata['key'])
        print ' -----\n%s ' % jsondata
        ## B2FIND mapping
        jsonmapped['title']=jsondata["title"]
        jsonmapped['notes']=jsondata["citation"]["text"]
        if ( jsondata["contacts"] and 'firstName' in jsondata["contacts"][0]):
           print '>>> %s' % jsondata["contacts"]
           jsonmapped['author']=jsondata["contacts"][0]["firstName"]+' '##??? +jsondata["contacts"][0]["lastname"]
           for contact in jsondata["contacts"][1:]:
             if ( 'lastName' in contact ):           
               jsonmapped['author']+=' ; '+contact["lastName"]
             if ( 'firstName' in contact ):
               jsonmapped['author']+=','+contact["firstName"]
        else :
           jsonmapped['author']=jsondata["createdBy"]
        jsonmapped['PublicationYear']=jsondata["created"] ## or pubDate ?? only YYYY !
        if (jsondata["contacts"] and 'organization' in jsondata["contacts"][0]):
           jsonmapped['Origin']=jsondata["contacts"][0]["organization"] ## or 
        jsonmapped['Language']=jsondata["language"] ## should be mapped to lang code !!

        
        ## write json file
        # generate a uniquely identifier for this dataset:
        uid = str(uuid.uuid5(uuid.NAMESPACE_DNS, jsondata['key'].encode('ascii','replace')))
        setno=nj/5000+1
        setdir="oaidata/gbif-eml/SET_{setno}/json".format(setno=setno)
        jsonfile='%s/%s.json' % (setdir,uid)
        print '-->>  %s' % jsonfile
        with open(jsonfile, 'w') as outfile:
            try:
              ## json.dump(jsondata,outfileorig, sort_keys = True, indent = 4)
              json.dump(jsonmapped,outfile, sort_keys = True, indent = 4)
            except:
              print '    | [ERROR] Cannot write json file %s' % jsonfile
              sys.exit()

      noffs+=100
      data = GBIF.action('package_list',noffs)

 
if __name__ == "__main__":
    main()
