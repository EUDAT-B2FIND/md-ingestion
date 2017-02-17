#!/usr/bin/env python
import sys, os
import argparse
import json
import pprint
import hashlib
import urllib2
import urllib
from urllib2 import HTTPError,URLError
from b2handle.clientcredentials import PIDClientCredentials
from b2handle.handleclient import EUDATHandleClient
from b2handle.handleexceptions import HandleAuthenticationError,HandleNotFoundException,HandleSyntaxError,GenericHandleError

# parse arguments and options
parser = argparse.ArgumentParser()
parser.add_argument('-v', '--verbose', action="count",
      help="increase output verbosity (e.g., -vv is more than -v)", default=False)
parser.add_argument("infile", nargs='?', help="JSON file containing the dataset to uploadas dictionary", default=None ,metavar='INFILE')
parser.add_argument('--mode', '-m', metavar='PROCESSINGMODE', help='\nThis can be used to do a partial workflow. Supported modes are (u)ploading to CKAN repos and/or (h)andle generation. Default is u-h, i.e. a total ingestion', default='u-h')
parser.add_argument('--ipadress', '-i', help="IP adress of CKAN server", default='eudat6c.dkrz.de', metavar='IP')
parser.add_argument('--auth', '-a', help="Authentification for CKAN APIs (API key, by default taken from file $HOME/.netrc)",metavar='STRING')
parser.add_argument('--handle_check', 
         help="check and generate handles of CKAN datasets as specified in given CREDSTROREFILE", default=None,metavar='CREDSTOREFILE')
parser.add_argument('--ckan_check',
         help="check existence and checksum against existing datasets in CKAN database", default='False', metavar='BOOLEAN')

args = parser.parse_args()

# Put the details of the dataset we're going to create into a dict.
if args.infile :
    if ( os.path.getsize(args.infile) > 0 ):
        with open(args.infile, 'r') as f:
            try:
                dataset_dict=json.loads(f.read(),encoding = 'utf-8')
                if not 'name' in dataset_dict : ## name is mandatory CKAN field
                    dataset_dict['name']=os.path.splitext(args.infile)[0]
            except:
                print ('[ERROR] Cannot load from JSON file %s' % args.infile)
                sys.exit(-1)
else:
    dataset_dict = {
        'name': 'my_dataset_name',
        'notes': 'Another new and long description of my dataset',
        'author':'Heinrich Widmann',
        'groups':[{ "name" : 'fishproject' }],
        'JMDVERSION': '2.2',
        'B2FINDHOST': args.ipadress,
        'IS_METADATA': True,
        'MD_SCHEMA': 'oai_dc',
        'MD_STATUS': 'B2FIND_registered'
}

pidAttrs=["CHECKSUM","URL","JMDVERSION","B2FINDHOST","IS_METADATA","MD_SCHEMA","MD_STATUS"]

dataset_dict["URL"]='http://'+args.ipadress+'/dataset/'+dataset_dict['name']
dataset_dict["CHECKSUM"]=hashlib.md5(json.dumps(dataset_dict, encoding='latin1').strip()).hexdigest()

# Verbose information about the data
if args.verbose > 0 : print(' The dataset to upload:\n\t%s' % dataset_dict)

# Use the json module to dump the dictionary to a string for posting.
data_string = urllib.quote(json.dumps(dataset_dict))
# By default package_create function is used to create a new dataset.
request = urllib2.Request(
            'http://'+args.ipadress+'/api/action/package_create')

# Create a handle client and check handle if required
handlestatus="unknown"
if (args.handle_check):
    try:
        cred = PIDClientCredentials.load_from_JSON(args.handle_check)
        client = EUDATHandleClient.instantiate_with_credentials(cred)
    except Exception as err:
        print ("%s : Could not create credentials from credstore %s" % (err,args.handle_check))
        sys.exit(-1)
    else:
        if args.verbose > 1 : print ("HandleClient created")

    pidRecord=dict()
    try:
        pid = cred.get_prefix() + '/eudat-jmd_' + dataset_dict['name']
        rec = client.retrieve_handle_record_json(pid)
    except Exception as err :
        print ("ERROR : %s in client.retrieve_handle_record_json()" % (err))
    else:
        if args.verbose > 0 : print(" Retrieved Handle %s with\n |%-12s\t|%-30s\t|%-30s|\n %s" % (pid,'Attribute','Value','Changed value',80*'-'))

    chargs={}
    if rec : ## Handle exists
        for pidAttr in pidAttrs : 
            try:
                pidRecord[pidAttr] = client.get_value_from_handle(pid,pidAttr,rec)
            except Exception:
                print ("[CRITICAL : %s] in client.get_value_from_handle(%s)" % (err,pidAttr) )
            else:
                if args.verbose > 0 : print(" Got value %s from attribute %s sucessfully" % (pidRecord[pidAttr],pidAttr))

            if ( pidRecord[pidAttr] == dataset_dict[pidAttr] ) :
                chmsg="-- not changed --"
                if pidAttr == 'CHECKSUM' :
                    handlestatus="unchanged"
            else:
                chmsg=dataset_dict[pidAttr]
                handlestatus="changed"
                chargs[pidAttr]=dataset_dict[pidAttr] 
            if args.verbose > 0 : print(" |%-12s\t|%-30s\t|%-30s|" % (pidAttr,pidRecord[pidAttr],chmsg))
    else:
        print(" No handle exists")
        handlestatus="new"

    if handlestatus == "unchanged" : # no action required :-) !
        print ' No action required :-) - please exit'
        sys.exit(0)
    elif handlestatus == "changed" : # update dataset !
        request = urllib2.Request(
            'http://'+args.ipadress+'/api/action/package_update')
    else : # create new handle !
        try:
            npid = client.register_handle(pid, dataset_dict["URL"], dataset_dict["CHECKSUM"], None, True )
        except (HandleAuthenticationError,HandleSyntaxError) as err :
            print("[CRITICAL : %s] in client.register_handle" % err )
        except Exception as err :
            print("[CRITICAL : %s] in client.register_handle" % err )
            sys.exit()
        else:
            print(" New handle %s with checksum %s created" % (pid,dataset_dict["CHECKSUM"]))
            for pidAttr in ["JMDVERSION","B2FINDHOST","IS_METADATA","MD_SCHEMA","MD_STATUS"] :
                if pidAttr in dataset_dict :
                    chargs[pidAttr]=dataset_dict[pidAttr] 

    ## Modify all changed handle attributes
    if chargs :
            try:
                    client.modify_handle_value(pid,**chargs) ## ,URL=dataset_dict["URL"]) 
            except (Exception,HandleAuthenticationError,HandleNotFoundException,HandleSyntaxError) as err :
                print("[CRITICAL : %s] client.modify_handle_value of %s in %s" % (err,chargs,pid))
            else:
                if args.verbose > 0 : print(" Attributes %s of handle %s changed sucessfully" % (chargs,pid))

if 'u' in args.mode : ## maybe add optional upload ??
    # Creating a CKAN dataset requires an authorization header with your API key, 
    # from your user account on the CKAN site that you're creating the dataset on.
    home = os.path.expanduser("~")
    if not args.auth and os.path.isfile(home+'/.netrc'):
        with open(home+'/.netrc','r') as f :
            lines=f.read().splitlines()
            for host in lines:
                if(args.ipadress == host.split()[0]):
                    args.auth = host.split()[1]
                    break
    else:
        print ('[ERROR] API key for host %s neither given by option auth nor found in %s/.netrc for host %s' % (args.ipadress,home) )
        sys.exit(-1)

    request.add_header('Authorization', args.auth)

    # Make the HTTP request.
    try:
        response = urllib2.urlopen(request, data_string)
        assert response.code == 200
    except HTTPError as e:
        print '[WARNING] %s' % e
        if e.code == 409 :
            print ' CKAN Dataset already exists => try update'
            # We'll use the package_update function to update existing dataset.
            request = urllib2.Request(
                'http://'+args.ipadress+'/api/action/package_update')
            request.add_header('Authorization', '2d5a6ad0-ec8f-4ee2-8a2d-dd79b4b2ef46')
            try:
                response = urllib2.urlopen(request, data_string)
                assert response.code == 200
            except HTTPError as e:
                print '[ERROR %s] during update' % e
        elif e.code == 403 :
            print('[ERROR %s] Access forbidden, maybe the API key %s is not valid?' % (e,args.auth))
            sys.exit(e.code)
        else:    
            sys.exit()

    if response :
        # Use the json module to load CKAN's response into a dictionary.
        response_dict = json.loads(response.read())
        assert response_dict['success'] is True

        # package_create returns the created package as its result.
        created_package = response_dict['result']
        if args.verbose > 0 : 
            print('%s package: \t%s' % (handlestatus,created_package))
            print ('CKAN dataset : \t%s' % dataset_dict["URL"])
