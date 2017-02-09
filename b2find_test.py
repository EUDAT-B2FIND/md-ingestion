#!/usr/bin/env python
import sys, os
import argparse
import urllib2
import urllib
from urllib2 import HTTPError,URLError
import json
import pprint

# parse arguments and options
parser = argparse.ArgumentParser()
parser.add_argument('-v', '--verbose', action="count",
      help="increase output verbosity (e.g., -vv is more than -v)", default=False)
parser.add_argument("infile", nargs='?', help="JSON file containing the dataset to uploadas dictionary", default=None ,metavar='INFILE')
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
        'notes': 'Another long description of my dataset',
}

if args.verbose > 0 :
    print ' The dataset to upload: %s' % args.verbose
    pprint.pprint(dataset_dict)

# Use the json module to dump the dictionary to a string for posting.
data_string = urllib.quote(json.dumps(dataset_dict))

# We'll use the package_create function to create a new dataset.
request = urllib2.Request(
    'http://'+args.ipadress+'/api/action/package_create')

# Creating a dataset requires an authorization header.
# Replace *** with your API key, from your user account on the CKAN site
# that you're creating the dataset on.
home = os.path.expanduser("~")
if not args.auth and os.path.isfile(home+'/.netrc'):
    with open(home+'/.netrc','r') as f :
        lines=f.read().splitlines()
        for host in lines:
            if(args.ipadress == host.split()[0]):
                args.auth = host.split()[1]
                break
else:
    print ('API key neither given by options auth nor found in %s/.netrc for host %s' % (home,args.ipadress) )
    sys.exit(-1)

request.add_header('Authorization', args.auth)

# Make the HTTP request.
try:
    response = urllib2.urlopen(request, data_string)
    assert response.code == 200
except HTTPError as e:
    print ' WARNING : %s :' % e
    if e.code == 409 :
        print '   Dataset already exists, try updating'
        # We'll use the package_update function to update existing dataset.
        request = urllib2.Request(
            'http://'+args.ipadress+'/api/action/package_update')
        request.add_header('Authorization', '2d5a6ad0-ec8f-4ee2-8a2d-dd79b4b2ef46')
        response = urllib2.urlopen(request, data_string)
        assert response.code == 200
    elif e.code == 403 :
        print('   Access forbidden, maybe the API key %s is not valid?' % args.auth)
        sys.exit(e.code)
    else:    
        sys.exit()

# Use the json module to load CKAN's response into a dictionary.
response_dict = json.loads(response.read())
assert response_dict['success'] is True

# package_create returns the created package as its result.
created_package = response_dict['result']
pprint.pprint(created_package)
