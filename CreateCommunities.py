#!/usr/bin/env python
import sys, os, optparse, time
from os.path import expanduser
from urllib import parse  ## PY2 : quote
##PY2 from urllib2 import urlopen, Request
##PY2 from urllib2 import HTTPError,URLError
from output import Output
from urllib.request import urlopen, Request
from urllib.error import HTTPError,URLError
import json
import pprint
import random, string

p = optparse.OptionParser(
    description = '''Description                                                              
===========                                                                           
 Management of B2FIND communities within EUDAT-B2FIND, comprising                                      
      - Creating communities, i.e. CKAN groups
      - .....
''',
    formatter = optparse.TitledHelpFormatter(),
    prog = 'CreateCommuities.py',
    version = "%prog " + 'v0.1',
    usage = "%prog [options] COMMUNITY" 
)
        
p.add_option('-v', '--verbose', action="count", help="increase output verbosity (e.g., -vv is more than -v)", default=False)
p.add_option('--iphost', '-i', help="IP adress of B2FIND portal (CKAN instance)", metavar='IP')
p.add_option('--auth', help="Authentification for CKAN API (API key, by default taken from file $HOME/.netrc)",metavar='STRING')
p.add_option('--jobdir', help='\ndirectory where log, error and html-result files are stored. By default directory is created as startday/starthour/processid .', default=None)

options,arguments = p.parse_args()

pstat=dict()
now = time.strftime("%Y-%m-%d %H:%M:%S")
jid = os.getpid()
OUT = Output(pstat,now,jid,options)
logger = OUT.setup_custom_logger('root',options.verbose)


community=sys.argv[1]
conffile='mapfiles/%s.json' % community
with open(conffile, 'r') as f:
    group_dict = json.load(f)
print('group_dict %s' % group_dict)

# checking given options:
if (not options.iphost):
    logger.critical('The option iphost is mandatory !')
    sys.exit()
          
if (not options.auth):
    home = os.path.expanduser("~")
    if (not os.path.isfile(home+'/.netrc')):
        logger.critical('Can not access job host authentification file %s/.netrc ' % home )
        sys.exit()
    else:
        f = open(home+'/.netrc','r')
        lines=f.read().splitlines()
        f.close()

        l = 0
        for host in lines:
            if(options.iphost == host.split()[0]):
                options.auth = host.split()[1]
                break
        if (not options.auth):
            logger.critical('API key is neither given by option --auth nor can retrieved from %s/.netrc' % home )
            sys.exit()

if (True):
##for group_dict in groupsdict.itervalues() :
    ##HEW-T print('group_dict:\t%s\n' % (group_dict))

    # Use the json module to dump the dictionary to a string for posting.
    ### data_string = urllib.parse.quote(json.dumps(dataset_dict))
    encoding='utf-8'
    data_string = parse.quote(json.dumps(group_dict)).encode(encoding)

    # We'll use the user_create function to create a new user.
    action='http://%s/api/action/group_create' % options.iphost
    request = Request(action,data_string)

    # Creating a group requires an authorization header.
    request.add_header('Authorization', options.auth)

    # Make the HTTP request.
    ###Py2 response = urllib.request.urlopen(request, data_string)
    response = urlopen(request)
    assert response.code == 200

    # Use the json module to load CKAN's response into a dictionary.
    response_dict = json.loads(response.read()).encode(encoding)
    assert response_dict['success'] is True

    # package_create returns the created package as its result.
    created_package = response_dict['result']
    pprint.pprint(created_package)
