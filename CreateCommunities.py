#!/usr/bin/env python
import sys, os, optparse, time
from os.path import expanduser
PY2 = sys.version_info[0] == 2
if PY2:
    from urllib import quote
    from urllib2 import urlopen, Request
    from urllib2 import HTTPError,URLError
else:
    from urllib import parse
    from urllib.request import urlopen, Request
    from urllib.error import HTTPError,URLError
from output import Output
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
p.add_option('--mode', '-m', metavar='PROCESSINGMODE', help='\nSupported modes are (c)reate, (u)pdate, (patch), (d)elete, (p)urge and (s)how . default is creation of a group', default='c')

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

print('aaauth %s' % options.auth)

if options.mode == 'c' :
    action='group_create'
##elif options.mode == 'u' :
##    action='group_update'
##    group_dict['id']=group_dict['name']
elif options.mode == 'patch' :
    action='group_patch'
    group_dict['id']=group_dict['name']
elif options.mode == 'd' :
    action='group_delete'
elif options.mode == 'p' :
    action='group_purge'
    group_dict['id']=group_dict['name']
elif options.mode == 's' :
    action='group_show'
    group_dict['id']=group_dict['name']
else :
    logger.critical('Mode %s not supported' % options.mode)
    sys.exit(-1)

##HEW-T print('group_dict %s' % group_dict)




if (True):
##for group_dict in groupsdict.itervalues() :
    ##HEW-T print('group_dict:\t%s\n' % (group_dict))

    # Use the json module to dump the dictionary to a string for posting.
    ### data_string = urllib.parse.quote(json.dumps(dataset_dict))
    encoding='utf-8'
    if PY2 :
        data_string = quote(json.dumps(group_dict))##.encode("utf-8") ## HEW-D 160810 , encoding="latin-1" ))##HEW-D .decode(encoding)
    else :
        data_string = parse.quote(json.dumps(group_dict)).encode(encoding) ## HEW-D 160810 , encoding="latin-1" ))##HEW-D .decode(encoding)
        
    # The action that should be excecuted.
    apiaction='http://%s/api/action/%s' % (options.iphost,action)
    print('API action excecuted : %s' % apiaction)
    request = Request(apiaction,data_string)

    # Creating a group requires an authorization header.
    request.add_header('Authorization', options.auth)

    # Make the HTTP request.
    ###Py2 response = urllib.request.urlopen(request, data_string)
    try:
        response = urlopen(request)
        assert response.code == 200
    except HTTPError as e:
        logger.critical('%s : Can not excecute the HTTP request' % e)
        sys.exit(-1)

    # Use the json module to load CKAN's response into a dictionary.
    ## print('Response %s' % response.read().decode('utf-8'))
    response_dict = response.read().decode('utf-8')
    ##HEW-T print('Response %s' % response_dict)
    response_dict = json.loads(response_dict)
    ## assert response_dict["success"] is True

    # package_create returns the created package as its result.
    created_package = response_dict['result']
    print('Response:')
    pprint.pprint(created_package)
