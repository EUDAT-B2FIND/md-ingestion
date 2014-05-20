#!/usr/bin/env python

"""ckan_api.py  Executes CKAN APIs (interface to CKAN)

Copyright (c) 2013 Heinrich Widmann (DKRZ)
                   John Mrziglod (DKRZ)

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

This is a prototype and not ready for production use.

"""
import os,sys,time
import simplejson as json
import optparse

from epicclient import EpicClient,Credentials
from EUDAT_JMD import CKAN_CLIENT


def main():
    
    # get command line options and check them:
    options = get_options()
    
    # clear the log file:
    clear_log(options.log)
    
    if (options.iphost is None or options.auth is None):
        print "[ERROR] No host and/or no api key is given!"
        exit()

    # create a new dkrz_ckanapi object:
    CKAN = CKAN_CLIENT(options.iphost,options.auth)

    # switch to interactive mode if options.action is not set:
    if(options.action == None):
        interactive_mode(CKAN,options)
    else:
        object = options.object

        if (object == None and options.file):
	        object = file2string(options.file)
    
        object = json.loads(object.replace("'", '"'))

        # call the action:
        call_action(CKAN,options,options.action,object)
        

def call_action(CKAN, options, action, object):
    write_log(options.log, "Call the action %s!\n" % action)
    if (options.epic):
          credentials = None
          ec = None
          try:
              credentials = Credentials('os',options.epic)
              credentials.parse()
          except Exception, err:
              print("%s Could not create credentials from credstore %s" % (err,options.epic))
          else:
              print "Create EPIC client instance to add uuid to handle server"
              ec = EpicClient(credentials)

    if (action == 'package_delete_all'):
        list=[]
        if ('group' in object):
            data = CKAN.action('member_list',{"id" : object['group'], "object_type":"package"})
            for d in data['result']:
                list.append(d[0])
        elif ('list' in object):
            list = object['list']
            
        list = list.reverse()
            
        print 'Total number of datasets to delete: ' + str(len(list))
        
        pcount = 0
        results = {
            "CKAN" : 0,
            "EPIC" : 0
        }
        
        start_time = time.time()
        for dataset in list:
            pcount += 1
            
            update_progress(
                'CKAN: %d, EPIC: %d\t' % (results['CKAN'],results['EPIC']), 
                pcount,len(list),start_time
            )
            
            write_log(options.log,"%s\n" % dataset)
            if((CKAN.action('package_update',{"name" : dataset, "state":"delete"}))['success']):
                results['CKAN'] += 1
                write_log(options.log,"\t#Deleted in CKAN\n")
                if(options.epic):
                    if (ec.deleteHandle(credentials.prefix + '/eudat-jmd_' + dataset)):
                        results['EPIC'] += 1
                        write_log(options.log,"\t#Deleted in EPIC\n")
        
        print '\n'
        print "%s" %('-'*100)                
        print "\nTotal number of dataset: %d\nDeleted in CKAN: %d\nDeleted in EPIC: %d" %(len(list),results['CKAN'],results['EPIC'])
    
    else:
        out = CKAN.action(action,object)
        print out
        
def update_progress(text,step,total,start_time):
    progress = 100*step/float(total)
    timer = time.time()-start_time
    
    t = ((total - step) * timer / step) / 3600
    h = int(t)
    m = int((t - h) * 60)
    
    sys.stdout.write('\r{0} [{1}{2}] {3}% - {4} h, {5} min        '.format(text,'#'*int(progress/10), ' '*int(10-progress/10), int(progress), h, m))
    sys.stdout.flush()

def clear_log(log):
    if (log == 'True'):
        try:
            f = open('eudat_jmd_api.log', 'w')
            f.write("")
            f.close()
        except IOError as (errno, strerror):
            print "file error({0}): {1}".format(errno, strerror)

def write_log(log,message):
    if (log == 'True'):
        try:
            f = open('api_2_run.log', 'a')
            f.write(message)
            f.close()
        except IOError as (errno, strerror):
            print "file error({0}): {1}".format(errno, strerror)

def interactive_mode(CKAN,options):
    print "Welcome to the ckan api tool!"
        
    action_short = {
        "d" : "package_delete",
        "c" : "package_create",
        "u" : "package_update"
    }
    
    print "Shorts of action names:"
    for n in action_short:
        print "%s: %s" % (n,action_short[n])
    
    action = ''
    while (action != 'q'):
        action = raw_input("\nPlease enter the action name ('q' to quit this program): ")
        
        if (action != 'q' and action != ''):
            if (action in action_short):
                action = action_short[action]
                print "Use action '%s'!" % action
        
            if (not CKAN.validate_action(action)):
                print '[ERROR] Action name '+ str(action) +' is not defined in CKAN_CLIENT! Allowed actions are:'
                list = CKAN.allowed_action.keys()
                print '\n'.join(sorted(list))
                continue
        
            if (action == 'package_delete'):
                id = raw_input("Please enter the object name: ")
                object = '{"id":"%s"}' % id
            else:
                object = raw_input("Please enter the object data (JSON format): ")
            
            
            object = json.loads(object.replace("'", '"'))

            # call the action:
            call_action(CKAN,options,action,object)
            
def get_options():
    # create a option parser and get the arguments given by the command line
    p = optparse.OptionParser(
	    description = '''Description: Management of meta data within CKAN instance , i.e. 
		    submit CKAN APIs via webservices''',
	    prog = 'ckan_api_v2.py',
	    usage = '''%prog [ OPTIONS ]'''
    )

    p.add_option('--object', '-o', help="attributes of CKAN data set in JSON format (default is an empty data set).\ne.g.: '--action create_dataset --object '{\"name\":\"Test\",\"title\":\"Test-Title\"}''")
    p.add_option('--file', '-f', help="path to a file with the attributes of CKAN data set in JSON format in the first line (program will ignored it if parameter '--object' was set)")
    p.add_option('--action', '-a', help="CKAN action to be executed")
    p.add_option('--log', '-l', help="Program will write a logfile to eudat_jmd_api.log")
    p.add_option('--epic', '-e',
         help="Check, generate and edit handles of CKAN datasets in handle server EPIC and with credentials as specified in given credstore file", default=None,metavar='FILE')
    p.add_option('--iphost', '-i', help="IP adress of CKAN instance")
    p.add_option('--auth', help="authentification for CKAN APIs (API key, taken from profile of CKAN user data")
    options, arguments = p.parse_args()
    
    return options

def file2string(filename):
	try:
		f = open(filename, 'r');
		string  = f.readline();
		f.close();
		return string;
	except IOError as (errno, strerror):
		print "file error({0}): {1}".format(errno, strerror);
		exit();
	

if __name__ == "__main__":
	main()
