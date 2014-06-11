#!/usr/bin/env python

"""check_json.py 
    Checks and validates mapped json files for uploading to CKAN database


Copyright (c) 2014 John Mrziglod (DKRZ)

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

This is a prototype and not ready for production use.

"""

import optparse, os, sys
import urllib2
import simplejson as json

def main():
    # get command line options:
    options, args = get_options()
    
    # load the orders for checking and validation:
    # "checklist" is for main fields and "checklist_extras" for extra fields 
    checklist, checklist_extras = load_checklists(options.checklist)
    
    # print the options:
    print "SEARCHING DIR: %s\nOUTPUT DIR: %s\nSHOW LIMIT: %d\n%s" % (options.dir, options.outputdir, int(options.show_limit), '-'*90)

    # find all files in directory "options.dir":
    for root, dirs, files in os.walk(options.dir):
        counter = 0
        no_files = len(filter(lambda x: x.endswith(".json") , files))
        
        if (no_files): print "\nProcessing dir: %s (%d files)" % (root, no_files)
        for file in filter(lambda x: x.endswith(".json") , files):
            counter += 1
            # shows a progress bar (update it for every 20th file):
            if (counter%20==0): update_progress(file,100*counter/no_files)
            
            # check file:
            check_file(os.path.join(root, file), checklist, checklist_extras)
    
    
    # print the results (depends on options.show and options.show_limit)  and write those to file:
    print "\n%s" % ('-'*90)
    
    # main fields:
    for field in checklist:
        for cmd in checklist[field]:
            length = len(checklist[field][cmd])
            if length > 0:
                print 'Command [%s] failed in FIELD [%s] in [%d] files' % (cmd, field, length)
                
                if (options.show):
                    limit = int(options.show_limit) if int(options.show_limit) < length else length-1
                    print "    "+ "\n    ".join(
                        checklist[field][cmd][0:limit])
                
                # save results in file:
                if (not os.path.exists(options.outputdir)):  os.makedirs(options.outputdir)
                f = open(options.outputdir+'/main_%s_%s' % (field,cmd), 'w')
                f.write("\n".join(checklist[field][cmd]))
                f.close
    
    # extra fields:
    for efield in checklist_extras:
        for cmd in checklist_extras[efield]:
            length = len(checklist_extras[efield][cmd])
            if length > 0:
                print 'Command [%s] failed in EXTRA FIELD [%s] in [%d] files' % (cmd, efield, length)
                if (options.show):
                    limit = int(options.show_limit) if int(options.show_limit) < length else length-1
                    print "    "+ "\n    ".join(
                        checklist_extras[efield][cmd][0:limit])
                
                # save results in file:
                if (not os.path.exists(options.outputdir)):  os.makedirs(options.outputdir)
                f = open(options.outputdir+'/main_%s_%s' % (efield,cmd), 'w')
                f.write("\n".join(checklist_extras[efield][cmd]))
                f.close

# Open the json file and run the commands of the checklists. 
# If one of those fails the file name will append to the list.
def check_file(file, checklist, checklist_extras):
    
    # load the json file:
    jsondata = dict()
    with open(file, 'r') as f:
        try:
            jsondata=json.loads(f.read())
        except Exception, e:
            print "[ERROR] Cannot load the json file! %s" % e
    
    # check main fields:
    for field in checklist:
        for cmd in checklist[field]:
            if (not command(cmd,field, jsondata)):
                checklist[field][cmd].append(file)
    
    # check the extra fields:
    extras_fields = dict()
    for extra in jsondata['extras']:
        extras_fields[extra['key']] = extra['value']
    
    for efield in checklist_extras:
        for cmd in checklist_extras[efield]:
            if (not command(cmd, efield, extras_fields)):
                checklist_extras[efield][cmd].append(file)

# The definition of the check and validation commands:
def command(cmd, field, dict, *other):
    if (cmd == 'existence'):
        return field in dict
    elif(cmd == 'validation_url'):
        try:
            return urllib2.urlopen(dict[field], timeout=1).getcode() < 400
        except:
            return False
    
# load file with orders for checking and validating and parse it:
def load_checklists(file):
    try:
        # open file:
        f = open(file, 'r')
        list_main = dict()
        list_extra = dict()
        category = ''
        
        for line in f:
            if ('[MAIN]' in line):
                category = 'main'
            elif ('[EXTRA]' in line):
                category = 'extra'
            elif (not (line == '' or line.startswith('#'))):
                # separate in field and commands:
                field, commands = line.rstrip().split(':')

                # separate the commands:
                dico = {}
                for cmd in  commands.split(","):
                    dico[cmd] = []
                
                # generate the dictionary:
                if category == 'main':
                    list_main[field] = dico
                else:
                    list_extra[field] = dico
    
        return list_main, list_extra
            
    except Exception, e:
        print "[ERROR] Cannot load the checklist file! %s" % e
            
# define allowed command line options and get those:
def get_options():
    p = optparse.OptionParser(
        description = "Description: Check mapped JSON files for correct fields",
        formatter = optparse.TitledHelpFormatter(),
        prog = 'check_json.py',
        version = "%prog 0.1"
    )
   
    p.add_option('--dir', '-d',  help='\nInput directory which contains the JSON files.', default=None, metavar='PATH')
    p.add_option('--outputdir', '-o',  help='\nOutput directory for the results.', default='failed', metavar='PATH')
    p.add_option('--checklist', '-c',  help='\nFile with orders for checking and validating', default='scripts/checklist', metavar='FILE')
    p.add_option('--show', '-s',  help='\nShow failed files in terminal.', default=False, metavar='BOOLEAN')
    p.add_option('--show_limit',  help='\nThe limit of showed failed files in terminal.', default=20, type='int', metavar='INTEGER')
    
    return p.parse_args()
    
# shows a progress bar:
def update_progress(file,progress):
    sys.stdout.write('\rProcessing file {0} : [{1}{2}] {3}%   '.format(file,'#'*(progress/10), ' '*(10-progress/10), progress))
    sys.stdout.flush()

if __name__ == "__main__":
    main()
