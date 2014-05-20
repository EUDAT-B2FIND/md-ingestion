#!/usr/bin/env python

import optparse, os, sys
import urllib2
import simplejson as json

def main():
    options, args = get_options()
    
    global checklist, checklist_extras
    checklist, checklist_extras = load_checklists(options.checklist)
    
    print "SEARCHING DIR: %s\nOUTPUT DIR: %s\nSHOW LIMIT: %d\n%s" % (options.dir, options.outputdir, int(options.show_limit), '-'*90)

    # find all files in directory:
    for root, dirs, files in os.walk(options.dir):
        counter = 0
        no_files = len(filter(lambda x: x.endswith(".json") , files))
        if (no_files): print "\nProcessing dir: %s (%d files)" % (root, no_files)
        for file in files:
            if file.endswith(".json"):
                counter += 1
                update_progress(file,100*counter/no_files)
                check_file(os.path.join(root, file))
                 
    print "\n%s" % ('-'*90)
                 
    for field in checklist:
        for cmd in checklist[field]:
            length = len(checklist[field][cmd])
            if length > 0:
                print 'ACTION [%s] failed in FIELD [%s] in [%d] files' % (cmd, field, length)
                
                if (options.show):
                    limit = int(options.show_limit) if int(options.show_limit) < length else length-1
                    print "    "+ "\n    ".join(
                        checklist[field][cmd][0:limit])
                
                # save results in file:
                if (not os.path.exists(options.outputdir)):  os.makedirs(options.outputdir)
                f = open(options.outputdir+'/main_%s_%s' % (field,cmd), 'w')
                f.write("\n".join(checklist[field][cmd]))
                f.close
            
    for efield in checklist_extras:
        for cmd in checklist_extras[efield]:
            length = len(checklist_extras[efield][cmd])
            if length > 0:
                print 'ACTION [%s] failed in EXTRA FIELD [%s] in [%d] files' % (cmd, efield, length)
                if (options.show):
                    limit = int(options.show_limit) if int(options.show_limit) < length else length-1
                    print "    "+ "\n    ".join(
                        checklist_extras[efield][cmd][0:limit])
                
                # save results in file:
                if (not os.path.exists(options.outputdir)):  os.makedirs(options.outputdir)
                f = open(options.outputdir+'/main_%s_%s' % (efield,cmd), 'w')
                f.write("\n".join(checklist_extras[efield][cmd]))
                f.close

def check_file(file):
    jsondata = dict()
    with open(file, 'r') as f:
        try:
            jsondata=json.loads(f.read())
        except Exception, e:
            print "[ERROR] Cannot load the json file! %s" % e
            
    for field in checklist:
        for cmd in checklist[field]:
            if (not command(cmd,field, jsondata)):
                checklist[field][cmd].append(file)
    
    extras_fields = dict()
    for extra in jsondata['extras']:
        extras_fields[extra['key']] = extra['value']
    
    for efield in checklist_extras:
        for cmd in checklist_extras[efield]:
            if (not command(cmd, efield, extras_fields)):
                checklist_extras[efield][cmd].append(file)
    
def command(cmd, field, dict, *other):
    if (cmd == 'existence'):
        return field in dict
    elif(cmd == 'validation_url'):
        try:
            return urllib2.urlopen(dict[field], timeout=1).getcode() < 400
        except:
            return False
    
            
def load_checklists(file):
    try:
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
            
            
def get_options():
    p = optparse.OptionParser(
        description = "Description: Check mapped JSON files for correct fields",
        formatter = optparse.TitledHelpFormatter(),
        prog = 'check_json.py',
        version = "%prog "
    )
   
    p.add_option('--dir', '-d',  help='\nDirectory where log, error and html-result files are stored. By default directory is created as startday/starthour/processid .', default=None)
    p.add_option('--outputdir', '-o',  help='\nDirectory where log, error and html-result files are stored. By default directory is created as startday/starthour/processid .', default='failed')
    p.add_option('--checklist', '-c',  help='\nDirectory where log, error and html-result files are stored. By default directory is created as startday/starthour/processid .', default='scripts/checklist', metavar='FILE')
    p.add_option('--show', '-s',  help='\nShow failed files in terminal.', default=False, metavar='BOOLEAN')
    p.add_option('--show_limit',  help='\nThe limit of showed failed files in terminal.', default=20, metavar='INTEGER')
    
    return p.parse_args()
    
def update_progress(community,progress):
    sys.stdout.write('\rProcessing file {0} : [{1}{2}] {3}%              '.format(community,'#'*(progress/10), ' '*(10-progress/10), progress))
    sys.stdout.flush()

if __name__ == "__main__":
    main()
