import optparse, os
import simplejson as json

def main():
    options, args = get_options()
    
    global checklist, checklist_extras
    checklist, checklist_extras = load_checklists(options.checklist)
    
    print "SEARCHING DIR: %s\nOUTPUT DIR: %s\nSHOW LIMIT: %d\n%s" % (options.dir, options.outputdir, int(options.show_limit), '-'*90)

    # find all files in directory:
    for root, dirs, files in os.walk(options.dir):
        for file in files:
            if file.endswith(".json"):
                 check_file(os.path.join(root, file))
                 
    for field in checklist:
        length = len(checklist[field])
        if length > 0:
            print 'FIELD [%s] is missing in [%d] files' % (field, length)
            
            if (options.show):
                limit = int(options.show_limit) if int(options.show_limit) < length else length-1
                print "    "+ "\n    ".join(
                    checklist[field][0:limit])
            
            # save results in file:
            if (not os.path.exists('missing')):  os.makedirs('missing')
            f = open('missing/main_%s' % field, 'w')
            f.write("\n".join(checklist[field]))
            f.close
            
    for efield in checklist_extras:
        length = len(checklist_extras[efield])
        if length > 0:
            print 'EXTRA FIELD [%s] is missing in [%d] files' % (efield, length)
            if (options.show):
                limit = int(options.show_limit) if int(options.show_limit) < length else length-1
                print "    "+ "\n    ".join(
                    checklist_extras[efield][0:limit])
            
            # save results in file:
            if (not os.path.exists('missing')):  os.makedirs('missing')
            f = open('missing/extra_%s' % efield, 'w')
            f.write("\n".join(checklist_extras[efield]))
            f.close

def check_file(file):
    jsondata = dict()
    with open(file, 'r') as f:
        try:
            jsondata=json.loads(f.read())
        except Exception, e:
            print "[ERROR] Cannot load the json file! %s" % e
            
    for field in checklist:
        if (not field in jsondata):
            checklist[field].append(file)
    
    extras_fields = list()
    for extra in jsondata['extras']:
        extras_fields.append(extra['key'])
    
    for efield in checklist_extras:
        if (not efield in extras_fields):
            checklist_extras[efield].append(file)
            
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
                if category == 'main':
                    list_main[line.rstrip()] = []
                else:
                    list_extra[line.rstrip()] = []
    
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
    p.add_option('--outputdir', '-o',  help='\nDirectory where log, error and html-result files are stored. By default directory is created as startday/starthour/processid .', default='missing')
    p.add_option('--checklist', '-c',  help='\nDirectory where log, error and html-result files are stored. By default directory is created as startday/starthour/processid .', default='scripts/checklist', metavar='FILE')
    p.add_option('--show', '-s',  help='\nShow failed files in terminal.', default=False, metavar='BOOLEAN')
    p.add_option('--show_limit',  help='\nThe limit of showed failed files in terminal.', default=20, metavar='INTEGER')
    
    return p.parse_args()

if __name__ == "__main__":
    main()
