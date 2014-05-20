#!/usr/bin/env python

import os, sys, re
import optparse

from EUDAT_JMD import CKAN_CLIENT

def main():
    options, args = get_options()
    list = []
    
    if (options.dir):
        # find all files in directory:
        for root, dirs, files in os.walk(options.dir):
            counter = 0
            no_files = len(filter(lambda x: x.endswith(options.filetype) , files))
            for file in filter(lambda x: x.endswith(options.filetype) , files):
                counter += 1
                update_progress(root,100*counter/no_files)
                check_file(list, os.path.join(root, file), options.pattern, options.offset)
        
        # print results: 
        print "\n%s" %('-'*100)
        print "Results on disk (%s), %d file(s)" % (options.dir, len(list))
        counter = 0
        for f,des in list:
            counter += 1
            if counter > options.disk_limit: break
            print '[%s]' % f
            print '    ...' +des+'...'
        

    if (options.ckan):
        print "\n%s" %('-'*100)
        
        CKAN = CKAN_CLIENT(options.ckan,None)
        answer = CKAN.action('package_search', {"q":options.pattern,"rows":options.ckan_limit})
        
        # print results:
        print "Results on CKAN (%s), %d dataset(s)" % (options.ckan, answer['result']['count'])
        for ds in answer['result']['results']:
            print '[%s]' % ds['name']
            print '    title: %s' % ds['title']
                
def check_file(list, file, pattern, offset):
    data = ''
    with open(file, 'r') as f:
        try:
            data=f.read()
        except Exception, e:
            print "[ERROR] Cannot load the json file! %s" % e
    
    
    if (pattern in data):
        index = data.index(pattern)
        range = [
            index-offset if index > offset-1 else 0,
            index+offset+len(pattern) if len(data) > index+offset+len(pattern) else len(data)
        ]
        list.append((file, data[range[0]:index-1]+' \033[1m'+pattern+'\033[0m'+data[index+len(pattern):range[1]]))
        
def get_options():
    p = optparse.OptionParser(
        description = "Description: Check mapped JSON files for correct fields",
        formatter = optparse.TitledHelpFormatter(),
        prog = 'check_json.py',
        version = "%prog "
    )
   
    p.add_option('--dir', '-d',  help='\nDirectory', default=None, metavar='PATH')
    p.add_option('--disk_limit',  help='\nLimit of shown files', default=20, type='int', metavar='INTEGER')
    p.add_option('--ckan', '-c',  help='\nScript will also search in this CKAN portal', default=None, metavar='IP/URL')
    p.add_option('--ckan_limit',  help='\nLimit of shown datasets', default=20, type='int', metavar='INTEGER')
    p.add_option('--pattern', '-p',  help='\nPattern .', default=None, metavar='STRING')
    p.add_option('--filetype', '-f',  help='\n', default='.json', metavar='FILE EXTENSION')
    p.add_option('--offset',  help='\nHow much characters will be shown before and after the found pattern', default=35, type='int', metavar='INTEGER')
    
    return p.parse_args()
               
def update_progress(des,progress):
    sys.stdout.write('\rProcessing file {0} : [{1}{2}] {3}%            '.format(des,'#'*(progress/10), ' '*(10-progress/10), progress))
    sys.stdout.flush()

if __name__ == "__main__":
    main()
