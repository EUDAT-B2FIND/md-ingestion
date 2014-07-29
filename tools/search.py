#!/usr/bin/env python

"""search.py  Search in files on disk and in packages on CKAN

Copyright (c) 2014 John Mrziglod (DKRZ)

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""

import os, sys, re
import optparse

# add parent directory to python library searching paths
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import B2FIND

def main():
    options, pattern = get_options()
    list = []
    
    if (options.dir):
        # find all files in directory:
        for root, dirs, files in os.walk(options.dir):
            counter = 0
            no_files = len(filter(lambda x: x.endswith(options.filetype) , files))
            for file in filter(lambda x: x.endswith(options.filetype) , files):
                counter += 1
                update_progress(root,100*counter/no_files)
                check_file(list, os.path.join(root, file), pattern, options.offset)
        
        # print results: 
        print "\n%s" %('-'*100)
        print "Results on disk (%s), %d file(s), show max. %d:" % (options.dir, len(list), options.disk_limit)
        counter = 0
        for f,des in list:
            counter += 1
            if counter > options.disk_limit: break
            print '[%s]' % f
            print '    ...' +des+'...'
        

    if (options.ckan):
        print "\n%s" %('-'*100)
        
        CKAN = B2FIND.CKAN_CLIENT(options.ckan,None)
        
        # create pattern for CKAN:
        ckan_pattern = pattern
        if (options.community):
            ckan_pattern += " AND groups:%s" % options.community
        
        answer = CKAN.action('package_search', {"q":ckan_pattern,"rows":options.ckan_limit})
        
        # print results:
        print "Results on CKAN (%s), %d dataset(s), show max. %d:" % (options.ckan, answer['result']['count'], options.ckan_limit)
        for ds in answer['result']['results']:
            print '[%s]' % ds['name']
            print '    title: %s' % ds['title']
            pidf = open('pid.file', 'a')

            for extra in ds['extras']:
                if (extra['key'] == 'PID'):
                   print '    PID : %s' % extra['value']
                   pidf.write(extra['value']+'\n')

            pidf.close()
            if (len(ds['groups'])):
                print '    group: %s' % ds['groups'][0]['name']
                
def check_file(list, file, pattern, offset):
    data = None
    
#    if (':' in pattern and file.endswith('.json')): # loads the json file:
#        with open(file, 'r') as f:
#            try:
#                data=json.loads(f.read())
#            except Exception, e:
#                print "[ERROR] Cannot load the json file %s! %s" % (file,e)
#        
#        if (pattern in data):
#            # check main fields:
#            for field in checklist:
#                for cmd in checklist[field]:
#                    if (not command(cmd,field, jsondata)):
#                        checklist[field][cmd].append(file)
#            
#            # check the extra fields:
#            extras_data = dict()
#            for extra in data['extras']:
#                extras_data[extra['key']] = extra['value']
#        
#            index = data.index(pattern)
#            range = [
#                index-offset if index > offset-1 else 0,
#                index+offset+len(pattern) if len(data) > index+offset+len(pattern) else len(data)
#            ]
#            list.append((file, data[range[0]:index]+'\033[1m'+pattern+'\033[0m'+data[index+len(pattern):range[1]]))
#    else:
    with open(file, 'r') as f:
        try:
            data=f.read()
        except Exception, e:
            print "[ERROR] Cannot load the file %s! %s" % (file, e)

    if (pattern in data):
        index = data.index(pattern)
        range = [
            index-offset if index > offset-1 else 0,
            index+offset+len(pattern) if len(data) > index+offset+len(pattern) else len(data)
        ]
        list.append((file, data[range[0]:index]+'\033[1m'+pattern+'\033[0m'+data[index+len(pattern):range[1]]))
        
def get_options():
    p = optparse.OptionParser(
        description = "Description: Search in ASCII files and CKAN packages",
        prog = 'search.py',
        usage = '%prog [ OPTIONS ] PATTERN\nPATTERN is your search pattern. Regular expressions and fields for disk searches are not supported.'
    )
   
    group_disk = optparse.OptionGroup(p, "Disk Options",
        "Use these options if you want to search in files on disk")
    group_disk.add_option('--dir', '-d',  help='Directory where you want to search in. The program will search recursively.', default=None, metavar='PATH')
    group_disk.add_option('--disk_limit',  help='Limit of shown files', default=20, type='int', metavar='INTEGER')
    group_disk.add_option('--filetype', '-f',  help='Filextension (e.g. "json", "xml", etc.)', metavar='FILE EXTENSION')
    group_disk.add_option('--offset',  help='How much characters will be shown before and after the found pattern', default=35, type='int', metavar='INTEGER')
    
    group_ckan = optparse.OptionGroup(p, "CKAN Options",
        "Use these options if you want to search in CKAN")
    group_ckan.add_option('--ckan',  help='Search in this CKAN portal', default=None, metavar='IP/URL')
    group_ckan.add_option('--community', '-c', help="Community where you want to search in", default='', metavar='STRING')
    group_ckan.add_option('--ckan_limit',  help='Limit of shown datasets', default=20, type='int', metavar='INTEGER')
    
    p.add_option_group(group_disk)
    p.add_option_group(group_ckan)
    
    options, args = p.parse_args()
    
    if (len(args) != 1):
        print "[ERROR] Need a pattern as an argument!"
        exit()
    
    return options, args[0]
               
def update_progress(des,progress):
    sys.stdout.write('\rProcessing files {0} : [{1}{2}] {3}%            '.format(des,'#'*(progress/10), ' '*(10-progress/10), progress))
    sys.stdout.flush()

if __name__ == "__main__":
    main()
