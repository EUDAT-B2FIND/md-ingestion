#!/usr/bin/env python

"""mapdisciplines2clara.py  
maps discipline vocab as formated in B2FIND JSON to format of CLARA JSON

Copyright (c) 2019 Heinrich Widmann (DKRZ)

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

"""

import optparse, os, sys, re
import json
import logging as log
from collections import OrderedDict

def options_parser():
    
    descI=""" 
maps discipline vocab as formated in B2FIND JSON to format of CLARA JSON
"""
    p = optparse.OptionParser(
        description = "Description: maps discipline vocab as formated in B2FIND JSON to format of CLARA JSON",
        formatter = optparse.TitledHelpFormatter(),
        prog = 'mapdisciplines2clara.py 1.0',
        epilog='For any further information and documentation please look at README.txt file.',
        version = "%prog "
    )
   
        
    p.add_option('-v', '--verbose', action="count", 
                        help="increase output verbosity (e.g., -vv is more than -v)", default=False)
    p.add_option('--input', '-i', help="path to the input B2FIND JSON file",default='mapfiles/b2find_disciplines.json',metavar='PATH')
    p.add_option('--output', '-o', help="path to the output CLARA JSON file",default='mapfiles/clara_disciplines.json',metavar='PATH')

    return p
    
def main():
    p = options_parser()
    options,arguments = p.parse_args()

    # make jobdir
    jid = os.getpid()
    print "\tStart of processing"

    odict= OrderedDict([( "id",'null'),("label",""),("number",0),("children",list())])
    id='null'
    label='""'
    number=0

    inputfile='%s' % options.input
    outputfile='%s' % options.output

    if os.path.isfile(inputfile) :
        print('\tInput JSON file %s' % inputfile)
        with open(inputfile, 'r') as inf :
            inputobj = json.load(inf)
            for line in inputobj["disciplines"] :
                ##print(line)
                linearr=line.split('#')
                numberarr=linearr[0].split('.')
                number='0'.join(numberarr)
                ##print numberarr[-1:][0]
                label='%s' % linearr[2]
                id='%s %s' % (number,label)
                if len(number) == 1 :
                    odict["children"].append(OrderedDict([( "id",id),("label",label),("number",number),("children",list())]))
                    level1=int(numberarr[-1:][0])-1
                if len(number) == 3 :
                    print(odict["children"])
                    odict["children"][level1]["children"].append(OrderedDict([( "id",id),("label",label),("number",number),("children",list())]))
                    level2=int(numberarr[-1:][0])-1
                if len(numberarr) == 3 :
                    odict["children"][level1]["children"][level2]["children"].append(OrderedDict([( "id",id),("label",label),("number",number),("children",list())]))
                    level3=int(numberarr[-1:][0])-1
                if len(numberarr) == 4 :
                    odict["children"][level1]["children"][level2]["children"][level3]["children"].append(OrderedDict([( "id",id),("label",label),("number",number),("children",list())]))
                    level4=int(numberarr[-1:][0])-1
    else:
        print "\tWARNING : Can not access %s for removing" % jsonfile
 

    ##HEW-T print('Output %s' % json.dumps(odict,indent=4))
    with open(outputfile, 'w') as fp:
        json.dump(odict, fp, indent=4)

if __name__ == "__main__":
    main()
