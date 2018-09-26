"""generating.py - class for Metadata generation : 
  - Generator    generates metadata from samples (e.g. comma separated lists)

Copyright (c) 2018 Heinrich Widmann (DKRZ)

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""

# from future
from __future__ import absolute_import
from __future__ import print_function
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# system relevant modules:
import os, glob, sys
import time, datetime, subprocess

# program relevant modules:
import logging
import traceback
import re

__author__ = "Heinrich Widmann"

PY2 = sys.version_info[0] == 2

# needed for class Generator:
import csv
from DublinCoreTerms import DublinCore

class Generator(object):
    
    """
    ### Generator - class
    # Provides methods to generate metadata from sample text files
    #
    # create GENERATOR object                       
    HV = Generator(OUT object, outdir)
    """
    
    def __init__ (self, OUT, pstat, base_outdir):
        self.logger = logging.getLogger('root')
        self.pstat = pstat
        self.OUT = OUT
        self.base_outdir = base_outdir
        self.DC_NS = 'http://purl.org/dc/elements/1.1/'
    
    def generate(self, request):
        ## method generate(GENERATOR object, [community, delimiter, mdprefix, mdsubset])
        # Generate XML files in format <mdprefix> and in subdir <mdsubset> from path <source> raw metadata (csv's, tsv's).
        # Generate every N. file a new subset directory.
        #
        # Parameters:
        # -----------
        # (list)  request - A request list with following items:
        #                    1. community
        #                    2. source
        #                    3. verb (comma | tab)
        #                    4. mdprefix (e.g. oai_dc)
        #                    5. mdsubset
        #
        # Return Values:
        # --------------
        # 1. (integer)  is -1 if something went wrong    
    
    
        start=time.time()
        ## set parameters
        community=request[0]
        inpath=request[1]
        if request[2] == 'comma' or request[2] == 'ListRecords' : # default
            delimiter=','
        else :
            delimiter='\t'
        mdprefix=request[3]
        if len(request)>4 and request[4] != None:
            mdsubset=request[4]  
        else:
            mdsubset='SET'

        outpath = '/'.join([self.base_outdir,community+'-'+mdprefix,mdsubset+'_1','xml'])
        if not os.path.isdir(outpath) :
            os.makedirs(outpath)
        ## csv file to parse
        fn='%s' % inpath

        ## mapfile
        mapfile="mapfiles/%s-%s.csv" % (community,mdprefix)
        print(' |- From %s-separated spreadsheet\n\t%s' % (request[2],inpath))

        """ Parse a CSV or TSV file """

        mapdc=dict()
        fields=list()
        try:
                fp = open(fn)
                ofields = re.split(delimiter,fp.readline().rstrip('\n').strip())
                print(' |- with original fields (headline)\n\t%s' % ofields)

                if os.path.isfile(mapfile) :
                    print(' |- using existing mapfile\t%s' % mapfile)
                    r = csv.reader(open(mapfile, "r"),delimiter='>')
                    for row in r:
                        fields.append(row[1].strip())
                else : 
                    print(' |- create mapfile\n\t%s and' % mapfile)
                    w = csv.writer(open(mapfile, "w"),delimiter='>')
                    for of in ofields:
                        if PY2:
                            mapdc[of.strip()]=raw_input('Target field for %s : ' % of.strip())
                        else:
                            mapdc[of.strip()]=input('Target field for %s : ' % of.strip())
                        fields.append(mapdc[of].strip())
                        w.writerow([of, mapdc[of]])

                if not delimiter == ',' :
                    tsv = csv.DictReader(fp, fieldnames=fields, delimiter='\t')
                else:
                    tsv = csv.DictReader(fp, fieldnames=fields, delimiter=delimiter)
                
                print(' |- generate XML files in %s' % outpath)
                for row in tsv:
                        dc = self.makedc(row)
                        if 'dc:identifier' in row:
                            outfile=re.sub('[\(\)]','',"".join(row['dc:identifier'].split()).replace(',','-').replace('/','-'))+'.xml'
                            print('  |--> %s' % outfile)
                            self.writefile(outpath+'/'+outfile, dc)
                        else:
                            print(' ERROR : At least target field dc:identifier must be specified') 
                            sys.exit()

        except IOError as strerror :
                print("Error ({0})".format(strerror))
                raise SystemExit
        fp.close()
        return -1

    def makedc(self,row):
        """ Generate a Dublin Core XML file from a TSV """
        metadata = DublinCore()
        with open('mapfiles/dcelements.txt','r') as f:
                dcelements = f.read().splitlines()
        for dcelem in dcelements :
                setattr(metadata,dcelem.capitalize(),row.get('dc:'+dcelem,''))

        with open('mapfiles/dcterms.txt','r') as f:
                dcterms = f.read().splitlines()
        for dcterm in dcterms :
                setattr(metadata,dcterm.capitalize(),row.get('dcterms:'+dcterm,''))

        return metadata

    def writefile(self,name, obj):

        """ Writes Dublin Core or Macrepo XML object to a file """
        if isinstance(obj, DublinCore):
                fp = open(name, 'w')
                fp.write(obj.makeXML(self.DC_NS))
        fp.close()

