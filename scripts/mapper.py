#!/usr/bin/env python

"""
Binyam Gebrekidan Gebre @Max Planck Institute - PL, 2014
Licensed under AGPLv3.
Moddifications :
  Heinrich Widmann @DKRZ 2014 : integrate actions in B2FIND.py
  Heinrich Widmann @DKRZ 2014 : rename (--> mapper.py) and add full mapping functionality to script

HOW TO RUN
$ ./mapper.py --mode MODE -i /path/to/input.[xml|json] -o /path/to/output.json -c /path/to/config.txt [-m /path/to/xpath-mapfile.xml 
Input: xml or json file, depending on the this, total mapping (xml to json by JAVA XPATH2.0 converting + 'paostproc') or only 'postprocessing' (json to json) is performed
Configuration file (this is a text file, where actions or rules are specified, In the scripts folder, you can see the format of the config.txt)
Map file (this is a xml file, where xml to json converting is defined by XPATH2.= rules)
Output:json file (the output of the mapping is a json file ready to be validated and/or uploaded to CKAN)

"""

import numpy as np
import os,sys
import argparse
import re, csv
import simplejson as json
import io
import codecs
import time
import subprocess
import shutil

mode = 0 ## 0(default) : json is input => only json 2 json mapping is performed / 1 : xml to jsaon mapping => i.e. whole pipeline inclding JAVA XPATH converting, python map_* functions and postprocessing is performed

# add parent directory to python library searching paths
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


import B2FIND
import urllib, mimetypes
import lxml.etree as etree
 
def get_dataset(srcFile):
    """
    reads file from disk and returns json text
    """
    global mode
    text = open(srcFile).read()
    url = urllib.pathname2url(srcFile)
    if (mimetypes.guess_type(url)[0] == 'text/xml') :
       mode=1
       dataset = etree.tostring(etree.fromstring(text), pretty_print = True).encode('ascii', 'ignore')
       ##dataset = text.encode('ascii', 'ignore')
       ##print 'dataset %s ' % dataset
       
    else :
       dataset = json.loads(text)
       mode=0
    return dataset
    
def get_conf(configFile):
    """
    reads config file 
    """
    f = codecs.open(configFile, "r", "utf-8")
    rules = f.readlines()[1:] # without the header
    rules = filter(lambda x:len(x) != 0,rules) # removes empty lines
    return rules
    
def save_data(dataset,dstFile):
    """
    saves a json file
    """
    with io.open(dstFile, 'w') as f: ## , encoding='utf-8') as f:
        ##HEW??? json.dump(dataset,f, ensure_ascii=False)
        ##encoded = dataset.encode('utf-8')
        ## dataset=encoded.decode(encoding, 'ignore')
        f.write(json.dumps(dataset, sort_keys = True, indent = 4,ensure_ascii=False))

def main():
	parser = argparse.ArgumentParser(description='Postprocesses a json file')
        parser.add_argument('-v', '--verbose', action="count",
                        help="increase output verbosity (e.g., -vv is more than -v)", default=False)
	parser.add_argument('-i','--srcFile',help='path to an input xml or json file to be mapped')
	parser.add_argument('-c','--configFile',help='(optional) path to a configuration text file')
	parser.add_argument('-m','--mapFile',help='path to a map file with xpath mapping rules (mandatory if srcFile is xml file)')
	parser.add_argument('-o','--dstFile', help='path to output json file')
	parser.add_argument('-j','--jobdir', help='path to log dir', default='log')
	
	# parse command line arguments
	args = parser.parse_args()
	srcFile = args.srcFile
	configFile = args.configFile
	dstFile = args.dstFile
	mapFile = args.mapFile
	
	if not (srcFile and dstFile):
	    print parser.print_help()
	    exit(1)

        # make jobdir
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        jid = os.getpid()
        pstat=[]
    
        # create logger and OUT output handler and initialise it:
        OUT = B2FIND.OUTPUT(pstat,now,jid,args)

        # create CONVERTER object:
        CV = B2FIND.CONVERTER(OUT,sys.path[0]+'/../../mapper/current')
	
	# read input and config files
 	dataset = get_dataset(srcFile)
 	##print 'mode %s' % mode

        global MapperVersion
        MapperVersion = '0.1'
        CV.logger.info('\nVersion:  \t%s' % MapperVersion)
        if mode == 1: 
          modetext='JAVA XPATH xml2json converting + Python json2json mapping' 
        else : 
            modetext='Python json2json mapping'
        CV.logger.info('Run mode:   \t%s' % modetext)
        CV.logger.debug('Process ID:\t%s' % str(os.getpid()))
        CV.logger.info('')

        if mode == 1 :
            # check mapfile
            if not (mapFile):
               print parser.print_help()
	       exit(1)
            if not os.path.isfile(mapFile):
               self.logger.error('[ERROR] Mapfile %s does not exist !' % mapfile)
               exit(1)

            if (not os.path.isdir('tmpdir/xml')):
               os.makedirs('tmpdir/xml')
            shutil.copy2(srcFile, 'tmpdir/xml')
            xmlfile=os.path.basename(srcFile)
            ##inpath=os.path.dirname(srcFile)
            inpath=os.path.abspath('tmpdir/xml')
            outpath=os.getcwd()+'/tmpdir'
            root='../mapper/current'
            cp = ".:"+":".join(filter(lambda x: x.endswith('.jar'), os.listdir(root+'/lib')))
            program = (filter(lambda x: x.endswith('.jar') and x.startswith('md-mapper-'), os.listdir(root)))[0]
            # run the converter
            CV.logger.debug(' Inpath\t%s\nJava root\t%s\nJava Class Path\t%s\nJAVA program\t%s' % (inpath,root,cp,program))
            proc = subprocess.Popen(
               ["cd '%s'; java -cp lib/%s -jar %s inputdir=%s outputdir=%s mapfile=%s"% (
                 os.getcwd()+'/'+root, cp,program,inpath,outpath, mapFile
               )], stdout=subprocess.PIPE, shell=True)
            (out, err) = proc.communicate()
            shutil.rmtree('tmpdir/xml')

            # check output and print it
            print 'out %s' % out
            jsonfile=os.path.abspath('tmpdir/json')+'/'+os.path.splitext(xmlfile)[0]+'.json'

            if err: 
                print '[ERROR] %s' % err
                exit(1)
            else :
                if ( os.path.getsize(jsonfile) > 0 ):
                  with open(jsonfile, 'r') as f:
                    try:
                      dataset=json.loads(f.read())
                    except:
                      print '    | [ERROR] Cannot load json file %s' % jsonfile
        else:
           jsonfile=srcFile

 	
        
        print 'conffile %s' % configFile
        if configFile :
          print 'xxx'
          if not os.path.isfile(configFile) :
             self.logger.info('[INFO] Can not access config file %s => no postprocessing excecuted !' % configFile)
          else:
             try:
               ## md postprocessing
               CV.logger.info('%s     INFO  PostProcessor - Processing: %s' % (time.strftime("%H:%M:%S"),jsonfile))
               conf_data = get_conf(configFile)
               dataset = CV.postprocess(dataset,conf_data)
             except:
               CV.logger.error('    | [ERROR] during postprocessing ')
        else:
          CV.logger.info('[INFO] No config file given => no postprocessing excecuted !')

        # loop over all fields
        for facet in dataset:
            if facet == 'extras':
               try:   ### Semantic mapping of extra fields
                  for extra in dataset[facet]:
                       if extra['key'] == 'Language':
                           # generic mapping of languages
                           extra['value'] = CV.map_lang(extra['value'])
                       if extra['key'] == 'Discipline':
                           # generic mapping of discipline
                           disctab = CV.cv_diciplines()
                           extra['value'] = CV.map_discipl(extra['value'],disctab.discipl_list)           # generic mapping of languages
               except:
                   CV.logger.error('    | [ERROR] during mapping of %s ' % extra['key'])
 	
            elif isinstance(dataset[facet], basestring) :
               ### mapping of default string fields
               dataset[facet]=dataset[facet].encode('ascii', 'ignore')
             	
 	# save output json to file
	save_data(dataset,dstFile)

if __name__ == "__main__":
	main()
