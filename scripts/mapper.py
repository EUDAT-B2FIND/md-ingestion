#!/usr/bin/env python

"""
Binyam Gebrekidan Gebre @Max Planck Institute - PL, 2014
Licensed under AGPLv3.
Moddifications :
  Heinrich Widmann @DKRZ 2014 : integrate actions in B2FIND.py
  Heinrich Widmann @DKRZ 2014 : rename (--> mapper.py) and add full mapping functionality to script
  Heinrich Widmann @DKRZ 2015 : adapted to the new design of B2FIND mapper module

HOW TO RUN
$ ./mapper.py -i /path/to/input.[xml|json] -o /path/to/output.json [ -c /path/to/config.txt ] [ -m /path/to/xpath-mapfile.xml 
Input (mandatory) : xml or json file, depending on the this, total mapping (xml to json by JAVA XPATH2.0 converting + 'paostproc') or only 'postprocessing' (json to json) is performed
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
from B2FIND import CONVERTER as CV2
import urllib.request, urllib.parse, urllib.error, mimetypes
import lxml.etree as etree
 
def get_dataset(srcFile):
    """
    reads file from disk and returns json text
    """
    global mode
    text = open(srcFile).read()
    url = urllib.request.pathname2url(srcFile)
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
    rules = [x for x in rules if len(x) != 0] # removes empty lines
    return rules
    
def save_data(dataset,dstFile):
    """
    saves a json file
    """ ##HEW-D with io.open(dstFile, 'w') as f: ## , encoding='utf-8') as f:
    with io.open(dstFile, 'w') as json_file:
        try:
            print('   | [INFO] decode json data')
            data = json.dumps(dataset,sort_keys = True, indent = 4).decode('utf8')
        except Exception as e:
            print('    | [ERROR] %s : Cannot decode dataset %s' % (e,dataset))
        try:
            print('   | [INFO] save json file')
            json_file.write(data)
        except TypeError as err :
            print('    | [ERROR] Cannot write json file %s : %s' % (path+'/json/'+filename,err))
        except Exception as e:
            print('    | [ERROR] %s : Cannot write json file %s' % (e,path+'/json/'+filename))

        ##HEW-D f.write(json.dumps(dataset, sort_keys = True, indent = 4,ensure_ascii=False))

def main():
	parser = argparse.ArgumentParser(description='Perform B2FIND mapping')
        parser.add_argument('-v', '--verbose', action="count",
                        help="increase output verbosity (e.g., -vv is more than -v)", default=False)
        parser.add_argument('--mode', '-m', metavar='PROCESSINGMODE', help='\nThis specifies the performed mapping steps. Supported modes are (m)apping, (c)onverting, (p)ostprocessing, (x)path, (v)alidating, . Default is mode "-m", i.e. the total mapping workflow from (x)path selection up to generic python mapping', default='m')
        parser.add_argument('inFile', metavar='INFILE',
                   help='Path to the input file to be processed (could be a xml or json file)')
        parser.add_argument('outFile', metavar='OUTFILE',
                   help='Path to the output file (the mapped json file)')
	parser.add_argument('--configFile', metavar='CONFIGFILE',help='path to the postprocessing configuration file (if not given tried to derivate from input file name)')
	parser.add_argument('--mapFile',help='path to a map file with xpath mapping rules (if not given tried to derivate from input file name)')
	parser.add_argument('-j','--jobdir', help='path to log dir', default='log')
	
	# parse command line arguments
	args = parser.parse_args()
        if not args.inFile:   # if inffile is not given
            parser.error('Input file is not given')
 	    print(parser.print_help())
	    exit(1)
        else:
            inFile = args.inFile
        if not args.outFile:   # if outfilen is not given
            parser.error('Output file is not given')
 	    print(parser.print_help())
	    exit(1)
        else:
            outFile = args.outFile

        mapFileDir=os.getcwd()+'/mapfiles/'
        oldmapFileDir='../mapper/current'
        (inPath,inFileName)=os.path.split(inFile)
        inPathList=inPath.split('/')
        ext=os.path.splitext(inFileName)[1]
        print('ext %s' % ext)
        subset=inPathList[-2]
        print('subset %s' % subset)
        (comm,mdformat)=inPathList[-3].split('-')

        if not args.configFile:   # if config file is not given
            if mode != 'p':
                configFile=mapFileDir+'mdpp_'+comm+'-'+mdformat+'.conf'
        else:
            configFile=os.path.abspath(args.configFile)

        # check mapFile
        if mode != 'x': # mapfile needed
            if not args.mapFile:   # if map file is not given
               mapFile=mapFileDir+comm+'-'+mdformat+'.xml'
            else:
               mapFile=args.mapFile

            if os.path.isfile(mapFile):
               mapFile=os.path.abspath(mapFile)
            else:
               print('[ERROR] Can not access mapfile %s !' % mapFile)
               exit(1)

            print(' |- Mapfile\t%s' % mapFile)




        # make jobdir
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        jid = os.getpid()
        pstat=[]
    
        # create logger and OUT output handler and initialise it:
        OUT = B2FIND.OUTPUT(pstat,now,jid,args)

        # create CONVERTER object:
        CV = B2FIND.CONVERTER(OUT)
	
	# read input and config files
 	dataset = get_dataset(inFile)

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
            if (not os.path.isdir('tmpdir/xml')):
               os.makedirs('tmpdir/xml')
            shutil.copy2(inFile, 'tmpdir/xml')
            xmlfile=os.path.basename(inFile)
            ##inpath=os.path.dirname(inFile)
            inpath=os.path.abspath('tmpdir/xml')
            outpath=os.getcwd()+'/tmpdir'
            cp = ".:"+":".join([x for x in os.listdir(oldmapFileDir+'/lib') if x.endswith('.jar')])
            program = ([x for x in os.listdir(oldmapFileDir) if x.endswith('.jar') and x.startswith('md-mapper-')])[0]
            # run the converter
            CV.logger.debug(' Inpath\t%s\nJava oldmapFileDir\t%s\nJava Class Path\t%s\nJAVA program\t%s' % (inpath,oldmapFileDir,cp,program))
            proc = subprocess.Popen(
               ["cd '%s'; java -cp lib/%s -jar %s inputdir=%s outputdir=%s mapfile=%s"% (
                 os.getcwd()+'/'+oldmapFileDir, cp,program,inpath,outpath, mapFile
               )], stdout=subprocess.PIPE, shell=True)
            (out, err) = proc.communicate()
            shutil.rmtree('tmpdir/xml')

            # check output and print it
            print('out %s' % out)
            jsonfile=os.path.abspath('tmpdir/json')+'/'+os.path.splitext(xmlfile)[0]+'.json'

            if err: 
                print('[ERROR] %s' % err)
                exit(1)
            else :
                if ( os.path.getsize(jsonfile) > 0 ):
                  with open(jsonfile, 'r') as f:
                    try:
                      dataset=json.loads(f.read())
                    except:
                      print('    | [ERROR] Cannot load json file %s' % jsonfile)
        else:
           jsonfile=inFile

        if configFile :
          if not os.path.isfile(configFile) :
             CV.logger.info('[INFO] Can not access config file %s => no postprocessing excecuted !' % configFile)
          else:
             try:
               ## md postprocessing
               CV.logger.info('%s     INFO  PostProcessor - Processing: %s' % (time.strftime("%H:%M:%S"),jsonfile))
               conf_data = get_conf(configFile)
               dataset = CV.postprocess(dataset,conf_data)
             except:
               CV.logger.error('    | [ERROR] during postprocessing ')
        else:
          CV.logger.info('[INFO] No config file given => rule specific postprocessing skipped !')

        disctab = CV.cv_disciplines()

        publdate=None
        # loop over all fields
        for facet in dataset:
              if facet == 'author':
                dataset[facet] = CV.cut(dataset[facet],'\(\d\d\d\d\)',1).strip()
                dataset[facet] = CV.remove_duplicates(dataset[facet])
              elif facet == 'tags':
                dataset[facet] = CV.list2dictlist(dataset[facet]," ")
              elif facet == 'url':
                iddict = CV.map_identifiers(dataset[facet])
              elif facet == 'extras':
                try:   ### Semantic mapping of extra fields
                  for extra in dataset[facet]:
                    if type(extra['value']) is list:
                      extra['value']=CV.uniq(extra['value'])
                      if len(extra['value']) == 1:
                        extra['value']=extra['value'][0] 
                    elif extra['key'] == 'Discipline':
                      extra['value'] = CV.map_discipl(extra['value'],disctab.discipl_list)
                    elif extra['key'] == 'Publisher':
                      extra['value'] = CV.cut(extra['value'],'=',2)
                      extra['value'] = CV.remove_duplicates(extra['value'])
                    elif extra['key'] == 'SpatialCoverage':
                      desc,slat,wlon,nlat,elon=CV.map_spatial(extra['value'])
                      if wlon and slat and elon and nlat :
                        spvalue="{\"type\":\"Polygon\",\"coordinates\":[[[%s,%s],[%s,%s],[%s,%s],[%s,%s],[%s,%s]]]}" % (wlon,slat,wlon,nlat,elon,nlat,elon,slat,wlon,slat)
                      if desc :
                        extra['value']=desc
                    elif extra['key'] == 'TemporalCoverage':
                      desc,stime,etime=CV.map_temporal(extra['value'])
                      if desc:
                        extra['value']=desc
                    elif extra['key'] == 'Language':
                      extra['value'] = CV.map_lang(extra['value'])
                    elif extra['key'] == 'PublicationYear':
                      publdate=CV.date2UTC(extra['value'])
                      extra['value'] = CV.cut(extra['value'],'\d\d\d\d',0)
                    elif extra['key'] == 'Contact':
                      extra['value'] = CV.remove_duplicates(extra['value'])
                    if type(extra['value']) is not str and type(extra['value']) is not str :
                      CV.logger.debug(' [INFO] value of key %s has type %s : %s' % (extra['key'],type(extra['value']),extra['value']))
                except Exception as e:
                  CV.logger.info('    | [WARNING] %s : during mapping of field %s with value %s' % (e,extra['key'],extra['value']))
                  continue
        if publdate :
                dataset['extras'].append({"key" : "PublicationTimestamp", "value" : publdate }) 

 	# save output json to file
	save_data(dataset,outFile)

if __name__ == "__main__":
	main()
