#!/usr/bin/env python

"""
Binyam Gebrekidan Gebre
@Max Planck Institute, 2014

HOW TO RUN
$ python md-postprocessor.py -i /path/to/input.json -o /path/to/output.json -c /path/to/config.txt
Input: json file (the input comes from the output of the mapper)
Configuration file (this is a text file, where actions or rules are specified, In the scripts folder, you can see the format of the config.tx)
Output:json file (the output of the postprocessor is another json file ready to be validated and/or uploaded to CKAN) 

"""

import numpy as np
import os,sys
import argparse
import re
import simplejson as json
import io
import codecs
import time

# add parent directory to python library searching paths
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import B2FIND

def get_dataset(srcFile):
    """
    reads file from disk and returns json text
    """
    text = open(srcFile).read()
    dataset = json.loads(text)
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
    with io.open(dstFile, 'w', encoding='utf-8') as f:
        f.write(unicode(json.dumps(dataset, indent = 4,ensure_ascii=False)))

def main():
	parser = argparse.ArgumentParser(description='Postprocesses a json file')
	parser.add_argument('-i','--srcFile',help='path to an input json file to be postprocessed')
	parser.add_argument('-c','--configFile',help='path to a configuration text file')
	parser.add_argument('-o','--dstFile', help='path to output json file')
	parser.add_argument('-j','--jobdir', help='path to log dir', default='log')
	parser.add_argument('-v','--verbose', help='verbose mode', default=False)
	
	# parse command line arguments
	args = parser.parse_args()
	srcFile = args.srcFile
	configFile = args.configFile
	dstFile = args.dstFile
	
	if not (srcFile and configFile and dstFile):
	    print parser.print_help()
	    exit(1)

        # make jobdir
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        jid = os.getpid()
        pstat=[]
    
        # create logger and OUT output handler and initialise it:
        OUT = B2FIND.OUTPUT(pstat,now,jid,args)

        # create CONVERTER object:
        CV = B2FIND.CONVERTER(OUT,'../../mapper/current')
	
	# read input and config files
 	dataset = get_dataset(srcFile)
 	conf_data = get_conf(configFile)
 	
 	# postprocess the json file
 	new_dataset = CV.postprocess(dataset,conf_data)
 	
 	# save output json to file
	save_data(new_dataset,dstFile)

if __name__ == "__main__":
	main()
