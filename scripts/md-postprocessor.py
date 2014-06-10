"""
Binyam Gebrekidan Gebre
@Max Planck Institute, 2014
"""

import numpy as np
import os
import argparse
import re
import simplejson as json
import io
import codecs


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

def replace(dataset,facetName,old_value,new_value):
    """
    replaces old value with new value for a given facet
    """
    for facet in dataset:
        if facet == facetName and dataset[facet] == old_value:
            dataset[facet] = new_value
            return dataset
        if facet == 'extras':
            for extra in dataset[facet]:
                if extra['key'] == facetName and extra['value'] == old_value:
                    extra['value'] = new_value
                    return dataset
    return dataset
 
def truncate(dataset,facetName,old_value,size):
    """
    truncates old value with new value for a given facet
    """
    for facet in dataset:
        if facet == facetName and dataset[facet] == old_value:
            dataset[facet] = old_value[:size]
            return dataset
        if facet == 'extras':
            for extra in dataset[facet]:
                if extra['key'] == facetName and extra['value'] == old_value:
                    extra['value'] = old_value[:size]
                    return dataset
    return dataset
       

def postprocess(dataset,rules):
    """
    changes dataset field values according to configuration
    """          
    for rule in rules:
        # rules can be checked for correctness
        assert(rule.count(',,') == 5),"a double comma should be used to separate items"
        
        rule = rule.split(',,') # splits  each line of config file 
        groupName = rule[0]
        datasetName = rule[1]
        facetName = rule[2]
        old_value = rule[3]
        new_value = rule[4]
        action = rule[5]
                    
        r = dataset.get("group",None)
        if groupName != '*' and  groupName != r:
            return dataset

        r = dataset.get("name",None)
        if datasetName != '*' and datasetName != r:
            return dataset
            
        if action == "replace":
            dataset = replace(dataset,facetName,old_value,new_value)
        if action == "truncate":
            pass
        if action == "another_action":
            pass
    
    return dataset
    
def main():
	parser = argparse.ArgumentParser(description='Postprocesses a json file')
	parser.add_argument('-i','--srcFile',help='path to an input json file to be postprocessed')
	parser.add_argument('-c','--configFile',help='path to a configuration text file')
	parser.add_argument('-o','--dstFile', help='path to output json file')
	
	# parse command line arguments
	args = parser.parse_args()
	srcFile = args.srcFile
	configFile = args.configFile
	dstFile = args.dstFile
	
	# read input and config files
 	dataset = get_dataset(srcFile)
 	conf_data = get_conf(configFile)
 	
 	# postprocess the json file
 	new_dataset = postprocess(dataset,conf_data)
 	
 	# save output json to file
	save_data(new_dataset,dstFile)

if __name__ == "__main__":
	main()
