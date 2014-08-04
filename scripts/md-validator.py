"""
Binyam Gebrekidan Gebre
@Max Planck Institute, 2014

HOW TO RUN
$ python md-validator.py /path/to/input.json /path/to/output.csv
Input: json file (the input comes from the output of the mapper)
Output: csv file (where the column names are the facets and the rows are datasets)

"""

import numpy as np
import os
import argparse
import re
import simplejson as json
import io
import codecs
import urllib2
import csv
import pycountry

# any facet name can be put in the following list
FACETS = ['Access',
         'Collection',
         'Group', #'Community'
         'author', #'Contributor',
         'Country',
         'CreationDate',
         'Creator',
         'Format', #'DataFormat',
         'MetadataProvider',
         'Discipline',
         'Language',
         'MetadataSchema',
         'MetadataSource',
         'PublicationTimestamp', # CreationDate
         'ResourceType',
         'SpatialCoverage',
         'title', # 'Subject',
         'TemporalCoverage',
         'url']
 
def get_dataset(srcFile):
    """
    reads file from disk and returns json text
    """
    text = open(srcFile).read()
    dataset = json.loads(text)
    return dataset
        
def write2file(validations,dstFile):
    """
    writes validations to file
    """
    if not os.path.isfile(dstFile):
        validations  = [FACETS] + validations
    with open(dstFile, 'ab') as f:
        writer = csv.writer(f)
        writer.writerows(validations)
  
  

def str_equals(str1,str2):
    """
    performs case insensitive string comparison by first stripping trailing spaces 
    """
    return str1.strip().lower() == str2.strip().lower()
    
    
def isUTC(date):
    """
    checks if date is in UTC format
    """
    # UTC format =  YYYY-MM-DDThh:mm:ssZ
    utc = re.compile(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z')
   
    utc_year = re.compile(r'\d{4}') # year (4-digit number)
    if utc.search(date): 
        return True
    else:
        return False
    
def url_exists(url):
    """
    checks if url exists (true or false)
    """ 
    try:
        status_code = urllib2.urlopen(url, timeout=1).getcode()
        return  status_code == 200 # < 400 in check_json.py
    except:
        return False

def country_exists(country):
    """
    checks if country exists
    """
    return country in (c.name for c in pycountry.countries)
    
def language_exists(language):
    """
    checks if language exists
    """
    # language names are accepted but not abbreviations
    language in (l.name for l in pycountry.languages) 
    
    
def is_valid_value(facet,value):
    """
    checks if value is the correct for the given facet
    """
    if str_equals(facet,'PublicationTimestamp'):
        return isUTC(value)
    if str_equals(facet,'url'): 
        return url_exists(value)
    if str_equals(facet,'Language'):
        return language_exists(value)
    if str_equals(facet,'Country'):
        return country_exists(value)
    # to be continued for every other facet
        
    
def validate(dataset, facets="all"):     
    """
    checks the values of facets of the datasets;
    facets could be empty, filled, correctly filled (0,1 or 2)
    """
    validations  = []
    for facet in FACETS:
        value = None
        if facet in dataset:
            value = dataset[facet]
        else:
            for extra in dataset['extras']:
                if str_equals(extra['key'],facet):
                    value = extra['value']                   
        if value:
            if is_valid_value(facet,value):
                validations.append(2)  
            else:
                validations.append(1)  
            
        else:
            validations.append(0)

            
    return [validations]
    
    
def main():
	parser = argparse.ArgumentParser(description='Validates a json file')
	parser.add_argument('srcFile',help='path to an input json file to be validated')
	parser.add_argument('dstFile',help='path to output csv file')
	
	# parse command line arguments
	args = parser.parse_args()
	srcFile = args.srcFile
	dstFile = args.dstFile
	
	if not (srcFile and dstFile):
	    print parser.print_help()
	    exit(1)
	
	# reads input file
 	dataset = get_dataset(srcFile)
 	
 	# validates the json file
 	validations = validate(dataset)
 	
 	# saves output as csv file
	write2file(validations,dstFile)

if __name__ == "__main__":
	main()
