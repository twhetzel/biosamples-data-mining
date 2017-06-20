from neo4j.v1 import GraphDatabase, basic_auth
import argparse
import os, sys
import csv, unicodecsv
import string, re
import requests, json

import nltk
from nltk.corpus import stopwords

import glob, os
import cPickle as pickle


def get_file_names():
    all_file_names = []
    os.chdir(args.dir)
    for file in glob.glob("*.csv"):
        all_file_names.append(file)

    return all_file_names


def read_attr_type_file():
    """ Read file of common attributes. """
    with open(args.attr_type_file_path, 'r') as f:
        content = f.readlines()

    # Strip lines of newline/return characters in csv file
    content = [x.strip(' \t\n\r') for x in content]

    # Generate dictionary of types and their count
    attribute_type_dict = {}
    for item in content:
        key, value = item.split('|')
        attribute_type_dict[key.strip()] = value.strip()
    
    return attribute_type_dict


def profile_attribute_types(attribute_type_dict):
    """ 
    Profile attribute types. 
    """
    
    # How many attributes have an iri? Not reliable since not consistently used
    # How many of these iris are from the same ontology based on ontology iri/namespace?

    attr_with_special_chars = {}
    attr_contains_numbers = {}
    attr_starts_with_numbers = {}
    attr_only_numbers = {}
    attr_num_of_tokens_distribution = {}

    
    all_attribute_types = attribute_type_dict.keys()
    
    for attr_type in all_attribute_types:
        attr_type_value_count = attribute_type_dict[attr_type]
        
        # How many terms contain a character that is not a number or alphabetic character
        # if re.match("[^A-Za-z0-9]+", attr_type):
        if re.search("[?!@#$%^&*()]_", attr_type):   # Is there a way to detect 3'
            print "** Contains special char: ", attr_type
            attr_with_special_chars[attr_type] = attribute_type_dict[attr_type]

        # How many types contain a number?
        RE_D = re.compile('\d')
        if RE_D.search(attr_type):
            print "Contains a number.", attr_type
            attr_contains_numbers[attr_type] = attribute_type_dict[attr_type]

        # How many types start with a number?
        if attr_type[:1] in '0123456789':
            print "Starts with a number: ", attr_type
            attr_starts_with_numbers[attr_type] = attribute_type_dict[attr_type]

        # How many types are only numbers?
        if attr_type.isdigit():
            print "Attribute type is a number: ", attr_type
            attr_only_numbers[attr_type] = attribute_type_dict[attr_type]


        # Count how many tokens exist in each type and plot by length
        attr_type_list = []
        num_attr_tokens = len(re.findall(r'\w+', attr_type))
        # print attr_type, num_attr_tokens
        if num_attr_tokens in attr_num_of_tokens_distribution:
            # Append this attr_type to the list of values of this length, ie where num_attr_tokens is the key
            attr_type_list = attr_num_of_tokens_distribution[num_attr_tokens]
            attr_type_list.append(attr_type)
            # print attr_type_list
        else:
            # Add key and attr_type to list of values
            attr_type_list.append(attr_type)
            attr_num_of_tokens_distribution[num_attr_tokens] = attr_type_list


        # How many types match an ontology, check using Zooma, and 
        # score likelihood of term content based on what the ontology 
        # is used for
        #TODO - work on further, Zooma uri, how to compile results when >1 result is returned
        # if count % 100 == 0:  #progress indicator
        #     print '...', count
            # sys.stdout.write("...")
        # zooma_mapping_results = _get_zooma_annotations(attr_type)
        # if zooma_mapping_results is not None:
        #     print attr_type, zooma_mapping_results

 
    # Generate summary messages
    # Special Characters
    if len(attr_with_special_chars) == 0:
        print "-- No attributes contain special characters"
    else:
        pass

    # Numbers
    # attribute types that contain a number
    total_samples_contain_numbers = 0
    if len(attr_contains_numbers) == 0:
        print "-- No attributes with numbers"
    else:
        print "-- Attributes that contain numbers: ", len(attr_contains_numbers)
        for k,v in attr_contains_numbers.iteritems():
            total_samples_contain_numbers = total_samples_contain_numbers + int(v)

    # attribute types that start with a number
    total_samples_start_with_numbers = 0
    if len(attr_starts_with_numbers) == 0:
        print "-- No attributes start with numbers"
    else:
        print "-- Attributes that start with numbers: ", len(attr_starts_with_numbers)
        for k,v in attr_starts_with_numbers.iteritems():
            total_samples_start_with_numbers = total_samples_start_with_numbers + int(v)

    # attribute types that are only numbers
    total_samples_only_numbers = 0
    if len(attr_only_numbers) == 0:
        print "-- No attributes are only numbers"
    else:
        print "-- Attributes that are only numbers: ", len(attr_only_numbers)
        for k,v in attr_only_numbers.iteritems():
            total_samples_only_numbers = total_samples_only_numbers + int(v)

    # attribute type token distribution
    print "\n-- Attribute length distribution (count):"
    for k,v in attr_num_of_tokens_distribution.iteritems():
        print k, len(v)


def _get_zooma_annotations(attr_type, ontology=None):
    """ 
    Get annotations using Zooma. 
    """
    url = "http://www.ebi.ac.uk/spot/zooma/v2/api/services/annotate?" \
          "propertyValue={attr_type:s}&" \
          "filter=required:[none]," \
          "filter=ontologies:{ontology:s}".format(attr_type=attr_type, ontology=ontology)

    print attr_type, "\n",url
    response = requests.get(url)
    mapping = None
    print attr_type, response
    if response.status_code == 200:
        resource = json.loads(response.content)
        # print "Resource: ", len(resource)
        if resource:
            # Why take only the first response from Zooma if multiples are returned? 
            result = resource[0]
            print "\nResult: ", result
            # print "\n\n** Result Fields: ", result['annotatedProperty']['propertyValue']
            
            # Add these:
            # result['derivedFrom']['uri']
            
            print result['annotatedProperty']['propertyValue'], result['semanticTags'][0], result['confidence']


def profile_attribute_type_values(attribute_type_dict, all_file_names):
    """
    For each attribute type, profile the values found for this type.
    """
    all_attribute_types = attribute_type_dict.keys()

    count = 0
    for k,v in attribute_type_dict.iteritems():
        count += 1
        if count < 10:
            print k,v

    for attr_type in all_attribute_types:
        attr_type_value_count = attribute_type_dict[attr_type]
        # print "Attribute type ", attr_type, "has %s values " % attr_type_value_count

        # format attribute type the same as attribute filename 
        attribute_type_filename = attr_type.lower()
        attribute_type_filename = attribute_type_filename.replace(" ", "_")
        attribute_type_filename = attribute_type_filename+".csv"

        # read attribute file
        with open(args.dir+attribute_type_filename, "r") as attr_value_file:
            content = attr_value_file.readlines()
            # strip lines of newline/return characters in csv file
            content = [x.strip(' \t\n\r') for x in content]
            # print "File contents: ", attr_type, #"\n", content, "\n\n"
            for item in content:
                # print "Content Item: ", item
                value, count, iri = item.split('\t')
                # print "Split: ", value.strip(),"\n"



if __name__ == '__main__':
    """ 
    Profile attribute values for each attribute type. 
    """
    print "Starting to profile atttribute values."

    # Commandline arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--dir', default="/Users/twhetzel/git/biosamples-data-mining/data_results/attr_type_values-data_results/")
    parser.add_argument('--attr_type_file_path', default="/Users/twhetzel/git/biosamples-data-mining/data_results/unique_attr_types_2017-06-20_14-31-00.csv")
    args = parser.parse_args()

    # Methods
    # get list of files that contain values (n<=10000) for each attr type
    all_file_names = get_file_names()

    # get attr_type file with count of attr per type
    attribute_type_dict = read_attr_type_file()

    # profile attr types for characteristics
    # profile_attribute_types(attribute_type_dict)

    # profile attr type values for characteristics
    profile_attribute_type_values(attribute_type_dict, all_file_names)



