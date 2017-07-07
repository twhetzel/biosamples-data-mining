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

import decimal
import datetime


class Profiler:
    """
    Provides methods to calculate characteristics of attribute types and values.
    """
    def __init__(self, data):
        self.data = data

    def check_for_special_characters(self):
        # NOTE: Might want to check if not a digit or character
        if re.search("[?!@#$%^&*()]_", self.data):   # Is there a way to detect 3'
            return True

    def check_for_numbers(self):
        RE_D = re.compile('\d')
        if RE_D.search(self.data):
            return True
 
    def check_if_only_numbers(self):
        if all(c in "0123456789.-E" for c in self.data):
            return True


    def check_if_starts_with_number(self):
        if str(self.data)[0] == '-':
            if (self.data)[1] in '0123456789.':
                return True
        else:
            if str(self.data)[0] in '0123456789':
                return True



# Methods
def get_timestamp():
    """ 
    Get timestamp of current date and time. 
    """
    timestamp = '{:%Y-%m-%d_%H-%M-%S}'.format(datetime.datetime.now())
    return timestamp


def get_file_names():
    """
    Get list of all filenames to examine.
    """
    all_file_names = []
    cwd = os.getcwd()
    # Change to dir with result files to analyze
    os.chdir(args.dir)
    
    for file in glob.glob("*.csv"):
        all_file_names.append(file)

    # Return to current working directory
    os.chdir(cwd)
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
        if re.search("[?!@#$%^&*()]_", attr_type):   # Is there a way to detect the prime in 3'
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
        #TODO: Need to account for value that are decimal point numbers, e.g. 12.5
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
    attr_type_count = 0

    TIMESTAMP = get_timestamp()
    outfile = open("attr_type_values_profiling_results_"+TIMESTAMP+".csv", "w")
    csvout = csv.writer(outfile)

    csvout.writerow(["Count", "Attr", "Special Char(CNT)", "Special Char(%)", \
        "Contains Nums (CNT)", "Contains Nums(%)",  "Starts wNumber(CNT)", "Starts wNumber(%)", \
        "Only Nums(CNT)", "Only Nums(%)"])

    for attr_type in all_attribute_types:
        attr_type_count += 1

        values_with_special_chars = {}
        values_contain_numbers = {}
        values_starts_with_numbers = {}
        values_only_numbers = {}

        count_values_with_special_characters = 0
        count_values_with_numbers = 0
        count_values_starts_with_numbers = 0
        count_values_only_numbers = 0
        flagged_values_contain_special_characters = []
        flagged_values_contain_numbers = []
        flagged_values_starts_with_numbers = []
        flagged_values_only_numbers = []

        attr_type_value_count = attribute_type_dict[attr_type]
        # print "Attribute type ", attr_type, "has %s values " % attr_type_value_count

        # format attribute type the same as attribute filename 
        attribute_type_filename = attr_type.lower()
        attribute_type_filename = attribute_type_filename.replace(" ", "_")
        attribute_type_filename = attribute_type_filename+".csv"

        if attr_type_count < 10:  # OR switch to args param to pass attr to examine?
            print "\n** Results for Attribute Type ", attr_type, "("+str(attr_type_count)+")"
            # read attribute file
            with open(args.dir+attribute_type_filename, "r") as attr_value_file:
                
                content = attr_value_file.readlines()
                content = [x.strip(' \t\n\r') for x in content]
                
                for item in content:
                    value, val_count, iri = item.split('\t')
                    # print "Line: ", value.strip(), val_count.strip(), iri.strip(), "\n" # Do these need strip()?

                    value_profiler = Profiler(value)

                    if value_profiler.check_for_special_characters():
                        flagged_values_contain_special_characters.append(value)
                        count_values_with_special_characters += int(val_count)

                    if value_profiler.check_for_numbers():
                        flagged_values_contain_numbers.append(value)
                        count_values_with_numbers += int(val_count)

                    # only check for these cases if any values contain numbers
                    if len(flagged_values_contain_numbers) > 0:
                        # print "Checking if values are only numbers..."
                        if value_profiler.check_if_only_numbers():
                            flagged_values_only_numbers.append(value)
                            count_values_only_numbers += int(val_count)
                    
                    if value_profiler.check_if_starts_with_number():
                        flagged_values_starts_with_numbers.append(value)
                        count_values_starts_with_numbers += int(val_count)


            # print "Values that start with numbers: \n", flagged_values_starts_with_numbers
            print "Values that are ONLY numbers: \n", flagged_values_only_numbers

            # print summary reports
            csvout.writerow([str(attr_type_count), attr_type, \
                str(count_values_with_special_characters), \
                str(("%.2f" % (count_values_with_special_characters/float(attribute_type_dict[attr_type])*100))), \
                str(count_values_with_numbers), \
                str(("%.2f" % (count_values_with_numbers/float(attribute_type_dict[attr_type])*100))), \
                str(count_values_starts_with_numbers), \
                str(("%.2f" % (count_values_starts_with_numbers/float(attribute_type_dict[attr_type])*100))), \
                str(count_values_only_numbers),
                str(("%.2f" % (count_values_only_numbers/float(attribute_type_dict[attr_type])*100)))
            ])

    outfile.close()
    print "\n** Profiling report generated."


# Profiling methods
def _check_for_special_characters(value, attribute_type_filename):
    # For review of values, create dict with filename as key, and list of values w/special char as dict value
    values_with_special_chars = {} #move this!
    flagged_values = []
    if re.search("[?!@#$%^&*()]_", value):   # Is there a way to detect 3'
            print "** Value contains special char: ", value
            flagged_values.append(values)
            values_with_special_chars[attribute_type_filename] = flagged_values
    # else:
    #     print "-- No special character found in ", value
    return values_with_special_chars


def _check_for_numbers(value, attribute_type_filename, values_contain_numbers):
    # print "Checking value for numbers...", value
    # values_contain_numbers = {}
    # values_starts_with_numbers = {}
    # values_only_numbers = {}

    flagged_values_contain_numbers = []

    # check if value contains numbers
    RE_D = re.compile('\d')
    if RE_D.search(value):
        # print "-- contains a number", value
        flagged_values_contain_numbers.append(value)
        values_contain_numbers[attribute_type_filename] = flagged_values_contain_numbers

    return values_contain_numbers



if __name__ == '__main__':
    """ 
    Profile attribute values for each attribute type. 
    """
    print "Starting to profile atttribute values..."

    # Commandline arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--dir', default="/Users/twhetzel/git/biosamples-data-mining/data_results/attr_type_values-data_results/")
    parser.add_argument('--attr_type_file_path', default="/Users/twhetzel/git/biosamples-data-mining/data_results/unique_attr_types_2017-06-20_14-31-00.csv")
    args = parser.parse_args()


    # Main Methods
    # Get list of files that contain values for each attr type. 
    # NOTE: The query to get values has a limit to get only 10,000 values 
    all_file_names = get_file_names()

    # Get attr_type file with count of attr per type
    attribute_type_dict = read_attr_type_file()

    # Generate profiles for attr types
    # profile_attribute_types(attribute_type_dict)

    # Generate profiles for attr type values 
    profile_attribute_type_values(attribute_type_dict, all_file_names)


