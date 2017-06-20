from neo4j.v1 import GraphDatabase, basic_auth
import argparse
import os, sys
import csv, unicodecsv
import string, re
import requests, json

import nltk
from nltk.corpus import stopwords


class Mapping:
    def __init__(self, label, iri, confidence):
        self.label = label
        self.iri = iri
        self.confidence = confidence

    def __str__(self):
        try:
            return "Mapping:\label={label:s}, iri={iri:s}," \
                    "confidence={confidence:s}".format(
                    label=self.label, iri=self.iri,
                    confidence=self.confidence
                )
        except UnicodeDecodeError:
            print "Something went wrong handling mapping for " #+ self.value


def read_file():
    """ Read file of common attributes. """
    with open(args.file_path, 'r') as f:
        content = f.readlines()

    # Strip lines of newline/return characters in csv file
    content = [x.strip(' \t\n\r') for x in content]

    # Generate dictionary of types and their count
    attribute_type_dict = {}
    for item in content:
        key, value = item.split('|')
        attribute_type_dict[key.strip()] = value.strip()
    
    return attribute_type_dict


def profile_attribute_types(attribute_type_dict, num_attr_types):
    """ Profile attribute types. """
    attr_with_special_chars = {}
    attr_contains_numbers = {}
    attr_starts_with_numbers = {}
    attr_only_numbers = {}
    attr_num_of_tokens_distribution = {}

    count = 0
    
    all_attribute_types = attribute_type_dict.keys()
    for attr_type in all_attribute_types:
        count += 1
        # print count
        # How many terms contain a character that is not a number or alphabetic character
        if re.match("[^A-Za-z0-9]+", attr_type):
            # print "** Contains special char: ", attr_type
            attr_with_special_chars[attr_type] = attribute_type_dict[attr_type]

        # How many types contain a number?
        RE_D = re.compile('\d')
        if RE_D.search(attr_type):
            # print "Contains a number.", attr_type
            attr_contains_numbers[attr_type] = attribute_type_dict[attr_type]

        # How many types start with a number?
        if attr_type[:1] in '0123456789':
            # print "Starts with a number: ", attr_type
            attr_starts_with_numbers[attr_type] = attribute_type_dict[attr_type]

        # How many types are only numbers?
        if attr_type.isdigit():
            # print "Attribute type is a number: ", attr_type
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
        if count % 100 == 0:  #progress indicator
            print '...', count
            # sys.stdout.write("...")
        ols_mapping_results = _get_ols_annotations(attr_type)
        if ols_mapping_results is not None:
            print attr_type, ols_mapping_results

        # TODO
        # For each token in each attr_type, are any tokens found in other attr_types?
        # _word_token_analysis(all_attribute_types) 
        # --> See token_analysis.py script for this analysis

 
    # Generate summary messages
    # Special Characters
    if len(attr_with_special_chars) == 0:
        print "-- No attributes contain special characters"
    else:
        pass

    # Numbers
    total_samples_contain_numbers = 0
    if len(attr_contains_numbers) == 0:
        print "-- No attributes with numbers"
    else:
        print "-- Attributes that contain numbers: ", len(attr_contains_numbers)
        for k,v in attr_contains_numbers.iteritems():
            total_samples_contain_numbers = total_samples_contain_numbers + int(v)

    total_samples_start_with_numbers = 0
    if len(attr_starts_with_numbers) == 0:
        print "-- No attributes start with numbers"
    else:
        print "-- Attributes that start with numbers: ", len(attr_starts_with_numbers)
        for k,v in attr_starts_with_numbers.iteritems():
            total_samples_start_with_numbers = total_samples_start_with_numbers + int(v)

    total_samples_only_numbers = 0
    if len(attr_only_numbers) == 0:
        print "-- No attributes are only numbers"
    else:
        print "-- Attributes that are only numbers: ", len(attr_only_numbers)
        for k,v in attr_only_numbers.iteritems():
            total_samples_only_numbers = total_samples_only_numbers + int(v)




def profile_attribute_values(attribute_type_dict, attr, num_attr_values):
    """ Profile attribute values for a given attribute type."""
    print "Profiling attribute values..."

    # Get attribute name and count from file
    attr_count = attribute_type_dict[attr]  

    # How many attributes have an iri?

    # How many of these iris are from the same ontology based on ontology iri/namespace?


def _get_ols_annotations(attr_type, ontology=None):
    """ Get annotations using Zooma. """
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


    #         mapping = Mapping(
                
    #         )
    # print "* * Mapping: ", mapping
    # return mapping


def _word_token_analysis(all_attribute_types):
    # Notes from: http://www.cs.duke.edu/courses/spring14/compsci290/assignments/lab02.html
    lower_case_attr_types = [x.lower() for x in all_attribute_types]
    # print "LC: ", lower_case_attr_types

    no_punc_attr_type_list = []
    for attr_type in lower_case_attr_types:
        no_punctuation_attr_type = attr_type.translate(None, string.punctuation)
        # print "No Punc: ", no_punctuation_attr_type
        no_punc_attr_type_list.append(no_punctuation_attr_type)

    # Tokenize 
    tokens = nltk.word_tokenize(no_punc_attr_type_list)
    print "** Tokens: ", tokens

    for attr_type in no_punc_attr_type_list[:20]:
        # print "Attr_type: ", attr_type
        pass
                




if __name__ == '__main__':
    """ Profile values for each attribute type. """
    print "Starting to profile atttribute types."

    driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "neo4jebi"))

    # Commandline arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--file_path', default="/Users/twhetzel/git/tw-biosamples-analysis/data_results/common_attributes/attr_common.csv")
    parser.add_argument('--attr', default="Organism")
    parser.add_argument('--num_attr_types', default=15662) # total in file is 15662
    parser.add_argument('--num_attr_values', default=500)
    args = parser.parse_args()

    attribute_type_dict = read_file()

    profile_attribute_types(attribute_type_dict, args.num_attr_types)

    # profile_attribute_values(attribute_type_dict, args.attr, args.num_attr_values)



