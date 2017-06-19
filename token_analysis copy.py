from neo4j.v1 import GraphDatabase, basic_auth
import argparse
import os, sys
import csv, unicodecsv
import string, re
import requests, json

import nltk
from nltk.corpus import stopwords

import networkx as nx
import matplotlib.pyplot as plt
import pandas


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


def generate_tokens(all_attribute_types):
    """ Convert attribute types to lowercase tokens without punctuation or stopwords."""
    # Notes from: http://www.cs.duke.edu/courses/spring14/compsci290/assignments/lab02.html
    lower_case_attr_types = [x.lower() for x in all_attribute_types]

    no_punc_attr_type_list = []
    print "-- convert attributes to lowercase..."
    for attr_type in lower_case_attr_types:
        no_punctuation_attr_type = attr_type.translate(None, string.punctuation)
        # print "No Punc: ", no_punctuation_attr_type
        no_punc_attr_type_list.append(no_punctuation_attr_type)


    print "-- tokenize attributes ..."
    attr_token_dict = {}
    NUMBER_ATTR_TO_EXAMINE = 15662 # 15662 all 
    for attr_type in no_punc_attr_type_list[:NUMBER_ATTR_TO_EXAMINE]:
        
        # Tokenize    
        tokens = nltk.word_tokenize(attr_type)
        # print "\n** Tokens: ", tokens
        
        # Remove stop words from tokens
        filtered = [w for w in tokens if not w in stopwords.words('english')]
        # print "** Filter: ", filtered

        #TODO: Incorporate lemmatization into analysis to convert tokens to base form
        # https://nlp.stanford.edu/IR-book/html/htmledition/stemming-and-lemmatization-1.html

        #TODO: Consider generating list of most common words just as background information
        # nltk.FreqDist(all_words)

        # Store tokens in dictionary
        attr_token_dict[attr_type] = filtered

    return attr_token_dict


def word_token_analysis(attr_token_dict):
    """ For each token, check if it exists as a token in another attribute type."""
    matches = []
    attr_token_matches = []
    editable_dict = attr_token_dict.copy()

    for attr_type, token_list in attr_token_dict.iteritems():
        # remove attribute to be examined from dict
        del editable_dict[attr_type]
        match_tuple = ()
        # print "\nAT: ", attr_type
        for token in token_list:
            # print "T: ", token
            for k,v_list in editable_dict.iteritems():
                if token in v_list:
                    match_tuple = (attr_type, k, token)
                    matches.append(match_tuple)

    # Print Matches found for each attr_type
    if matches:
        print "** Matches: ", len(matches)

    return matches




# Main 
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

    # Methods
    attribute_type_dict = read_file()
    all_attribute_types = attribute_type_dict.keys()
    # print "All Attributes: ", len(all_attribute_types)
    
    attr_token_dict = generate_tokens(all_attribute_types)

    attr_token_matches = word_token_analysis(attr_token_dict)
    print type(attr_token_matches)

    # Write results of tuples to file
    with open("tokenization_results.csv", "w") as att_type_out:
        att_type_out.write('\n'.join('%s | %s | %s' % x for x in attr_token_matches))







