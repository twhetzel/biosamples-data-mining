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
        # NOTE: Might want to handle decimal place separate since it is a valid part of a number
        # NOTE: May want to have separate patterns for certain characters since they may make sense as 
        # part of a number _or_ string for an attribute type _or_ value, but not all combinations
        if re.search("[:!-/@#=%&_.^$*?+'[\](){}]", self.data):
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
            if len(self.data) > 1:
                if (self.data)[1] in '0123456789.':
                    return True
            else:
                #TODO: Log this case in some way 
                pass
        else:
            if str(self.data)[0] in '0123456789':
                return True


    def check_for_boolean_value(self):
        formatted_value = self.data.lower().strip()
        # print "Boolean Check: ", "\""+self.data+"\"", "\""+formatted_value+"\""
        if formatted_value == 'true' or formatted_value == 'false' \
        or formatted_value == 'y' or formatted_value == 'n' \
        or formatted_value == 'yes' or formatted_value == 'no':
            return True


    def check_for_unknowns(self):
        formatted_value = self.data.lower().strip()

        if formatted_value == 'unknown' or formatted_value == 'unspecified' \
        or formatted_value == 'not provided' or formatted_value == 'not determined' \
        or formatted_value == 'not available' or formatted_value == 'not given' \
        or formatted_value == '?':
            return True


    def check_for_measurement_attribute_types(self):
        formatted_value = self.data.lower().strip()

        # Check for attribute types like 100g or 50um or 4nf kb or 5 year or 9 bp or ppm or ul
        measurement_related_tokens = ['g', 'um', 'kb', 'year', 'bp', 'ppm', 'ul']
        for token in measurement_related_tokens:
            if (' ' + token + ' ') in (' ' + formatted_value + ' '):
                return True
 

    # EVALUATIONS FOR ATTRIBUTE TYPES
    # Mainly for Attribute types
    def check_for_sequence_mentions(self):
        formatted_value = self.data.lower().strip()

        sequence_related_tokens = ['16s', 'prime', 'fastq', 'cdna', 'probe', 'rna', 'vector', 'sequenc']
        for token in sequence_related_tokens:
            if (' ' + token + ' ') in (' ' + formatted_value + ' '):
                return True


    def check_for_sequence_strings(self):
        formatted_value = self.data.lower().strip()

        for token in formatted_value.split():
            #NOTE: This is a little noisy, but definitely picks up strings that are just sequences
            sequence_string_characters = set('agct')
            if set(token) <= sequence_string_characters:
                return True


    # Mainly for Attribute types
    def check_for_age_mentions(self):
        formatted_value = self.data.lower().strip()
        age_related_tokens = ['age', 'day', 'differen', 'time']

        for token in age_related_tokens:
            # https://stackoverflow.com/questions/5319922/python-check-if-word-is-in-a-string
            if (' ' + token + ' ') in (' ' + formatted_value + ' '):
                return True


    # Mainly for Attribute types
    def check_for_antibody_mentions(self):
        formatted_value = self.data.lower().strip()
        if 'antibody' in formatted_value:
            return True

    # Mainly for Attribute types
    def check_for_weight_mentions(self):
        formatted_value = self.data.lower().strip()

        weight_related_tokens = ['weight', 'bmi']
        for token in weight_related_tokens:
            if (' ' + token + ' ') in (' ' + formatted_value + ' '):
                    return True

    # Mainly for Attribute types
    def check_for_id_mentions(self):
        formatted_value = self.data.lower().strip()

        if (' ' + 'id' + ' ') in (' ' + formatted_value + ' '):
            return True


    # Mainly for Attribute types
    def check_for_cell_mentions(self):
        formatted_value = self.data.lower().strip()

        if (' ' + 'cell' + ' ') in (' ' + formatted_value + ' '):
            return True


    # Mainly for Attribute types
    def check_for_clinical_mentions(self):
        formatted_value = self.data.lower().strip()

        clinical_related_tokens = ['clinical', 'patient']
        for token in clinical_related_tokens:
            if (' ' + token + ' ') in (' ' + formatted_value + ' '):
                return True


    # Mainly for Attribute types
    def check_for_disease_mentions(self):
        formatted_value = self.data.lower().strip()

        if 'disease' in formatted_value:
            return True


    # Other Attribute Type token clusters: donor, embryo, (Env, Environment, Environmental), Facs, Gen, Grow, 
    # Histol, Host, Infect, Organi, Prote, Samp, Strain, Treatm, Vioscreen, 


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


def profile_attribute_type_values(attribute_type_dict, all_file_names):
    """
    For each attribute type, profile the values found for this type.
    """
    all_attribute_types = attribute_type_dict.keys()
    attr_type_counter = 0

    TIMESTAMP = get_timestamp()
    outfile = open("attr_type_values_profiling_results_"+TIMESTAMP+".csv", "w")
    csvout = csv.writer(outfile)

    csvout.writerow(["Count", "Attr", \
        "Attr Special Char(CNT)", "Attr Special Char(%)", \
        "Attr Contains Nums (CNT)", "Attr Contains Nums(%)",  "Attr Starts wNumber(CNT)", "Attr Starts wNumber(%)", \
        "Attr Only Nums(CNT)", "Attr Only Nums(%)", "Attr Boolean(CNT)", "Attr Boolean(%)", \
        "Attr Unknown(cnt)", "Attr Unknown(%)", \
        "Attr Age Related(CNT)", " Attr Age Related(%)", "Attr Antibody Related(CNT)", "Attr Antibody Related(%)", \
        "Attr Weight Related(CNT)", "Attr Weight Related(%)", "Attr ID Related(CNT)", "Attr ID Related(%)", \
        "Attr Clinical Related(CNT)", "Attr Clinical Related(%)", "Attr Disease Related(CNT)", "Attr Disease Related(%)", \
        "Attr Sequence Related(CNT)", "Attr Sequence Related(%)", "Attr Seq Strings(CNT)", "Attr Seq Strings(%)", \
        "Value Special Char(CNT)", "Value Special Char(%)", \
        "Value Contains Nums (CNT)", "Value Contains Nums(%)",  "Value Starts wNumber(CNT)", "Value Starts wNumber(%)", \
        "Value Only Nums(CNT)", "Value Only Nums(%)", "Value Boolean(CNT)", "Value Boolean(%)", \
        "Value Unknown(cnt)", "Value Unknown(%)", \
        "Value Age Related(CNT)", "Value Age Related(%)", "Value Antibody Related(CNT)", "Value Antibody Related(%)", \
        "Value Weight Related(CNT)", "Value Weight Related(%)", "Value ID Related(CNT)", "Value ID Related(%)", \
        "Value Clinical Related(CNT)", "Value Clinical Related(%)", "Value Disease Related(CNT)", "Value Disease Related(%)", \
        "Value Sequence Related(CNT)", "Value Sequence Related(%)", "Value Seq Strings(CNT)", "Value Seq Strings(%)"])


    for attr_type in all_attribute_types:
        attr_type_counter += 1

        values_with_special_chars = {}
        values_contain_numbers = {}
        values_starts_with_numbers = {}
        values_only_numbers = {}
        values_boolean = {}
        values_unknown = {}
        values_age = {}
        values_antibody = {}
        values_weight = {}
        values_id = {}
        values_clinical = {}
        values_disease = {}
        values_sequence = {}
        values_sequence_strings = {}

        count_values_with_special_characters = 0
        count_values_with_numbers = 0
        count_values_starts_with_numbers = 0
        count_values_only_numbers = 0
        count_values_boolean = 0
        count_values_unknown = 0
        count_values_age = 0
        count_values_antibody = 0
        count_values_weight = 0
        count_values_id = 0
        count_values_clinical = 0
        count_values_disease = 0
        count_values_sequence = 0
        count_values_sequence_strings = 0

        flagged_values_contain_special_characters = []
        flagged_values_contain_numbers = []
        flagged_values_starts_with_numbers = []
        flagged_values_only_numbers = []
        flagged_values_boolean = []
        flagged_values_unknown = []
        flagged_values_age = []
        flagged_values_antibody = []
        flagged_values_weight = []
        flagged_values_id = []
        flagged_values_clinical = []
        flagged_values_disease = []
        flagged_values_sequence = []
        flagged_values_sequence_strings = []


        attr_type_value_count = attribute_type_dict[attr_type]
        # print "Attribute type ", attr_type, "has %s values " % attr_type_value_count

        # format attribute type the same as attribute filename 
        attribute_type_filename = attr_type.lower()
        attribute_type_filename = attribute_type_filename.replace(" ", "_")
        attribute_type_filename = attribute_type_filename+".csv"

        if attr_type_counter <= int(args.num_attr_review):
            print "\n** Results for Attribute Type ", attr_type, "("+str(attr_type_counter)+")"
            #TODO: Add args for content to profile, e.g. attr_type, values, or all
            #TODO: Add if args set to profile attr_type, then build out tests of Profiler methods

            #TODO: Add if args set to profile values, then execute code below
            # read attribute file
            with open(args.dir+attribute_type_filename, "r") as attr_value_file:
                
                content = attr_value_file.readlines()
                content = [x.strip(' \t\n\r') for x in content]
                
                for item in content:
                    value, val_count, iri = item.split('\t')
                    # print "** Line: ", value.strip(), val_count.strip(), iri.strip() # Do these need strip()?

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

                    if value_profiler.check_for_boolean_value():
                        flagged_values_boolean.append(value)
                        count_values_boolean += int(val_count)

                    if value_profiler.check_for_unknowns():
                        flagged_values_unknown.append(value)
                        count_values_unknown += int(val_count)

                    if value_profiler.check_for_age_mentions():
                        flagged_values_age.append(value)
                        count_values_age += int(val_count)

                    if value_profiler.check_for_antibody_mentions():
                        flagged_values_antibody.append(value)
                        count_values_antibody += int(val_count)

                    if value_profiler.check_for_weight_mentions():
                        flagged_values_weight.append(value)
                        count_values_weight += int(val_count)

                    if value_profiler.check_for_id_mentions():
                        flagged_values_id.append(value)
                        count_values_id += int(val_count)

                    if value_profiler.check_for_clinical_mentions():
                        flagged_values_clinical.append(value)
                        count_values_clinical += int(val_count)

                    if value_profiler.check_for_disease_mentions():
                        flagged_values_disease.append(value)
                        count_values_disease += int(val_count)

                    if value_profiler.check_for_sequence_mentions():
                        flagged_values_sequence.append(value)
                        count_values_sequence += int(val_count)

                    if value_profiler.check_for_sequence_strings():
                        flagged_values_sequence_strings.append(value)
                        count_values_sequence_strings += int(val_count)


            # print "Values that start with numbers: \n", flagged_values_starts_with_numbers
            if flagged_values_unknown:
                print "Values that are Unknown Related: \n", flagged_values_unknown


            # print summary reports
            csvout.writerow([str(attr_type_counter), attr_type, \
                str(count_values_with_special_characters), \
                str(("%.2f" % (count_values_with_special_characters/float(attribute_type_dict[attr_type])*100))), \
                str(count_values_with_numbers), \
                str(("%.2f" % (count_values_with_numbers/float(attribute_type_dict[attr_type])*100))), \
                str(count_values_starts_with_numbers), \
                str(("%.2f" % (count_values_starts_with_numbers/float(attribute_type_dict[attr_type])*100))), \
                str(count_values_only_numbers),
                str(("%.2f" % (count_values_only_numbers/float(attribute_type_dict[attr_type])*100))), \
                str(count_values_boolean),
                str(("%.2f" % (count_values_boolean/float(attribute_type_dict[attr_type])*100))), \
                str(count_values_unknown),
                str(("%.2f" % (count_values_unknown/float(attribute_type_dict[attr_type])*100))), \
                str(count_values_age),
                str(("%.2f" % (count_values_age/float(attribute_type_dict[attr_type])*100))), \
                str(count_values_antibody),
                str(("%.2f" % (count_values_antibody/float(attribute_type_dict[attr_type])*100))), \
                str(count_values_weight),
                str(("%.2f" % (count_values_weight/float(attribute_type_dict[attr_type])*100))), \
                str(count_values_id),
                str(("%.2f" % (count_values_id/float(attribute_type_dict[attr_type])*100))), \
                str(count_values_clinical),
                str(("%.2f" % (count_values_clinical/float(attribute_type_dict[attr_type])*100))), \
                str(count_values_disease),
                str(("%.2f" % (count_values_disease/float(attribute_type_dict[attr_type])*100))), \
                str(count_values_sequence),
                str(("%.2f" % (count_values_sequence/float(attribute_type_dict[attr_type])*100))), \
                str(count_values_sequence_strings),
                str(("%.2f" % (count_values_sequence_strings/float(attribute_type_dict[attr_type])*100)))
            ])

    outfile.close()
    print "\n** Profiling report generated."


if __name__ == '__main__':
    """ 
    Profile attribute values for each attribute type. 
    """
    print "Starting to profile atttribute values..."

    # Commandline arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--dir', default="/Users/twhetzel/git/biosamples-data-mining/data_results/attr_type_values-data_results/")
    parser.add_argument('--attr_type_file_path', default="/Users/twhetzel/git/biosamples-data-mining/data_results/unique_attr_types_2017-06-20_14-31-00.csv")
    parser.add_argument('--num_attr_review', help="Number of Attributes to analyze their values", default=16000)
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


