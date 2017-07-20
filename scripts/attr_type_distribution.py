import argparse
import os, sys
import csv, unicodecsv
import string, re
import decimal
import datetime

import matplotlib.pyplot as plt
import numpy as np


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


def generate_word_distribution(attribute_type_dict):
    """ 
    Generate word length distribution of attribute types. 
    """

    attr_num_of_tokens_distribution = {}
    all_attribute_types = attribute_type_dict.keys()
    attr_type_counter = 0

    TIMESTAMP = get_timestamp()

    all_attr_type_len_list = [] # List of all lengths of attribute tokens
    
    for attr_type in all_attribute_types:
        attr_type_counter += 1
        attr_type_value_count = attribute_type_dict[attr_type]

        if attr_type_counter <= int(args.num_attr_review):
            # Count how many tokens exist in each type and plot by length
            attr_type_list = []
            num_attr_tokens = len(re.findall(r'\w+', attr_type))
            
            all_attr_type_len_list.append(num_attr_tokens)

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

    # attribute type token distribution
    print "\n-- Attribute length distribution (count):"
    for k,v in attr_num_of_tokens_distribution.iteritems():
        print k, len(v)


    # Convert to ndarray, https://docs.scipy.org/doc/numpy/user/basics.creation.html
    print len(all_attr_type_len_list)
    x = np.array(all_attr_type_len_list)
    # print x

    # Generate Histogram, https://matplotlib.org/users/screenshots.html
    num_bins = 30
    fig, ax = plt.subplots()
    n, bins, patches = ax.hist(x, num_bins, normed=0, log=True)
    # ax.plot(bins, '--')
    ax.set_xlabel('Token Length')
    ax.set_ylabel('Bin Size')
    ax.set_title(r'Attribute Type Word Length')

    # # Tweak spacing to prevent clipping of ylabel
    fig.tight_layout()
    plt.show()


if __name__ == '__main__':
    """ 
    Profile attribute values for each attribute type. 
    """
    print "Generating attr_type word distribution..."

    # Commandline arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--attr_type_file_path', default="/Users/twhetzel/git/biosamples-data-mining/data_results/unique_attr_types_2017-06-20_14-31-00.csv")
    parser.add_argument('--num_attr_review', help="Number of Attributes to analyze their values", default=16000)
    args = parser.parse_args()


    # Main Methods
    # Get attr_type file with count of attr per type
    attribute_type_dict = read_attr_type_file()

    # Generate word length distribution of attribute types
    generate_word_distribution(attribute_type_dict)


