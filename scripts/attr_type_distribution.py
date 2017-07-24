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


def generate_distribution(attribute_type_dict):
    """
    Generate token count and length distribution. 
    Goal is to use this information to find attribute types that are concatenations of 
    individual words, e.g. Dietatsampling, Celllineage
    """

    all_attr_distribution = []
    all_attribute_types = attribute_type_dict.keys()
    attr_type_counter = 0

    TIMESTAMP = get_timestamp()


    for attr_type in all_attribute_types:
        attr_type_counter += 1
        formatted_attr_type = attr_type.lower()
        formatted_attr_type = formatted_attr_type.replace(" ", "_")

        if attr_type_counter <= int(args.num_attr_review):
            token_distribution = {}

            num_attr_tokens = len(re.findall(r'\w+', attr_type))
            token_distribution["token_count"] = num_attr_tokens


            token_lengths_list = []
            attr_distribution = {}

            for token in attr_type.split():
                token_length_dict = {}
                token_length_dict[token] = len(token)
            
                token_lengths_list.append(token_length_dict)

            token_distribution["token_lengths"] = token_lengths_list

            attr_distribution[formatted_attr_type] = token_distribution

            all_attr_distribution.append(attr_distribution)
    
    return all_attr_distribution


def calculate_token_count_distribution(all_attr_distribution):
    """
    Plot distribution of token count per attribute type.
    """
    all_attr_type_count_list = [] # List of all counts of attribute tokens
    all_attr_token_length_list = [] # List of all lengths of attribute tokens

    for attr_type_obj in all_attr_distribution:
        key = attr_type_obj.keys()
        token_count = attr_type_obj[key[0]]["token_count"]
        all_attr_type_count_list.append(token_count)

        for token_key in attr_type_obj[key[0]]["token_lengths"]:
            for token,token_length in token_key.iteritems():
                all_attr_token_length_list.append(token_length)


    # Prepare to plot token count distribution
    # Convert to ndarray, https://docs.scipy.org/doc/numpy/user/basics.creation.html
    # print len(all_attr_type_count_list)
    x_token_counts = np.array(all_attr_type_count_list)

    # print len(all_attr_token_length_list)
    x_token_lengths = np.array(all_attr_token_length_list)


    _plot_attr_token_counts(x_token_counts)
    _plot_attr_token_lengths(x_token_lengths)


def _plot_attr_token_counts(x_token_counts):
    """ 
    Plot histogram of token counts. 
    """
    # Generate Histogram, https://matplotlib.org/users/screenshots.html
    num_bins = 30
    fig, ax = plt.subplots()
    n, bins, patches = ax.hist(x_token_counts, num_bins, normed=0, log=True)
    # ax.plot(bins, '--')
    ax.set_xlabel('Token Count')
    ax.set_ylabel('Bin Size')
    ax.set_title(r'Attribute Type Token Count')

    # Tweak spacing to prevent clipping of ylabel
    fig.tight_layout()
    plt.show()


def _plot_attr_token_lengths(x_token_lengths):
    """ 
    Plot histogram of token lengths. 
    """
    # Generate Histogram, https://matplotlib.org/users/screenshots.html
    num_bins = 30
    fig, ax = plt.subplots()
    n, bins, patches = ax.hist(x_token_lengths, num_bins, normed=0, log=True)
    # ax.plot(bins, '--')
    ax.set_xlabel('Token Length')
    ax.set_ylabel('Bin Size')
    ax.set_title(r'Attribute Type Token Length')

    # Tweak spacing to prevent clipping of ylabel
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

    # Generate token length distribution
    all_attr_distribution = generate_distribution(attribute_type_dict)

    # Calculate token count distribution
    calculate_token_count_distribution(all_attr_distribution)
