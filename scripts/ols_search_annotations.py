import argparse
from functools import wraps
from time import time
import requests, json
import datetime
import csv, unicodecsv
import itertools
import os


def timing(f):
    """
    Create wrapper to report time of functions.
    """
    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
        print 'Function: %r took: %2.2f sec, %2.2f min' % \
          (f.__name__, te-ts, (te-ts)/60)
        return result
    return wrap


def get_timestamp():
    """ 
    Get timestamp of current date and time. 
    """
    timestamp = '{:%Y-%m-%d_%H-%M-%S}'.format(datetime.datetime.now())
    return timestamp


def read_attr_type_file():
    """ 
    Read file of common attributes. 
    """
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


@timing
def get_ols_results(attribute_type_dict, ontology=None):
    """ 
    Get search results from OLS.
    """

    TIMESTAMP = get_timestamp()

    all_attribute_types = attribute_type_dict.keys()
    attr_type_count = 0

    for attr_type in all_attribute_types:
        attr_type_count += 1

        if attr_type_count <= int(args.num_attr_review):
            print "\n** Attribute Type: ", attr_type, attribute_type_dict[attr_type]

            # prepare file for output of attribute value information
            attribute_type_filename = attr_type.lower()
            attribute_type_filename = attribute_type_filename.replace(" ", "_")
            attribute_type_filename = attribute_type_filename+"_"+TIMESTAMP+".csv"
            save_directory_path = os.getcwd() #assumes script is run from "data-results" directory
            data_directory = "ols_search_results"
            completeName = os.path.join(save_directory_path, data_directory, attribute_type_filename)
            
            # open file to write data
            outfile = open(completeName, "w")
            csvout = unicodecsv.writer(outfile)

            url = " http://www.ebi.ac.uk/ols/api/search?q={attr_type:s}&" \
                "groupField=true".format(attr_type=attr_type)

            csvout.writerow([attr_type, attribute_type_dict[attr_type]])
            csvout.writerow(["Ontology", "Number of Results"])

            try:
                response = requests.get(url)
                if response.status_code == 200:
                    results = json.loads(response.content)
                    if results:
                        facet_counts = results['facet_counts']['facet_fields']['ontology_prefix']

                        d = dict(itertools.izip_longest(*[iter(facet_counts)] * 2, fillvalue=""))
                        for k,v in d.iteritems():
                            # print k,v
                            csvout.writerow([k, v])
                    else:
                        # print "No reponse"
                        csvout.writerow(["none", "none"])
            except requests.exceptions.RequestException as e:
                print e
                csvout.writerow(e)
    
    outfile.close()


if __name__ == '__main__':
    print "Starting to search OLS..."

    # Commandline arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--attr_type_file_path', default="/Users/twhetzel/git/biosamples-data-mining/data_results/unique_attr_types_2017-06-20_14-31-00.csv")
    parser.add_argument('--num_attr_review', help="Number of Attributes to search OLS", default=16000)
    args = parser.parse_args()

    # Get attr_type file with count of attr per type
    attribute_type_dict = read_attr_type_file()

    # Generate OLS Search results for Attribute Types 
    get_ols_results(attribute_type_dict)





