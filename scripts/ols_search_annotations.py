import argparse
from functools import wraps
from time import time
import requests, json
import datetime
import csv, unicodecsv
import itertools
import os


class DataFormatter:
    """
    Format results into JSON for further analysis.
    """

    def __init__(self, data):
        self.data = data


def get_ontology_topics():
    """
    Use Google Sheets API to get ontology topic data.
    """
    pass


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
def ols_search(attribute_type_dict, ontology=None):
    """ 
    Get search results from OLS.
    """

    # Search with entire attr type with &local=true&exact=true, 
    params1 = "&local=true&exact=true"
    # if no results found, search with each token (not stopword) &local=true&exact=true
    # if no results, search with entire attr type, &local=true&exact=false
    params2 = "&local=true&exact=false"
    # if no results, search each token (not stopword) &local=true&exact=false

    # Results: Get first 10 results returned
    # format fields into list of Json objects


    # prepare file for output of attribute value information
    TIMESTAMP = get_timestamp()
    filename = "attr_type_ols_search_results_"+TIMESTAMP+".csv"
    save_directory_path = "/Users/twhetzel/git/biosamples-data-mining/data_results"
    data_directory = "OLSSearchResults"
    completeName = os.path.join(save_directory_path, data_directory, filename)
    
    # open file to write data -> this will prob move outside loop when generating all data
    outfile = open(completeName, "w")
    csvout = unicodecsv.writer(outfile)

    all_attribute_types = attribute_type_dict.keys()
    attr_type_count = 0

    for attr_type in all_attribute_types:
        attr_type_count += 1

        csvout.writerow(["Ontology", "Number of Results", attr_type, attribute_type_dict[attr_type]])

        attribute_type_formatted = attr_type.lower()
        attribute_type_formatted = attribute_type_formatted.replace(" ", "_")

        if attr_type_count <= int(args.num_attr_review):
            print "\n** Attribute Type: ("+ str(attr_type_count) +")", attr_type, attribute_type_dict[attr_type]

            # get ols search results
            terms_found = _get_results(attr_type, params1)
            if terms_found != "None":
                csvout.writerow([terms_found])
            else:
                terms_found = _get_results(attr_type, params2)
                csvout.writerow([terms_found])

            
    outfile.close()


def _get_results(attr_type, params):
    """
    Get OLS Results for attr_type and OLS params
    """
    OLS_URL = " http://www.ebi.ac.uk/ols/api/search?q={attr_type:s}&" \
                "{params}".format(attr_type=attr_type, params=params)

    print "\n** ATTR-TYPE, PARAMS: ", attr_type, params

    try:
        response = requests.get(OLS_URL)
        if response.status_code == 200:
            results = json.loads(response.content)
            if results:
                num_results = results["response"]["numFound"]
                if num_results > 0:
                    # get first 10 results
                    terms_found = results["response"]["docs"]
                    print "** Terms found: ", terms_found
                    return terms_found
                else:
                    print "** No terms found. Try params2."
                    # call method with params2
                    return "None"
            else:
                # print "No reponse"
                csvout.writerow(["none", "none"])
    except requests.exceptions.RequestException as e:
        print e
        csvout.writerow(e)


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
    ols_search(attribute_type_dict)





