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

    def __init__(self, attr_type, num_results, data):
        self.attr_type = attr_type
        self.num_results = num_results
        self.data = data


    def create_term_result_obj(self):
        formatted_attr_type = self.attr_type.lower()
        formatted_attr_type = formatted_attr_type.replace(" ", "_")

        value_obj = {}
        results_list = []
        
        for term_result in self.data:
            result_obj = {}

            keys = term_result.keys()
            op_key = "ontology_prefix"
            label_key = "label"
            iri_key = "iri"

            if op_key in keys:
                result_obj[op_key] = term_result[op_key]
            if label_key in keys:
                result_obj[label_key] = term_result[label_key]
            if iri_key in keys:
                result_obj[iri_key] = term_result[iri_key]
            results_list.append(result_obj)

        value_obj["number_of_ols_results"] = self.num_results
        value_obj["results"] = results_list

        return value_obj


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
def ols_search(attribute_type_dict):
    """ 
    Get search results from OLS.
    """

    # Search with entire attr type with &local=true&exact=true, 
    params1 = "local=true&exact=true"
    # if no results found, search with each token (not stopword) &local=true&exact=true
    # if no results, search with entire attr type, &local=true&exact=false
    params2 = "local=true&exact=false"
    # if no results, search each token (not stopword) &local=true&exact=false

    # Results: Get first 10 results returned
    # format fields into list of Json objects
    ols_result_obj = {}


    # prepare file for output of attribute value information
    TIMESTAMP = get_timestamp()
    filename = "attr_type_ols_search_results_"+TIMESTAMP+".csv"
    save_directory_path = "/Users/twhetzel/git/biosamples-data-mining/data_results"
    data_directory = "OLSSearchResults"
    completeName = os.path.join(save_directory_path, data_directory, filename)

    outfile = open(completeName, "w")

    all_attribute_types = attribute_type_dict.keys()
    attr_type_count = 0

    for attr_type in all_attribute_types:
        attr_type_count += 1

        attribute_type_formatted = attr_type.lower()
        attribute_type_formatted = attribute_type_formatted.replace(" ", "_")

        if attr_type_count <= int(args.num_attr_review):
            print "\n** Attribute Type("+ str(attr_type_count) +"): ", attr_type, attribute_type_dict[attr_type]

            # get ols search results
            NO_RESULTS = 0
            formatted_attr_type = attr_type.lower()
            formatted_attr_type = formatted_attr_type.replace(" ", "_")

            num_results, ols_term_result_obj = _get_results(attr_type, params1)

            if num_results != NO_RESULTS:
                ols_result_obj[formatted_attr_type] = ols_term_result_obj
            else:
                num_results, ols_term_result_obj = _get_results(attr_type, params2)
                ols_result_obj[formatted_attr_type] = ols_term_result_obj
    
    # write ols search results for attr_trype to file
    json.dump(ols_result_obj, outfile)
    outfile.close()


def _get_results(attr_type, params):
    """
    Get OLS Results for attr_type and OLS params
    """
    OLS_URL = " http://www.ebi.ac.uk/ols/api/search?q={attr_type:s}&" \
                "{params}".format(attr_type=attr_type, params=params)

    try:
        response = requests.get(OLS_URL)
        if response.status_code == 200:
            results = json.loads(response.content)
            if results:
                num_results = results["response"]["numFound"]
                # print "** NumResults: ", num_results
                if num_results > 0:
                    terms_found = results["response"]["docs"]
                    data_formatter = DataFormatter(attr_type, num_results, terms_found)
                    return (num_results, data_formatter.create_term_result_obj())
                else:
                    # print "** No terms found. Try params2."
                    # call method with params2
                    return (num_results,"None")
            else:
                # print "** No reponse!!!"
                # csvout.writerow(["none", "none"])
                pass
        else:
            print "** RESPONSE STATUS CODE: ", response.status_code
            if response.status_code == 500:
                print "\n--> ReTry OLS...", attr_type, params, "\n"
                _get_results(attr_type, params)
    except requests.exceptions.RequestException as e:
        print e
        # csvout.writerow(e)


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





