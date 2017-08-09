import argparse
from functools import wraps
from time import time, sleep
import requests, json
import datetime
import csv, unicodecsv
import itertools
import glob, os

import google_sheets_quickstart


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
            result_obj["ontology_topics"] = ontology_topics[term_result[op_key]]
            results_list.append(result_obj)

        value_obj["number_of_ols_results"] = self.num_results
        value_obj["results"] = results_list

        return value_obj


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


def get_attr_type_value_file_names():
    """
    Get list of all filenames to examine.
    """
    all_value_file_names = []
    cwd = os.getcwd()
    # Change to dir with result files to analyze
    os.chdir(args.attr_values_dir)
    
    for file in glob.glob("*.csv"):
        all_value_file_names.append(file)

    # Return to current working directory
    os.chdir(cwd)
    return all_value_file_names


def get_ontology_topics():
    """
    Use Google Sheets API to get ontology topic data.
    """
    ontology_topics_dict = google_sheets_quickstart.get_google_sheets_data()
    return ontology_topics_dict


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


@timing
def ols_search_for_values(all_value_file_names):
    """ 
    Get search results from OLS for attribute type Values.
    """

    params1 = "local=true&exact=true"
    params2 = "local=true&exact=false"
    ols_result_obj = {}
    # value_result_obj = {}

    # prepare file for output of attribute value information
    TIMESTAMP = get_timestamp()
    filename = "values_ols_search_results_"+TIMESTAMP+".csv"
    save_directory_path = "/Users/twhetzel/git/biosamples-data-mining/data_results"
    data_directory = "OLSSearchResults"
    completeName = os.path.join(save_directory_path, data_directory, filename)

    outfile = open(completeName, "w")

    all_attribute_types = attribute_type_dict.keys()
    attr_type_count = 0

    for attr_type in all_attribute_types:
        attr_type_count += 1

        # format attribute type the same as attribute filename 
        formatted_attribute_type = attr_type.lower()
        formatted_attribute_type = formatted_attribute_type.replace(" ", "_")
        attribute_type_filename = formatted_attribute_type+".csv"

        if attr_type_count >= int(args.restart_attr_count) and attr_type_count <= int(args.num_attr_review):
            print "\n** Attribute Type("+ str(attr_type_count) +"): ", attr_type, \
            "-- UniqValues: ",attribute_type_dict[attr_type]

            # open file with values for ols search
            with open(args.attr_values_dir+attribute_type_filename, "r") as attr_value_file:
                
                content = attr_value_file.readlines()
                content = [x.strip(' \t\n\r') for x in content]
                
                line_count = 0
                value_result_list = []
                
                for item in content:
                    line_count += 1
                    if line_count < 100:
                        value_result_obj = {}
                        # value_result_list = []
                        value, val_count, iri = item.split('\t')

                        # check if value is a number
                        if all(c in "-0123456789.E/\%" for c in value):
                            print "\n-- Skip searching with ", value
                            pass
                        else:
                            print "** Value("+str(line_count)+")", value ,\
                            "-- Attribute Type("+str(attr_type_count)+")", str(attr_type)
                            
                            # get ols search results
                            NO_RESULTS = 0
                            formatted_value = value.lower()
                            formatted_value = formatted_value.replace(" ", "_")

                            if len(value) < 100:
                                print "** LV: ",len(value)
                                sleep(.2)
                                num_results, ols_term_result_obj = _get_results(value, params1)
                            else:
                                pass

                            if num_results != NO_RESULTS:
                                print "*** Found values for ", value
                                value_result_obj[formatted_value] = ols_term_result_obj
                                # ols_result_obj[formatted_attribute_type] = value_result_obj
                                value_result_list.append(value_result_obj)
                            else:
                                sleep(.5)
                                num_results, ols_term_result_obj = _get_results(value, params2)
                                print "*** Found values for ", value
                                value_result_obj[formatted_value] = ols_term_result_obj
                                # ols_result_obj[formatted_attribute_type] = value_result_obj
                                value_result_list.append(value_result_obj)

                        ols_result_obj[formatted_attribute_type] = value_result_list
    
    # write ols search results for attr_trype to file
    json.dump(ols_result_obj, outfile)
    outfile.close()


def _get_results(search_value, params):
    """
    Get OLS Results for attr_type and OLS params
    """
    OLS_URL = " http://www.ebi.ac.uk/ols/api/search?q={search_value:s}&" \
                "{params}".format(search_value=search_value, params=params)

    # print "*** Searching with ",search_value
    # print "*** OLS_URL: ", OLS_URL

    try:
        response = requests.get(OLS_URL)
        if response.status_code == 200:
            results = json.loads(response.content)
            if results:
                num_results = results["response"]["numFound"]
                # print "** NumResults: ", num_results
                if num_results > 0:
                    terms_found = results["response"]["docs"]
                    data_formatter = DataFormatter(search_value, num_results, terms_found)
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
            # if response.status_code == 500 or response.status_code == 400:
            print "\n--> ReTry OLS...", search_value, params, "\n"
            _get_results(search_value, params)
    except requests.exceptions.RequestException as e:
        print e
        # csvout.writerow(e)


if __name__ == '__main__':
    print "Starting to search OLS..."

    # Commandline arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--attr_values_dir', default="/Users/twhetzel/git/biosamples-data-mining/data_results/attr_type_values-data_results/")
    parser.add_argument('--attr_type_file_path', default="/Users/twhetzel/git/biosamples-data-mining/data_results/unique_attr_types_2017-06-20_14-31-00.csv")
    parser.add_argument('--num_attr_review', default=16000, help="Number of Attributes to search OLS")
    parser.add_argument('--restart_attr_count', default=0, help="Count of which attribute to re-start OLS search at.")
    args = parser.parse_args()

    # Get ontology topics from Google Sheet
    global ontology_topics
    ontology_topics = get_ontology_topics()

    # Get attr_type file with count of attr per type
    attribute_type_dict = read_attr_type_file()

    # Generate OLS Search results for Attribute Types 
    # ols_search(attribute_type_dict)

    # Get list of attr_type value files
    all_value_file_names = get_attr_type_value_file_names()

    # Generate OLS Search results for Values
    ols_search_for_values(all_value_file_names)




