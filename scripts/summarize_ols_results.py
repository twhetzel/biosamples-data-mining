import argparse
import json
import datetime
from time import time
import csv, os
from collections import Counter
from functools import wraps


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


def load_ols_results(filename):
    """ 
    Read JSON file of OLS results. 
    """
    with open(filename, 'r') as data_file:
        data = json.load(data_file)
    return data


@timing
def generate_attr_type_summary(data):
    """
    Summarize results of attr_type search results.
    """
    ols_results_directory = "/Users/twhetzel/git/biosamples-data-mining/data_results/OLSSearchResults/SummaryResults/"
    TIMESTAMP = get_timestamp()
    # outfile = open(ols_results_directory+"ols_summary_results"+TIMESTAMP+".csv", "w")
    # csvout = csv.writer(outfile)

    attr_type_overall_results = {}

    attr_type_count = 0
    for attr_type, value_obj in data.iteritems():
        overall_result_list = []
        result_list = []

        # ontology_topics_list = []
        # ontology_prefix_list = []
        attr_type_count += 1

        if value_obj != "None":
            # print "\n"
            for count, result in enumerate(value_obj["results"]):
                ontology_prefix = value_obj["results"][count]["ontology_prefix"].encode('utf-8')
                ontology_topics = value_obj["results"][count]["ontology_topics"]
                formatted_ontology_topics = [item.encode('utf-8') for item in ontology_topics]
                
                result_list = [count, ontology_prefix,formatted_ontology_topics]
                # print "** ResultList: ", result_list

                overall_result_list.append(result_list)

                # ontology_prefix_list.append(ontology_prefix)
                # ontology_topics_list.append(formatted_ontology_topics)

            # print "** Attr_Type("+ str(attr_type_count) +"): ", attr_type
            # print "** Results: ", overall_result_list
            
            # print "** Topics: ", ontology_topics_list
            # print "** Prefix: ", ontology_prefix_list
            # csvout.writerow([attr_type, overall_result_list])
            attr_type_overall_results[attr_type] = overall_result_list
        else:
            # print "\n** Attr_Type("+ str(attr_type_count) +"): ", attr_type
            # print "** Results: ", value_obj
            
            # print "** Topics: ", value_obj
            # print "** Prefix: ", value_obj
            # csvout.writerow([attr_type, value_obj])
            attr_type_overall_results[attr_type] = [[-1, value_obj, [value_obj]]]

    # outfile.close()
    return attr_type_overall_results


@timing
def generate_values_summary(data):
    """
    Summarize results of values search results.
    """
    ols_results_directory = "/Users/twhetzel/git/biosamples-data-mining/data_results/OLSSearchResults/SummaryResults/"
    TIMESTAMP = get_timestamp()
    # outfile = open(ols_results_directory+"ols_value_summary_results"+TIMESTAMP+".csv", "w")
    # csvout = csv.writer(outfile)

    value_overall_results = {}

    attr_type_count = 0
    for attr_type, value_list in data.iteritems():
        # print "\n** ATTR-TYPE: ", attr_type
        search_results = {}
        # overall_result_list = []
        # result_list = []

        # ontology_topics_list = []
        # ontology_prefix_list = []
        attr_type_count += 1

        # if value_obj != "None":
        if len(value_list) > 0: 
            for result_obj in value_list:
                overall_result_list = []
                # print "** Results: ", result_obj
                for search_value, results in result_obj.iteritems():
                    result_list = []
                    # print "\n** SV: ", search_value+" ("+attr_type+")"
                    if results != "None":
                        for count, result in enumerate(results["results"]):
                            # print "\n** C-R: ", count, result
                            ontology_prefix = results["results"][count]["ontology_prefix"].encode('utf-8')
                            ontology_topics = results["results"][count]["ontology_topics"]
                            formatted_ontology_topics = [item.encode('utf-8') for item in ontology_topics]
                            
                            result_list = [count, ontology_prefix,formatted_ontology_topics]
                            # print "** ResultList: ", result_list

                            overall_result_list.append(result_list)
                        # print "** ORL: ", overall_result_list
                        
                search_results[search_value] = overall_result_list
            # print "** SR: ", search_results

                            # ontology_prefix_list.append(ontology_prefix)
                            # ontology_topics_list.append(formatted_ontology_topics)

                        # print "** Attr_Type("+ str(attr_type_count) +"): ", attr_type, search_value
                        # print "** Results: ", overall_result_list
                        # print "** Topics: ", ontology_topics_list
                        # print "** Prefix: ", ontology_prefix_list
                        # csvout.writerow([attr_type, overall_result_list])
            value_overall_results[attr_type] = search_results
        else:
            # print "** Attr_Type("+ str(attr_type_count) +"): ", attr_type
            
            # print "** Results: ", "None"
            
            # print "** Topics: ", value_obj
            # print "** Prefix: ", value_obj
            # csvout.writerow([attr_type, value_obj])
            value_overall_results[attr_type] = {u'None': [[-1, 'None', ['None']]]}

    # outfile.close()
    return value_overall_results


@timing
def find_attr_type_value_similarities(attr_type_overall_results, value_overall_results):
    """
    Find ontology mapping matches between attr_type and values for that attr_type.
    """

    TIMESTAMP = get_timestamp()
    filename = "NEW-summary_ols_search_results_"+TIMESTAMP+".csv"
    save_directory_path = "/Users/twhetzel/git/biosamples-data-mining/data_results"
    data_directory = "OLSSearchResults"
    completeName = os.path.join(save_directory_path, data_directory, filename)

    outfile = open(completeName, "w")
    csvout = csv.writer(outfile)
    # csvout.writerow(["AttributeType", "Value", "Summary", "Ontology Matches", "AT-OntologyHits", "VAL-OntologyHits"])
    csvout.writerow(["AttributeType", "Summary"])

    # attr_type_keys_count = attr_type_overall_results.keys()
    # print "** ATKC: ", len(attr_type_keys_count)

    # Iterate through attr_type_overall_results to get key and list of ols results
    for attr_type, attr_results in attr_type_overall_results.iteritems():
        attr_type_topic_map = {}
        all_attr_value_ontology_match_pairs = []

        # Check if this attr_type has value data
        if str(attr_type) in value_overall_results:
            print "** ATOR: ", attr_type, attr_results
            print "** VOR-MAP: ", value_overall_results[str(attr_type)]
    
            pair = False
            # ordered_attr_results = _get_most_frequent_ontology(attr_results, pair)
            
            # found_match = False
            for value, value_results in value_overall_results[str(attr_type)].iteritems():
                print "\n** Val: ", value, ", Attr_Type: ", attr_type
                print "** Val-Ontol_Results: ", value_results
                
                # get ordered ontology count results for ols value search
                # ordered_value_results = _get_most_frequent_ontology(value_results, pair)
                
                # all_match_pairs = []
                for count, val_results in enumerate(value_results):
                    all_match_pairs = []
                    found_match = False
                    print "** RES: ", val_results[1]
                    search = val_results[1]
                    # print "** SEARCH: ", search

                    # compare two lists for matches 
                    # https://stackoverflow.com/questions/1156087/python-search-in-lists-of-lists/1156114#1156114
                    for sublist in attr_results:
                        if sublist[1] == search:
                            # print "Found it!", sublist, val_results
                            found_match = True
                            match_pair = [sublist, val_results]
                            print "** MP: ", match_pair
                            all_match_pairs.append(match_pair)
                            # print "\n** ML-1: ", all_match_pairs
                    all_attr_value_ontology_match_pairs.extend(all_match_pairs)
                    print "*** Done searching for matches with ", val_results[1], "\n"
                            
                # print "\n** ML: ", all_match_pairs

                pair = True
                if pair and len(all_match_pairs) > 0:
                    # print "\n** ML-2: ", all_match_pairs
                    summary = _get_most_frequent_ontology(all_match_pairs, pair)
                
                if found_match:
                    # csvout.writerow([attr_type, value.encode('utf-8'), summary, \
                    #     all_match_pairs, ordered_attr_results, ordered_value_results])
                    # when there are matches, further summarize data for each attr_type
                    pass
                    
                else:
                    print "No matching ontology results between: ", attr_type, value
                    # csvout.writerow([attr_type, value.encode('utf-8'), "No Matches", \
                    #     "No Matches", ordered_attr_results, ordered_value_results])
        else:
            # print "** Attr_type not in VOR"
            pass

        print "\n** AAVOMP: ", len(all_attr_value_ontology_match_pairs)
        all_attr_value_ontology_match_pairs_summary = _get_most_frequent_ontology(all_attr_value_ontology_match_pairs, True)
        csvout.writerow([attr_type, all_attr_value_ontology_match_pairs_summary])
        print "\n** AAVOMP-SUMMARY: ", all_attr_value_ontology_match_pairs_summary
        
        print "** Out of ATOR loop\n=-=-=-=-=-="


    # print "\n** DATA1: ", attr_type_overall_results["vioscreen_alcohol"], \
    # "\n** DATA2: ", value_overall_results["vioscreen_alcohol"]
    
    outfile.close()


def _get_most_frequent_ontology(matches, pair):
    """
    Given list of list of matches, find most frequently found ontology.
    Example data: [[[5, 'CLO', ['Cell']], [0, 'CLO', ['Cell']]], [[6, 'CLO', ['Cell']], [0, 'CLO', ['Cell']]], [[7, 'CLO', ['Cell']], [0, 'CLO', ['Cell']]], [[8, 'CLO', ['Cell']], [0, 'CLO', ['Cell']]], [[9, 'CLO', ['Cell']], [0, 'CLO', ['Cell']]]]
    """
    ontology_match_list = []
    topic_ontology_match_list = []
    for ontology in enumerate(matches):
        if pair:
            print "** Ontology with Match: ", ontology, ontology[1][1][1]
            ontology_match_list.append(ontology[1][1][1])
            topic_ontology_match_list.append(ontology[1][1][2])
        else:
            ontology_match_list.append(ontology[1][1])
            topic_ontology_match_list.append(ontology[1][2])
    
    print "** OML: ", ontology_match_list
    summary = Counter(ontology_match_list).most_common()
    print "** Summary: ", summary
    topic_summary = [list(x) for x in set(tuple(x) for x in topic_ontology_match_list)]
    print "** TS: ", topic_summary
    
    # if pair:
    #     print "** SummaryMatch: ", summary
    # else:
    #     print "** Summary: ", summary
    return topic_summary, summary


@timing
def new_find_attr_type_value_similarities(attr_type_search_results, value_search_results):
    """
    Summarize data as 1 row per attribute type, with combined count score 
    from all values where there was an ontology match.
    - Need to know:
    -- total samples for attr_type
    -- total values for the attr_type
    -- total values searched per attr_type (can be up to 100 based on limit in code)
    -- total search results per value
    -- if no match, what are results for values only and maybe attr_type only
    -- if no match, are values all numbers?

    Output: 
    attr_type, summary as: topic_1 (OntolPrefix1: Cnt:X, OntolPrefix2, Count: Y), topic_2 (OntolPrefix1: Cnt:X, ),
    total samples with this attr_type
    """
    TIMESTAMP = get_timestamp()
    filename = "NEW-summary_ols_search_results_"+TIMESTAMP+".csv"
    save_directory_path = "/Users/twhetzel/git/biosamples-data-mining/data_results"
    data_directory = "OLSSearchResults"
    completeName = os.path.join(save_directory_path, data_directory, filename)

    # outfile = open(completeName, "w")
    # csvout = csv.writer(outfile)
    # csvout.writerow(["AttributeType", "Summary"])

    # print "** Num AttrType: ", len(attr_type_search_results.keys())

    # Iterate through attr_type_overall_results to get key and list of ols results
    attr_no_results = 0
    value_no_results = 0
    try:
        for attr_type, attr_results in attr_type_search_results.iteritems():
            # Handle case when attr_type does not have any search results
            if len(attr_results) == 1 and attr_results[0][0] == -1:
                attr_no_results += 1
                print "\n===============\n** ATR: "+attr_type+", NumResults:", len(attr_results), "\nResults: ", attr_results
      
                # Check if this attr_type has value data
                if str(attr_type) in value_search_results:
                    print "\n** VR-MAP: ", value_search_results[str(attr_type)]
                    # print "\n** VR-MAP KEYS: ", value_search_results[str(attr_type)].keys()
                    number_of_values_searched = len(value_search_results[str(attr_type)].keys())

                    # find count of values with no results
                    not_searched = 0
                    for key in value_search_results[str(attr_type)].keys():
                        if key == 'None':
                            # {u'None': [[-1, 'None', ['None']]]}
                            # print "Value was number"
                            value_no_results += 1

                    all_value_results = []
                    for k,v in value_search_results[str(attr_type)].iteritems():
                        print v
                        if key == 'None':
                            pass
                        else:
                            # print "** Value has search results: ", v
                            for result in v:
                                if v == []:
                                    # {u'gatacgttcgca': [], u'ttgcgttagcag': []}
                                    # print "Value searched but had no results"
                                    not_searched += 1
                                else:
                                    # print "Prefix: ", result[1]
                                    all_value_results.append(result[1])
                    # print "Not Searched:", not_searched, "\n** All value prefix results: ", all_value_results

                    if len(all_value_results) > 0:
                        summary = Counter(all_value_results).most_common()
                        print "** Summary: ", summary
                        new_summary = []
                        for prefix, count in summary:
                            denom = 100.00 * number_of_values_searched
                            percentage = round((count/denom) * 100, 2)
                            new_summary.append((prefix,percentage))
                        print "*** NumValues per AttrType: ", number_of_values_searched
                        print "** New Summary: ", new_summary
                    else:
                        print "** No AttrType and No Value results"


                    # check if any values have search results
                    if not_searched == len(value_search_results[str(attr_type)].keys()):
                        value_no_results += 1
                        print "** No search results for attr_type or it's values"
       
                    print "Number of values with no results: ", not_searched, "of "+ \
                    str(len(value_search_results[str(attr_type)].keys())), "total values"
            else:
                # Handle case when attr_type has search results
                print "\n=-=-=-=-=-=-=-=-=-=-=-=\n** ATR: "+attr_type+", NumResults:", len(attr_results), "\nResults: ", attr_results
                all_attr_value_ontology_match_pairs = []
                number_of_values_searched = len(value_overall_results[str(attr_type)].keys())
                print "** NumValues per AttrType: ", number_of_values_searched
                
                for value, value_results in value_overall_results[str(attr_type)].iteritems():
                    # print "\n** Val: ", value, ", Attr_Type: ", attr_type
                    # print "** Val-Ontol_Results: ", value_results

                    all_matches = []
                    for count, val_results in enumerate(value_results):
                            search = val_results[1]

                            # compare two lists for matches 
                            # https://stackoverflow.com/questions/1156087/python-search-in-lists-of-lists/1156114#1156114
                            for sublist in attr_results:
                                if sublist[1] == search:
                                    # print "--- Found it!", sublist, val_results
                                    all_matches.append(sublist[1])
                            # print "*** Done searching for matches with ", val_results[1], "\n"
                    all_attr_value_ontology_match_pairs.extend(all_matches)


                if len(all_attr_value_ontology_match_pairs) > 0:
                    summary = Counter(all_attr_value_ontology_match_pairs).most_common()
                    # print "** Summary: ", summary
                    new_summary = []
                    for prefix, count in summary:
                        denom = 100.00 * number_of_values_searched
                        percentage = round((count/denom) * 100, 2)
                        new_summary.append((prefix,percentage))
                    # print "*** NumValues per AttrType: ", number_of_values_searched
                    print "** New Summary: ", new_summary
                else:
                    summary = "No matches"
                    print "** No matches"
    except KeyError:
        print "No values for this attr_type: ", attr_type

    print "\n** Total AttrType with No results: ", attr_no_results
    print "** Total AttrType with No Results and Values with No results: ", value_no_results
    # outfile.close()



if __name__ == '__main__':
    """ 
    Summarize search results for each Attribute Type. 
    """
    print "Starting to summarize OLS search results..."

    RESULTS_DIR = "/Users/twhetzel/git/biosamples-data-mining/data_results/OLSSearchResults/"
    ATTR_TYPES_DIR = "/AttrTypeResults/"
    VALUES_DIR = "/ValuesResults/"
    ALL_ATTR_TYPE_FILE = "ALL_attr_type_ols_search_results_2017-08-11_15-54-07.csv"
    ALL_VALUE_FILE = "All_values_ols_search_results_2017-08-18-Combined.csv"

    # Commandline arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--attr_type_ols_result_file', default=RESULTS_DIR+ATTR_TYPES_DIR+ALL_ATTR_TYPE_FILE)
    parser.add_argument('--value_ols_result_file', default=RESULTS_DIR+VALUES_DIR+ALL_VALUE_FILE)
    args = parser.parse_args()

    # Is this stil needed?
    # all_file_names = get_file_names()
    
    # Read in file of ols search results for attr_type
    attr_type_filename = args.attr_type_ols_result_file
    attr_type_data = load_ols_results(attr_type_filename)

    # Read in file of ols search results for values
    value_filename = args.value_ols_result_file
    value_data = load_ols_results(value_filename)


    # Generate summary of "attr_type" ols results
    attr_type_overall_results = generate_attr_type_summary(attr_type_data)
    
    # print "** ATOR: ", attr_type_overall_results
    # {u'included_final_analysis': [[0, 'PO', ['Plant']], [1, 'PO', ['Plant']], [2, 'BTO', ['Anatomy']], \
    # [3, 'PRIDE', ['Protein']], [4, 'CHMO', ['Chemical']], [5, 'FIX', ['Chemical']], \
    # [6, 'IAO', ['Information Artifact']], [7, 'MS', ['Protein']], [8, 'MS', ['Protein']], \
    # [9, 'CO_337', ['Groundnut, Plant']]], u'double_barcodes': [[0, 'SO', ['Sequence']], \
    # [1, 'FIX', ['Chemical']], [2, 'SIO', ['All']], [3, 'SYMP', ['Disease']], \
    # [4, 'FOODON', ['Food Processing']], [5, 'HP', ['Human, Phenotype']], [6, 'HP', ['Human, Phenotype']], \
    # [7, 'DOID', ['Disease, Human']], [8, 'CO_338', ['Chickpea, Phenotype']], [\
    # 9, 'CO_338', ['Chickpea, Phenotype']]]}

    
    # Generate summary of "value" ols results
    value_overall_results = generate_values_summary(value_data)
    
    # print "**VOR: ", value_overall_results
    # {u'included_final_analysis': {u'result1': [[],[]]}, {u'result2': [[],[]]}, \
    # u'double_barcodes': {u'result1': [[],[]]}}
    

    # Find ontology mapping similarities between attr_type and values --OLD SUMMARY 
    # find_attr_type_value_similarities(attr_type_overall_results, value_overall_results)

    new_find_attr_type_value_similarities(attr_type_overall_results, value_overall_results)




    # EXAMPLES FOR PARSING DATA
    # Examples to parse "attr_type_data"
    # print "** Keys: ", attr_type_data.keys()
    # for k,v in attr_type_data.iteritems():
    #     print "\n** Key: ", k, #"\nValue: ", v
    #     for k1,v1 in v.iteritems():
    #         print "\n** Key-1:", k1, "\nValue-1: ", v1
    #         print "** Result List : ", v["results"]
    #         # for result in v["results"]:
            #     print "\n** Result: ", result["ontology_topics"]      
        # print "\n** Result: ", v["results"][0]["ontology_topics"]

        # for count, result in enumerate(v["results"]):
        #     # print count, result
        #     print "\n** Result: ", v["results"][count]["ontology_topics"]


    # Example to parse "value_overall_results"
    # for k, v in value_overall_results.iteritems():
    #     print "\n** KEY: ", k, "\n** VAL: ", json.dumps(v)
    #     for k1, v1 in v.iteritems():
    #         print "** KEY1: ", k1, "\n** VAL1: ", json.dumps(v1)


