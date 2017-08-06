import argparse
import json
import datetime
import csv


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


def load_ols_results():
    """ 
    Read JSON file of OLS results. 
    """
    with open(args.ols_result_file, 'r') as data_file:
        data = json.load(data_file)
    return data


def generate_summary(data):
    """
    Summarize results for attr_type <-> topic mapping.
    """
    ols_results_directory = "/Users/twhetzel/git/biosamples-data-mining/data_results/OLSSearchResults/SummaryResults/"
    TIMESTAMP = get_timestamp()
    outfile = open(ols_results_directory+"ols_summary_results"+TIMESTAMP+".csv", "w")
    csvout = csv.writer(outfile)


    attr_type_count = 0
    for attr_type, value_obj in data.iteritems():
        ontology_topics_list = []
        ontology_prefix_list = []
        attr_type_count += 1

        if value_obj != "None":
            for count, result in enumerate(value_obj["results"]):
                ontology_prefix = value_obj["results"][count]["ontology_prefix"].encode('utf-8')
                ontology_topics = value_obj["results"][count]["ontology_topics"]
                formatted_ontology_topics = [item.encode('utf-8') for item in ontology_topics]

                ontology_prefix_list.append(ontology_prefix)
                ontology_topics_list.append(formatted_ontology_topics)

            print "\n** Attr_Type("+ str(attr_type_count) +"): ", attr_type
            print "** Topics: ", ontology_topics_list
            print "** Prefix: ", ontology_prefix_list
            csvout.writerow([attr_type, ontology_prefix_list, ontology_topics_list])
        else:
            print "\n** Attr_Type("+ str(count) +"): ", attr_type
            print "** Topics: ", value_obj
            print "** Prefix: ", value_obj
            csvout.writerow([attr_type, value_obj, value_obj])

    outfile.close()


if __name__ == '__main__':
    """ 
    Summarize OLS search results for each Attribute Type. 
    """
    print "Starting to summarize OLS search results..."

    # Commandline arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--dir', default="/Users/twhetzel/git/biosamples-data-mining/data_results/ols_search_results/")
    # parser.add_argument('--attr_type_file_path', default="/Users/twhetzel/git/biosamples-data-mining/data_results/unique_attr_types_2017-06-20_14-31-00.csv")
    parser.add_argument('--num_attr_review', help="Number of Attributes to analyze their values", default=16000)
    parser.add_argument('--ols_result_file', \
        default="/Users/twhetzel/git/biosamples-data-mining/data_results/OLSSearchResults/attr_type_ols_search_results_2017-08-05_23-04-05.csv", \
        help="Full path to Ols result file to summarize.")
    args = parser.parse_args()


    # all_file_names = get_file_names()
    
    # read in file of ols search results 
    data = load_ols_results()

    # summarize results, for each attribute type return csv list of attr_type and topics
    # if exact match, give 100% to match/topic, if mutl results, give x/10*100 to each match
    generate_summary(data)
    


    # Examples to parse "data"
    # print "** Keys: ", data.keys()
    # for k,v in data.iteritems():
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

