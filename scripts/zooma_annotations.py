import argparse
import requests, json
import datetime
from functools import wraps
from time import time
import csv, unicodecsv


def get_timestamp():
    """ 
    Get timestamp of current date and time. 
    """
    timestamp = '{:%Y-%m-%d_%H-%M-%S}'.format(datetime.datetime.now())
    return timestamp


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


# TODO: Read in file of attribute characteristics in order to filter out attr types 
# where the values are all numbers or booleans and do not need to be examined by Zooma


@timing
def get_zooma_annotations(attribute_type_dict, ontology=None):
    """ 
    Get annotations using Zooma
    """
    
    # url = "http://www.ebi.ac.uk/spot/zooma/v2/api/services/annotate?" \
    #       "propertyValue={attr_type:s}&" \
    #       "filter=required:[none]," \
    #       "filter=ontologies:{ontology:s}".format(attr_type=attr_type, ontology=ontology)
    TIMESTAMP = get_timestamp()
    outfile = open("/Users/twhetzel/git/biosamples-data-mining/data_results/ZoomaResults/attr_type_zooma_results_"+TIMESTAMP+".csv", "w")
    csvout = unicodecsv.writer(outfile)

    csvout.writerow(["Attribute Type", "propertyValue", "semanticTags", "confidence", "derivedFromUri", "provenanceEvidence"])

    all_attribute_types = attribute_type_dict.keys()
    attr_type_count = 0

    for attr_type in all_attribute_types:
        attr_type_count += 1

        if args.zooma_url_prefix.lower() == "dev":
            ZOOMA_URL_PREFIX = "http://snarf.ebi.ac.uk:8580"
        else:
            ZOOMA_URL_PREFIX = "http://www.ebi.ac.uk"


        ZOOMA_URL = ZOOMA_URL_PREFIX+"/spot/zooma/v2/api/services/annotate?" \
              "propertyValue={attr_type:s}".format(attr_type=attr_type)


        if attr_type_count <= int(args.num_attr_review):
            print "\n** Attribute Type: ", attr_type, attr_type_count

            try:
                response = requests.get(ZOOMA_URL)
                if response.status_code == 200:
                    results = json.loads(response.content)
                    if results:
                        for result in results:
                            print result['annotatedProperty']['propertyValue'], result['semanticTags'][0], result['confidence'], result['derivedFrom']['uri'], result['provenance']['evidence']
                            csvout.writerow([attr_type, result['annotatedProperty']['propertyValue'], result['semanticTags'][0], result['confidence'], result['derivedFrom']['uri'], result['provenance']['evidence']])
                    else:
                        print "No reponse"
                        csvout.writerow([attr_type, "none", "none", "none", "none", "none"])
            except requests.exceptions.RequestException as e:
                print e
                csvout.writerow(e)
    
    outfile.close()


if __name__ == '__main__':
    print "Starting to profile atttribute values..."

    # Commandline arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--attr_type_file_path', default="/Users/twhetzel/git/biosamples-data-mining/data_results/unique_attr_types_2017-06-20_14-31-00.csv")
    parser.add_argument('--num_attr_review', help="Number of Attributes to annotate with Zooma", default=16000)
    parser.add_argument('--zooma_url_prefix', help="Specify DEV or PROD Url prefix for Zooma web service call")
    args = parser.parse_args()

    # Get attr_type file with count of attr per type
    attribute_type_dict = read_attr_type_file()

    # Generate Zooma annotations for Attribute Types 
    get_zooma_annotations(attribute_type_dict)

    # Data Output Format: csv or json of attr_type, zooma results obj, ontology topics
    # later add file? of values, zooma results obj, ontology topics
    # post-analysis: find MATCH between attr type/value ontology topics







