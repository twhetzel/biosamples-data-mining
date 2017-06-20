#pip install neo4j-driver
from neo4j.v1 import GraphDatabase, basic_auth

import datetime
from functools import wraps
from time import time

import csv
import os.path


def timing(f):
    """
    Create wrapper to report time of functions.
    """
    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
        print 'Function: %r args:[%r, %r] took: %2.2f sec, %2.2f min' % \
          (f.__name__, args, kw, te-ts, (te-ts)/60)
        return result
    return wrap


def get_timestamp():
    """ 
    Get timestamp of current date and time. 
    """
    timestamp = '{:%Y-%m-%d_%H-%M-%S}'.format(datetime.datetime.now())
    return timestamp


@timing
def get_all_attribute_types_and_values(driver):
    """ 
    Get all unique attribute types and values for each type. 
    """
    print "Generating file of unique attribute types..."
    
    results = []

    TIMESTAMP = get_timestamp()
    with open("unique_attr_types_"+TIMESTAMP+".csv", "w") as outfile:
        csvout = csv.writer(outfile)
        
        with driver.session() as session:
            # Get all attribute types
            results = session.run(
                "MATCH (:Sample)-[u:hasAttribute]->(a:Attribute) \
                RETURN a.type AS type, COUNT(u) AS usage_count \
                ORDER BY usage_count DESC"
            )
            
            for result in results:
                row = ["{} | {}".format(result["type"], result["usage_count"])]
                print "{} | {}".format(result["type"], result["usage_count"])
                csvout.writerow(row)

                print "Generating file of values for attribute type..."

                # For each attribute type, get all values
                attribute_type_name = result["type"]
                with driver.session() as session2:
                    attr_type_values = session2.run(
                        "MATCH (s:Sample)-->(a:Attribute)-->(t:AttributeType{name:'"+str(attribute_type_name)+"'}), \
                        (a:Attribute)-->(v:AttributeValue) WITH a,t,v,COUNT(s) AS usage_count \
                        RETURN a.iri as iri, t.name AS type, v.name AS value, usage_count \
                        ORDER BY usage_count DESC"
                    )

                    # prepare file for output of attribute value information
                    attribute_type_filename = attribute_type_name.lower()
                    attribute_type_filename = attribute_type_filename.replace(" ", "_")
                    attribute_type_filename = attribute_type_filename+".csv"
                    save_directory_path = os.getcwd()
                    data_directory = "attr_type_values-data_results"
                    completeName = os.path.join(save_directory_path, data_directory, attribute_type_filename)  
                    # open file to write data
                    att_type_values_data = open(completeName, "w")

                    for attr_type_value in attr_type_values:
                        value = attr_type_value["value"].encode('utf-8')
                        value_count = attr_type_value["usage_count"]
                        value_iri = attr_type_value["iri"]
                        
                        if value_iri is None:
                            value_iri = "none"
                        else:
                            value_iri = value_iri.encode('utf-8')

                        att_type_values_data.write(value+"\t"+str(value_count)+"\t"+value_iri+"\n")
                print "\n"


    print "** DONE - generated file of most common attribute types."
    print "** DONE - generated files with all values for each attribute type."




if __name__ == "__main__":
    """ 
    Generate file of most common Attribute Types and their count. 
    """
    driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "neo4jebi"))
    
    get_all_attribute_types_and_values(driver)
    
    # get_values_for_attribute_type(results)


    # with open("TEST-attr_common.csv", "w") as outfile:
    #     csvout = csv.writer(outfile)
        
    #     with driver.session() as session:
    #         results = session.run(
    #             "MATCH (:Sample)-[u:hasAttribute]->(a:Attribute) \
    #             RETURN a.type AS type, COUNT(u) AS usage_count \
    #             ORDER BY usage_count DESC LIMIT 3000"
    #         )
              # Get all attribute types
    #         for result in results:
    #             print "{} ({})".format(result["type"], result["usage_count"])
    #             row = ["{} | {}".format(result["type"], result["usage_count"])]

                  # For each attribute type, get all values
    #             attribute_type_name = result["type"]
    #             with driver.session() as session2:
    #                 results2 = session2.run(
    #                     "MATCH (s:Sample)-->(a:Attribute)-->(t:AttributeType{name:'"+str(result["type"])+"'}), \
    #                     (a:Attribute)-->(v:AttributeValue) WITH a,t,v,COUNT(s) AS usage_count \
    #                     RETURN a.iri as iri, t.name AS type, v.name AS value, usage_count \
    #                     ORDER BY usage_count DESC LIMIT 8000"
    #                 )

    #                 # prepare file for output
    #                 attribute_type_filename = attribute_type_name.lower()
    #                 attribute_type_filename = attribute_type_filename.replace(" ", "_")
    #                 attribute_type_filename = attribute_type_filename+".csv"
    #                 att_type_out = open(attribute_type_filename, "w")

    #                 count = 0
    #                 for result2 in results2:
    #                     count += 1
    #                     type_value = result2["value"].encode('utf-8')
    #                     type_count = result2["usage_count"]
    #                     type_iri = result2["iri"]
    #                     if type_iri is None:
    #                         type_iri = "none"
    #                     else:
    #                         type_iri = type_iri.encode('utf-8')
    #                     # print "Row by Value: ", type_value, type_count, type_iri
    #                     if count % 200 == 0:
    #                         print ".",
    #                     att_type_out.write(type_value+"\t"+str(type_count)+"\t"+type_iri+"\n")
    #             print "\n"
    #             csvout.writerow(row)

