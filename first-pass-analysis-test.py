#pip install neo4j-driver

from neo4j.v1 import GraphDatabase, basic_auth

import csv

import datetime

from functools import wraps
from time import time



def timing(f):
    """
    Create wrapper to report time of functions.
    """
    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
        print 'Function: %r args:[%r, %r] took: %2.4f sec' % \
          (f.__name__, args, kw, te-ts)
        return result
    return wrap


def get_timestamp():
    """ 
    Get timestamp of current date and time. 
    """
    timestamp = '{:%Y-%m-%d_%H-%M-%S}'.format(datetime.datetime.now())
    return timestamp


@timing
def get_all_attribute_types(driver):
    """ 
    Get all unique attribute types. 
    """
    print "Generating spreadsheet of unique attribute types..."
    
    TIMESTAMP = get_timestamp()
    with open("TEST-attr_common_"+TIMESTAMP+".csv", "w") as outfile:
        csvout = csv.writer(outfile)
        
        with driver.session() as session:
            results = session.run(
                "MATCH (:Sample)-[u:hasAttribute]->(a:Attribute) \
                RETURN a.type AS type, COUNT(u) AS usage_count \
                ORDER BY usage_count DESC LIMIT 10"
            )
            
            for result in results:
                row = ["{} | {}".format(result["type"], result["usage_count"])]
                print "{} | ({})".format(result["type"], result["usage_count"])
                csvout.writerow(row)

    print "** DONE - generated file of most common attribute types."


if __name__ == "__main__":
    """ 
    Generate file of most common Attribute Types and their count. 
    """
    driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "neo4jebi"))
    get_all_attribute_types(driver)
    
    # driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "neo4jebi"))

    # #spreadsheet of most common attribute types and values
    # print "Generating spreadsheet of most common attribute types and values..."
    # start_time = time.time()
    # print "START:", start_time

    # with open("TEST-attr_common.csv", "w") as outfile:
    #     csvout = csv.writer(outfile)
        
    #     with driver.session() as session:
    #         results = session.run(
    #             "MATCH (:Sample)-[u:hasAttribute]->(a:Attribute) \
    #             RETURN a.type AS type, COUNT(u) AS usage_count \
    #             ORDER BY usage_count DESC LIMIT 3000"
    #         )
            
    #         for result in results:
    #             print "{} ({})".format(result["type"], result["usage_count"])
    #             row = ["{} | {}".format(result["type"], result["usage_count"])]

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
    
    # end_time = time.time()
    # print "END:", end_time
    # total_time = end_time - start_time           
    
    # print "** DONE - generated spreadsheet (attr_common.csv) of most common attribute types and values in %s seconds" % total_time
    # print "Time in minutes", total_time/60

