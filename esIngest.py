#!/usr/bin/env python3

"""
This script takes some data and ingests it into an ElasticSearch database.
"""

from argparse import ArgumentParser
import datetime as dt
from elasticsearch import Elasticsearch
import json
import pandas as pd
import sys


def createMap(es, indexName, typeName):
	""" If this index does not appear in ElasticSearch, we will create the index
		and the mapping for said index here. 
		
		Inputs:
		es: ElasticSearch connector
		indexName: The name of the index/table
		typeName: The name of the type/rows
		
		Output:
		An ElasticSearch Table will be created with the mapping specified here.
	"""
    # Creates Mapping
    
    """
    Note, the dictionary below 'properties', you will give each of your column
    names a dict where at least 'type' is specified. Below are some examples.
    
    Date: You need to give date a format so the string can be correlated to an
    	  actual timestamp. A table with what each 'formatting directive' is at:
    	  https://docs.python.org/2/library/datetime.html#strftime-and-strptime-behavior
    Coordinate: This must be a string so that => "<latitude>, <longitude>"
    String: If "index":"not_analyzed" is set, the string will only be found if
    		you search for its full value. If you remove that value, the string
    		can be found if you searched for part of the string. The latter uses
    		more system resources at the benefit of greater searching ease.
    
    Note the last variable type has no comma preceding it
    """
    requestBody = {
        "settings" : {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {
            typeName: {
                'properties': {
                    'date': {'type':'date', 
                             'format':"yyyy-MM-dd_HH:mm:ss"},
                    'decimalNumber': {'type':'long'},
                    'wholenumber': {'type':'short'},
                    'coordinate': {'type':'geo_point', 'index':'not_analyzed'},
                    'string': {'type': 'string', "index": "not_analyzed"},
                    'string': {'type': 'string'}
                }
            }
        }
    }
    
    es.indices.create(index=indexName, body=requestBody)


def loadData(pathToFile):
	""" This function loads the data in this CSV file and saves it as a Pandas
		DataFrame first. From there, we convert it into a JSON dict object.
		
		Inputs:
		pathToFile: Path to a CSV file
		
		Outputs:
		df: The DataFrame contents of this CSV file, converted to a dictionary
	"""
	df = pd.read_csv(pathToFile)
	
	## Replace this with processing as needed
	
	return df.to_dict(orient='records')


def ingestEs(data):
	""" This function ingests a dictionary object (from what used to be a 
		Pandas DataFrame) into ElasticSearch. If this index does not exist in
		ElasticSearch, we will create the mapping first.
		
		Inputs:
		data: Dictionary of data to ingest into ElasticSearch.
	"""
	# Set Index Settings here
	#indexName = <nameOfTableHere>
	#typeName = <nameOfEachRowHere>
	
	# Creates ElasticSearch connector
	es = Elasticseach()
	
	# Creates index/mapping if it doesn't exist
	try:
		createMap(es, indexName, typeName)
		print('Created new index. Adding data now.')
	except:
		print('Index already created. Adding data now.')
	
	# Adds values to ElasticSearch index
    for i, doc in enumerate(data, start=1):
        try:
            # Create an 'ID' for each row which includes date. Name this idStr.
            # idStr = Fill Code here
            
            # Ingests Dictionary into ElasticSearch
            es.index(index=indexName, doc_type=typeName, id=idStr, 
                     refresh=True, body=json.dumps(doc))
            status = 'SUCCESS: All Runs Indexed'
        except Exception as e:
            print('ERROR: Failed at {}/{} for: {}'.format(i, len(data), 
                                                          json.dumps(doc)))
            print(e)
            # By failing to make a status in this case, we let the script crash
    return status


if __name__ == "__main__":
	""" By default, when this script is ran, we will load these two functions.
		Specifically we will load the data, save it as a dictionary, and then
		send that to the ingestEs() function to add it to ElasticSearch.
	"""
	data = loadData(sys.argv[1])
	status = ingestEs(data)
	print(status)