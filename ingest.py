#!/usr/bin/env python3

"""
This script takes some data and ingests it into an ElasticSearch database.
"""

import json
import logging
import sys
import pandas as pd
from elasticsearch import Elasticsearch

logging.basicConfig(level=logging.DEBUG, format='\n%(levelname)s: %(message)s')


def create_map(es, index_name, type_name):
    """ If this index does not appear in ElasticSearch, we will create the
        index and the mapping for said index here.

        Inputs:
        es: ElasticSearch connector
        index_name: The name of the index/table
        type_name: The name of the type/rows

        Output:
        An ElasticSearch Table will be created with the mapping specified here.

        Note, in the dictionary below 'properties', you will give each of your
        column names a dict where at least 'type' is specified. Docs are at:
        https://elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html

        Note the last variable type has no comma preceding it
    """
    # Creates Mapping
    request_body = {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {
            type_name: {
                'properties': {
                    'date': {'type': 'date',
                             'format': "yyyy-MM-dd_HH:mm:ss"},
                    'decimalNumber': {'type': 'long'},
                    'wholenumber': {'type': 'short'},
                    'coordinate': {'type': 'geo_point',
                                   'index': 'not_analyzed'},
                    'text': {'type': 'text', "index": "false"},  # 1 Word
                    'keyword': {'type': 'keyword'}  # Break to many words
                }
            }
        }
    }

    es.indices.create(index=index_name, body=request_body)


def read_csv(csv_file):
    """ This function loads the data in this CSV file and saves it as a Pandas
        DataFrame first. From there, we convert it into a JSON dict object.

        Inputs:
        csv_file: Path to a CSV file

        Outputs:
        data_dict: Dictionary of the CSV file's contents
    """
    data_df = pd.read_csv(csv_file)
    data_dict = data_df.to_dict(orient='records')

    # Replace this with processing as needed
    return data_dict


def ingest_data(data):
    """ This function ingests a dictionary object (from what used to be a
        Pandas DataFrame) into ElasticSearch. If this index does not exist in
        ElasticSearch, we will create the mapping first.

        Inputs:
        data: Dictionary of data to ingest into ElasticSearch.
    """
    # Set Index Settings here
    index_name = 'your_index_name_here'
    type_name = 'your_type_name_here'

    # Creates ElasticSearch connector
    es = Elasticsearch()

    # Creates index/mapping if it doesn't exist
    try:
        create_map(es, index_name, type_name)
        logging.info('Created new index, adding data now.')
    except Exception:
        logging.info('Index already created, adding data now.', exc_info=True)

    # Adds values to ElasticSearch index
    for i, doc in enumerate(data, start=1):
        try:
            # Create an 'ID' for each row which includes date. Name this idStr.
            id_str = 'your_id_string_here'

            # Ingests Dictionary into ElasticSearch
            es.index(index=index_name, doc_type=type_name, id=id_str,
                     body=json.dumps(doc))
            logging.info('All data was successfully ingested')
        except Exception:
            logging.error('Failed ingesting at {}/{} for: {}'
                          .format(i, len(data), json.dumps(doc)),
                          exc_info=True)


if __name__ == "__main__":
    ingest_data(read_csv(sys.argv[1]))
