import os
import sys
import csv
import json
import pandas as pd
from elasticsearch import helpers, Elasticsearch
import argparse


# def createindices(client):
#     client.indices.create('transaction_contract_2017')
#     client.indices.create('transaction_direct_payment_2017')

'''
--------- # Add the mapper section once you receive the mapper file 

with open('index_format_file.json') as mapper_file:
	json.load(mapper_file)

def master_function(chunk,base_index_name='transactions'):
    indices_dict = defaultdict(list)
    for line in chunk:
        index_name = '{}-{}'.format(base_index_name, lower(line['transaction_award_type'])
        indices_dict[index_name].append(link)
    return indices_dict

'''

# def post_to_es(records, index_name):
# 	es = Elasticsearch(['https://search-dexi-elastic-chyww75sud3elx5ppuez5b7p5q.us-east-1.es.amazonaws.com'])
# 	helpers.bulk(es, records, index=index_name, doc_type='json')


# def function_to_send(chunk_size_total, transaction_type, index_name):
# 	chunksize = chunk_size_total
# 	csv_filename = '/Users/venkatmynam/Desktop/Query_Results.csv'
# 	f = open(csv_filename) # read csv
# 	csvfile = pd.read_csv(f, iterator=True, chunksize=chunksize) 
# 	for i,df in enumerate(csvfile): 
# 	    df_filtered = df[(df.award_category == transaction_type)].T.to_dict()
# 	    #records=df.where(pd.notnull(df), None).T.to_dict()
# 	    list_records=[records[it] for it in df_filtered]
# 	    #print(list_records)
# 	    post_to_es(list_records, index_name)



def gen_chunks(reader, fields, chunksize=100):
    chunk = []
    g = lambda x: dict(zip(fields,x))
    for index, line in enumerate(reader):
        if (index % chunksize == 0 and index > 0):
            yield chunk
            del chunk[:]
        chunk.append(g(line))
    yield chunk

def post_to_es(records, index_name):
	es = Elasticsearch(['https://search-dexi-elastic-chyww75sud3elx5ppuez5b7p5q.us-east-1.es.amazonaws.com'])
	helpers.bulk(es, records, index=index_name, doc_type='json')


with open('/Users/venkatmynam/Desktop/query_export_2017.csv', 'r')  as file:
	reader = csv.reader(file)
	fields = reader.next()
	csv_generator = gen_chunks(reader, fields)
	print(fields)
	for chunk in csv_generator:
		post_to_es(chunk, "testing_chunk_am")


# import sys
# print sys.version

# for chunk in gen_chunks(range(10), chunksize=3):
#     print chunk # process chuck
















