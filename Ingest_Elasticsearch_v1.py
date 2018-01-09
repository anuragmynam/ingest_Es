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
--------- # Add the mapper section once you recieve the mapper file 

with open('index_format_file.json') as mapper_file:
	json.load(mapper_file)

def 

'''

def post_to_es(records, index_name):
	es = Elasticsearch(['https://search-dexi-elastic-chyww75sud3elx5ppuez5b7p5q.us-east-1.es.amazonaws.com'])
	helpers.bulk(es, records, index=index_name, doc_type='json')


def function_to_send(chunk_size, transaction_type, index_name):
	chunksize = 3000
	csv_filename = '/Users/venkatmynam/Desktop/Query_Results.csv'
	f = open(csv_filename) # read csv
	csvfile = pd.read_csv(f, iterator=True, chunksize=chunksize) 
	for i,df in enumerate(csvfile): 
	    df_filtered = df[(df.award_category == transaction_type)].T.to_dict()
	    #records=df.where(pd.notnull(df), None).T.to_dict()
	    list_records=[records[it] for it in df_filtered]
	    #print(list_records)
	    post_to_es(list_records, index_name)



function_to_send(5000, "loans", "transaction-loans")



