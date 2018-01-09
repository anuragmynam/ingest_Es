import csv
import os
import argparse
import json
from collections import defaultdict
from elasticsearch import Elasticsearch, helpers

host = host=os.getenv('ES_HOSTNAME')
#host = 'https://search-dexi-elastic-chyww75sud3elx5ppuez5b7p5q.us-east-1.es.amazonaws.com'
client = Elasticsearch(host)

with open('es_mapping.json') as f:
    data = json.load(f)
    ES_MAPPING = json.dumps(data)

def gen_chunks(reader, fields, chunksize):

    """
    Chunk generator. Take a CSV `reader` and yield
    `chunksize` sized slices.
    fields
    --------
    list of csv fields mapping data to ['text','title','date']
    """

    g = lambda x: dict(zip(fields,x))

    chunk = []
    for i, line in enumerate(reader):
        if (i % chunksize == 0 and i > 0):
            yield chunk
            del chunk[:]
        chunk.append(g(line))
    yield chunk

def break_up_chunk(chunk,base_index_name):
    '''chunk - list of dictionaries'''
    indices_dict = defaultdict(list)
    for line in chunk:
        index_name = '{}-{}{}'.format(base_index_name, line['award_category'].replace(" ", "").lower(),'s')
        # print(index_name)
        indices_dict[index_name].append(line)
    return indices_dict

def post_to_es(chunk,args):
    indices_dict = break_up_chunk(chunk,args.base_indexname)
    for index_name,body in indices_dict.items():
        print(index_name)
        if not client.indices.exists(index_name):
        #     client.indices.create(index=str(index_name),body=ES_MAPPING)
        # helpers.bulk(client, body, index=index_name, doc_type='json')

def main():
    #TODO make models argument
    argument_parser = argparse.ArgumentParser('Will add help text')
    argument_parser.add_argument('--infile',type=str,
                                    required=True)
    argument_parser.add_argument('--base_indexname',type=str,default='sample-transactions')
    argument_parser.add_argument('--doc_type',type=str,default='json')
    argument_parser.add_argument('--chunk_size',type=int,
                                default=100000)
    args = argument_parser.parse_args()
    count = 0

    with open(args.infile) as f:
        reader = csv.reader(f)
        fields = reader.next()
        csv_generator = gen_chunks(reader,fields,args.chunk_size)
        chunk = csv_generator.next()
        post_to_es(chunk,args)

if __name__ == '__main__':
    main()


