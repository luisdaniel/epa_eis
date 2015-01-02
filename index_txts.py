#Import stuff
import json
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import codecs
import pickle
import os
import sys
from datetime import datetime

#get keys
json_data=open('k.json')
keys = json.load(json_data)
json_data.close()

#connect to ES
from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC 
host_params = {'host':keys['es_host'], 'port':80, 'use_ssl':False}
es = ES([host_params], connection_class=RC, http_auth=(keys['es_user'], keys['es_pass']),  use_ssl=False)

#connect to S3
conn = S3Connection(keys['aws_key'], keys['aws_secret'])
bucket = conn.get_bucket('epaeis')
bucket.list()

HOST = 'localhost:9200'
USER = keys['es_user']
PASS = keys['es_pass']
INDEX = 'eistxt'
TYPE = 'attachment'

def main():
	if sys.argv[1] == 'clear':
		clear_and_map()
	index_all_files()


def clear_and_map():
    #clear and create all new mapping.
    print "Clearing old index and creating new one with mapping."

    cmd = "curl -X DELETE \"{}/{}\"".format(HOST, INDEX)
    os.system(cmd)

    #create index
    cmd = "curl -X PUT \"{}/{}\"".format(HOST, INDEX)
    os.system(cmd)

    #create mapping
    cmd = "curl -X PUT \"{}/{}/{}/_mapping\" -d \'".format(HOST, INDEX, TYPE)
    cmd +='''{
      "attachment" : {
        "properties" : {
          "file" : {
            "type" : "attachment",
            "fields" : {
                "title" : { "store" : "yes" },
                "file" : { "term_vector":"with_positions_offsets", "store":"yes" },
                "content_type": {"type":"string", "store":"yes"},
                "date" : {"store" : "yes"},
                "keywords" : {"store" : "yes", "analyzer":"keyword"},
                "content_length" : {"type":"string", "store" : "yes"}
            }
          },
            "eis": {
                "type":"string",
                "index":"not_analyzed"
            },
            "file_url": {
                "type":"string",
                "index":"not_analyzed"
            }
        }
      }
    }\''''
    os.system(cmd)


#check wether a file has already been indexed
def index_exists(file_url):
    res = es.search(index=INDEX, body={
        "fields": ['eis'],
        "query" : {
            "filtered" : {
                "filter" : {
                    "term" : {
                        "file_url" : file_url
                    }
                }
            }
        }
    })
    return res['hits']['hits']


def index_file(file_url, file_name):
    title = file_url[20:].replace("-", " ").replace(".pdf","").replace(".txt","").title()
    eis = file_url[11:19]
    epa_date = file_url[:10].replace("-","/")
    file64 = open(file_name, "rb").read().encode("base64")
    f = open('temp.json', 'w')
    data = { 
        'file': file64, 
        'title': title, 
        'eis': eis, 
        'file_url':file_url
    }
    json.dump(data, f) # dump json to tmp file
    f.close()
    cmd = 'curl -X POST "{}/{}/{}" -d @'.format(HOST,INDEX,TYPE)
    cmd += 'temp.json'
    os.system(cmd)


#index all files
def index_all_files():
	unindexed = []
	#get file from S3
	rs = bucket.list()
	for key in rs:
		if key.name[len(key.name)-4:] == '.txt': #only PDF's
			try:
				if index_exists(key.name):
					print str(key.name) + " already indexed"
					#add to a list or something.
				else:
					print "Getting: " + key.name
					file_key = bucket.get_key(key.name)
					if file_key.size > 100000000:
						print "Too large, using txt file"
						file_url = key.name.replace(".pdf", ".txt")
						file_key = bucket.get_key(file_url)
						temp_file_name = 'temp_file.txt'
						temp_file = file_key.get_contents_to_filename(temp_file_name)
					else:
						file_url = key.name
						temp_file_name = 'temp_file.pdf'
						temp_file = file_key.get_contents_to_filename(temp_file_name)
					index_file(file_url, temp_file_name)
					os.remove(temp_file_name)
			except Exception, e:
				print "Could not get file: " + str(e)
				unindexed.append(key.name)
	print unindexed



main()






