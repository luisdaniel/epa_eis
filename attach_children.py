#Import stuff
import json
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import codecs
import csv
import os
import sys
import urllib2
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

HOST = "localhost:9200"
USER = keys['es_user']
PASS = keys['es_pass']
INDEX = 'impactstatement'
PARENT = 'report'


def main():
	index_all_files()


def report_exists(eis_number):
    res = es.get(index=INDEX, doc_type=PARENT, id=eis_number)
    return res['found']


def get_report_by_eis_number(eis_number):
    res = es.get(index=INDEX, doc_type=PARENT, id=eis_number)
    return res['_source']

def create_temp_file_from_s3(file_url):
    file_key = bucket.get_key(file_url)
    temp_file = file_key.get_contents_to_filename('temp.txt')

def remove_temp_file():
    os.remove('temp.txt')

def make_comment_letter_list():
    letters = []
    with open('comment_letters_metadata.csv', mode='r') as infile:
        reader = csv.reader(infile)
        for row in reader:
            if row[5] != 'file_url':
                file_name = urllib2.unquote(row[5])
                file_name = file_name[file_name.index('$file')+6:].replace(".PDF", "")
                letters.append(file_name)
    return letters

def comment_letter(file_url):
    return file_url in comment_letters

def index_individual_child(file_url):
    #skip if comment letter
    if file_url[len(file_url)-4:] == '.txt':
        if not comment_letter(file_url.replace(".txt","")[20:28]):
            base_url = 'https://s3.amazonaws.com/epaeis/'
            eis_number = file_url[11:19]
            create_temp_file_from_s3(file_url)
            title = file_url[20:].replace("-", " ").replace(".txt","").title()
            file64 = open('temp.txt', "rb").read().encode("base64")
            f = open('temp.json', 'w')
            data = { 
                'file': file64, 
                'title': title, 
                'file_url':base_url + file_url
            }
            print "Indexing: " + file_url
            json.dump(data, f) # dump json to tmp file
            f.close()
            remove_temp_file()
            cmd = 'curl -X POST "{}/{}/{}?parent={}" -d @'.format(HOST, INDEX, "eis_file", eis_number)
            cmd += 'temp.json'
            os.system(cmd)
        else: 
            print "Comment letter. Skipping."


def index_all_files():
    unindexed = []
    #get file from S3
    rs = bucket.list()
    for key in rs:
        index_individual_child(key.name)

comment_letters = make_comment_letter_list()
main()

