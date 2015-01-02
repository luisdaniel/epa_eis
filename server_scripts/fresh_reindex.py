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
from bs4 import BeautifulSoup as bs
import re
import json
from mongoengine import *
import models
import bson
from bson import json_util
import pandas as pd
import httplib
import subprocess

#get keys
json_data=open('k.json')
keys = json.load(json_data)
json_data.close()
HOST = keys['es_host']
USER = keys['es_user']
PASS = keys['es_pass']
MONGO = keys['mongo_uri']
INDEX = 'impactstatement'
PARENT = 'report'

#connect to ES
from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC 
host_params = {'host':keys['es_host'], 'port':80, 'use_ssl':False}
es = ES([host_params], connection_class=RC, http_auth=(keys['es_user'], keys['es_pass']),  use_ssl=False)

#connect to S3
conn = S3Connection(keys['aws_key'], keys['aws_secret'])
bucket = conn.get_bucket('epaeis')
bucket.list()

#connect to mongo
connect('db', host=MONGO)


def main():
	clear_and_map()
	index_all_reports()
	index_all_report_files()

def clear_and_map():
    #clear and create all new mapping.
    print "Clearing old index and creating new one with mapping."

    cmd = "curl -X DELETE \"{}/{}\"".format(HOST, INDEX)
    os.system(cmd)

    #create index
    cmd = "curl -X PUT \"{}/{}\"".format(HOST, INDEX)
    os.system(cmd)

    #create mapping
    cmd = "curl -X PUT \"{}/{}/{}/_mapping?pretty=1\" -d \'".format(HOST, INDEX, PARENT)
    cmd +='''{
        "report": {
          "_id": {
            "index":"not_analyzed",
            "store": true
          },
          "properties": {
            "agency" : {
              "type" : "string"
            },
            "agency_abbrev" : {
              "type" : "string",
              "index" : "not_analyzed"
            },
            "amended_notice" : {
              "type" : "string"
            },
            "amended_notice_date" : {
              "type" : "date",
              "format" : "dateOptionalTime"
            },
            "comment_due_review_date" : {
              "type" : "date",
              "format" : "dateOptionalTime"
            },
            "comment_letter_date" : {
              "type" : "date",
              "format" : "dateOptionalTime"
            },
            "contact_name" : {
              "type" : "string"
            },
            "contact_phone" : {
              "type" : "string"
            },
            "date" : {
              "type" : "date",
              "format" : "dateOptionalTime"
            },
            "date_uploaded" : {
              "type" : "date",
              "format" : "dateOptionalTime"
            },
            "document_type" : {
              "type" : "string"
            },
            "eis_number" : {
              "type" : "string",
              "index" : "not_analyzed"
            },
            "federal_register_date" : {
              "type" : "date",
              "format" : "dateOptionalTime"
            },
            "rating" : {
              "type" : "string",
              "index" : "not_analyzed"
            },
            "report_link" : {
              "type" : "string",
              "index" : "not_analyzed"
            },
            "state" : {
              "type" : "string"
            },
            "state_abbrev" : {
              "type" : "string",
              "index" : "not_analyzed"
            },
            "supplemental_info" : {
              "type" : "string"
            },
            "title" : {
              "type" : "string"
            },
            "website" : {
              "type" : "string",
              "index" : "not_analyzed"
            }
          }
        }
    }\''''
    os.system(cmd)
    cmd = "curl -X PUT \"{}/{}/{}/_mapping?pretty=1\" -d \'".format(HOST, INDEX, 'eis_file')
    cmd +='''{
        "eis_file": {
          "_parent": {
            "type":"report"
          },
          "properties": {
            "file": {
              "type": "attachment",
              "fields": {
                "title" : { "store" : "yes" },
                "file" : { "term_vector":"with_positions_offsets", "store":"yes" },
                "content_type": {"type":"string", "store":"yes"},
                "date" : {"store" : "yes"},
                "keywords" : {"store" : "yes", "analyzer":"keyword"},
                "content_length" : {"type":"string", "store" : "yes"}
              }
            },
            "file_url": {
                "type":"string",
                "index":"not_analyzed"
            },
            "eis_number": {
                "type": "string",
                "index":"not_analyzed
            }
            "title": {
              "type":"string"
            }
        }
      }
    }\''''
    os.system(cmd)
    cmd = "curl -X PUT \"{}/{}/{}/_mapping?pretty=1\" -d \'".format(HOST, INDEX, 'comment_letter')
    cmd +='''{
      "comment_letter": {
          "_parent": {
            "type":"report"
          },
          "properties": {
            "file": {
              "type": "attachment",
              "fields": {
                "title" : { "store" : "yes" },
                "file" : { "term_vector":"with_positions_offsets", "store":"yes" },
                "content_type": {"type":"string", "store":"yes"},
                "date" : {"store" : "yes"},
                "keywords" : {"store" : "yes", "analyzer":"keyword"},
                "content_length" : {"type":"string", "store" : "yes"}
              }
            },
            "file_url": {
                "type":"string",
                "index":"not_analyzed"
            },
            "eis_number": {
                "type": "string",
                "index":"not_analyzed
            },
            "title": {
              "type":"string"
            }
        }
      }
    }\''''
    os.system(cmd)

def index_all_reports():
    data = pd.read_csv('reports.csv')
    data = data.fillna('')
    all_reports = [row[1]['eis_number'] for row in data.T.iteritems()]
    for eis in all_reports:
        print "indexing: " + str(eis)
        report = models.Report.objects(eis_number=str(eis)).first()
        if report:
            doc = {
                "agency_abbrev": report.agency[0],
                "amended_notice": report.amended_notice,
                "amended_notice_date": report.amended_notice_date,
                "comment_due_review_date": report.comment_due_review_date,
                "comment_letter_date": report.comment_letter_date,
                "contact_name": report.contact_name,
                "contact_phone": report.contact_phone,
                "date_uploaded": report.date_uploaded,
                "date": report.federal_register_date,
                "document_type": report.document_type,
                "eis_number": report.eis_number,
                "federal_register_date": report.federal_register_date,
                "rating": report.rating,
                "report_link": report.report_link,
                "state_abbrev": report.state[0],
                "supplemental_info": report.supplemental_info.encode('utf-8'),
                "title": report.title,
                "website": report.website,
            }
            res = es.index(index=INDEX, doc_type=PARENT, id=eis, body=doc)
            print res['created']
        else: 
            print "Report not found: " + str(eis)

def index_all_report_files():
    data = pd.read_csv('reports.csv')
    data = data.fillna('')
    all_reports = [row[1]['eis_number'] for row in data.T.iteritems()]
    missing_data = {
        "eis":[],
        "file_url":[],
        "error":[]
    }
    for eis in missing_reports:
        report = models.Report.objects(eis_number=str(eis)).first()
        if report:
            for f in report.report_files:
                file_url = f.file_url_s3.replace("https://s3.amazonaws.com/epaeis/","").replace(".pdf",".txt")
                key = bucket.get_key(file_url)
                if key:
                    try:
                        temp_file = key.get_contents_to_filename('temp.txt')
                        file64 = open('temp.txt', "rb").read().encode("base64")
                        filejson = open('temp.json', 'w')
                        data = { 
                            'file': file64, 
                            'title': f.title, 
                            'file_url':f.file_url_s3,
                            'eis_number': eis
                        }
                        print "Indexing: " + file_url
                        json.dump(data, filejson)
                        filejson.close()
                        cmd = 'curl -u {}:{} -X POST "{}/{}/{}?parent={}" -d @'.format(USER, PASS, HOST, INDEX, "eis_file", eis)
                        cmd += 'temp.json'
                        os.system(cmd)
                        os.remove('temp.txt')
                    except Exception, e:
                        print "Could not get: " + file_url
                        missing_data['eis'].append(eis)
                        missing_data['file_url'].append(file_url)
                        missing_data['error'].append("Could not find file on S3")
                else:
                    print "Could not get: " + file_url
                    missing_data['eis'].append(eis)
                    missing_data['file_url'].append(file_url)
                    missing_data['error'].append("Could not find file on S3")
    dataFrame = pd.DataFrame(missing_data)
    dataFrame.to_csv('fresh_reindex_error_log.csv', encoding='utf-8')

main()


