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

#PDF Miner
# from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
# from pdfminer.converter import TextConverter
# from pdfminer.layout import LAParams
# from pdfminer.pdfpage import PDFPage
# from cStringIO import StringIO

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
	index_missing_reports()
	index_missing_report_files()

def index_missing_reports():
    data = pd.read_csv('reports_to_be_added_to_mongo.csv')
    data = data.fillna('')
    missing_reports = [row[1]['eis_number'] for row in data.T.iteritems()]
    for eis in missing_reports:
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

def index_missing_report_files():
    data = pd.read_csv('reports_to_be_added_to_mongo.csv')
    data = data.fillna('')
    missing_reports = [row[1]['eis_number'] for row in data.T.iteritems()]
    for eis in missing_reports:
        report = models.Report.objects(eis_number=str(eis)).first()
        if report:
            for f in report.report_files:
                file_url = f.file_url_s3.replace("https://s3.amazonaws.com/epaeis/","").replace(".pdf",".txt")
                key = bucket.get_key(file_url)
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
                cmd = 'curl -X POST "{}/{}/{}?parent={}" -d @'.format(HOST, INDEX, "eis_file", eis)
                cmd += 'temp.json'
                os.system(cmd)
                os.remove('temp.txt')

main()




