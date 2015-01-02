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
# from bs4 import BeautifulSoup as bs
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
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
from cStringIO import StringIO

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
# from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC 
# host_params = {'host':keys['es_host'], 'port':80, 'use_ssl':False}
# es = ES([host_params], connection_class=RC, http_auth=(keys['es_user'], keys['es_pass']),  use_ssl=False)

#connect to S3
conn = S3Connection(keys['aws_key'], keys['aws_secret'])
bucket = conn.get_bucket('epaeis')
bucket.list()

# connect to mongo
connect('db', host=MONGO)


def main():
	print "Converting PDF files to text..."
	convert_files_to_text()

def convert_files_to_text():
    data = {
        "eis_number":[],
        "link":[],
        "error":[]
    }
    reports = models.Report.objects().only('report_files.file_url_s3', 'report_files.converted_to_text')
    for r in reports:
        for rf in r.report_files:
            if not rf.converted_to_text:
                print "Processing: " + rf.file_url_s3
                key = bucket.get_key(rf.file_url_s3.replace("https://s3.amazonaws.com/epaeis/",""))
                key.get_contents_to_filename('temp_file.pdf')
                try:
                    subprocess.check_output(['pdftotext', "temp_file.pdf"])
                    try:
                        new_key = Key(bucket)
                        new_key.key = key.name.replace(".pdf", ".txt")
                        f = open("temp_file.txt", 'r')
                        new_key.set_contents_from_file(f)
                        f.close()
                        rf.converted_to_text = True
                        r.save()
                    except Exception, e:
                        print "Could not save to S3: " + str(e)
                        data['eis_number'].append(r.eis_number)
                        data['link'].append(rf.file_url_s3)
                        data['error'].append(str(e))
                except Exception, e:
                    print "Could not convert PDF: " + str(e)
                    data['eis_number'].append(r.eis_number)
                    data['link'].append(rf.file_url_s3)
                    data['error'].append(str(e))
    dataFrame = pd.DataFrame(data)
    dataFrame.to_csv('unable_to_convert_to_text.csv', encoding='utf-8')


main()


