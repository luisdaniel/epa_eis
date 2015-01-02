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

# #PDF Miner
# from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
# from pdfminer.converter import TextConverter
# from pdfminer.layout import LAParams
# from pdfminer.pdfpage import PDFPage
# from cStringIO import StringIO

#get keys
# json_data=open('k.json')
# keys = json.load(json_data)
# json_data.close()
# HOST = keys['es_host']
# USER = keys['es_user']
# PASS = keys['es_pass']
# MONGO = keys['mongo_uri']
# INDEX = 'impactstatement'
# PARENT = 'report'

#connect to ES
# from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC 
# host_params = {'host':keys['es_host'], 'port':80, 'use_ssl':False}
# es = ES([host_params], connection_class=RC, http_auth=(keys['es_user'], keys['es_pass']),  use_ssl=False)

#connect to S3
# conn = S3Connection(keys['aws_key'], keys['aws_secret'])
# bucket = conn.get_bucket('epaeis')
# bucket.list()

#connect to mongo
# connect('db', host=MONGO)


def main():
	print "Scraping reports..."
	create_full_csv_of_reports()


def create_full_csv_of_reports():
    data = {
        'agency':[],
        'amended_notice':[],
        'amended_notice_date':[],
        'comment_due_review_date':[],
        'comment_letter_date':[],
        'comment_letters': [],
        'comment_letters_titles': [],
        'contact_name':[],
        'contact_phone':[],
        'date_uploaded':[],
        'document_type':[],
        'eis_number':[],
        'federal_register_date':[],
        'rating':[],
        'report_files': [],
        'report_files_titles': [],
        'report_link':[],
        'state':[],
        'supplemental_info': [],
        'title': [],
        'website': []
    }
    #date = row[2]
    #report_link = row[4]
    #title = row[6]
    with open('eis_links.csv', mode='r') as infile:
        reader = csv.reader(infile)
        for row in reader:
            if row[4] != 'report_link':
                print "Getting: " + row[4]
                page = urllib2.urlopen(row[4]).read()
                soup = bs(page)
                table = soup.findAll('td')
                data['agency'].append(table[9].text)
                data['amended_notice'].append(table[21].text)
                data['amended_notice_date'].append(table[19].text)
                data['comment_due_review_date'].append(table[15].text)
                data['comment_letter_date'].append(table[27].text.rstrip())
                data['contact_name'].append(table[13].text)
                data['contact_phone'].append(table[17].text)
                data['date_uploaded'].append(table[11].text)
                data['document_type'].append(table[7].text)
                data['eis_number'].append(table[3].text)
                data['federal_register_date'].append(table[11].text)
                data['rating'].append(table[29].text)
                data['report_link'].append(row[2])
                data['state'].append('00' if table[5].text == 'Multi' else table[5].text)
                data['supplemental_info'].append(table[23].text)
                data['title'].append(row[6])
                data['website'].append(table[25].text.rstrip())
                #sometimes you have comment letters which changes the order of the table rows
                exclude_links = [
                        'http://www.epa.gov/epahome/pdf.html',
                        'http://www.epa.gov/compliance/contact/nepa.html#commentform']
                if table[30].find(text=re.compile(r'\bComment\sLetter\(s\)')):
                    data['comment_letters'].append('||'.join([l['href'] for l in table[30].findAll('a') if l['href'] not in exclude_links]))
                    data['comment_letters_titles'].append('||'.join([l.getText() for l in table[30].findAll('a') if l['href'] not in exclude_links]))
                    data['report_files'].append('||'.join([l['href'] for l in table[31].findAll('a')]))
                    data['report_files_titles'].append('||'.join([l.getText() for l in table[31].findAll('a')]))
                else:
                    data['comment_letters'].append("")
                    data['comment_letters_titles'].append("")
                    data['report_files'].append('||'.join([l['href'] for l in table[30].findAll('a')]))
                    data['report_files_titles'].append('||'.join([l.getText() for l in table[30].findAll('a')]))
    dataFrame = pd.DataFrame(data)
    dataFrame.to_csv('reports.csv', encoding='utf-8')
    if dataFrame.shape[0]:
        return True
    else:
        return False


main()