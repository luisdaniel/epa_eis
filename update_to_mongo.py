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
from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC 
host_params = {'host':keys['es_host'], 'port':80, 'use_ssl':False}
es = ES([host_params], connection_class=RC, http_auth=(keys['es_user'], keys['es_pass']),  use_ssl=False)

#connect to S3
conn = S3Connection(keys['aws_key'], keys['aws_secret'])
bucket = conn.get_bucket('epaeis')
bucket.list()

#connect to mongo
connect('db', host=MONGO)

#base urls
base_url = "http://yosemite.epa.gov/oeca/webeis.nsf/viEIS01!OpenView&Start="
document_base_url="http://yosemite.epa.gov/oeca/webeis.nsf/"

def main():
	#get_eis_links_from_epa()
	#update_documents_to_mongo()
    #upload_files_to_s3()
    convert_files_to_text()

def get_max_records():
    page = urllib2.urlopen(base_url + str(1)).read()
    soup = bs(page)
    records = soup.findAll(text=re.compile(r'\bdocuments\swere\s\sretrieved'))[0]
    max_records = [int(s) for s in records.split() if s.isdigit()][0]
    return max_records

def get_eis_links_from_epa():
    columns = ['date', 'agency', 'state', 'document_type', 'title', 'report_link']
    df = pd.DataFrame(columns=columns)
    increment = 29
    index = 0
    max_records = get_max_records()
    for page_num in range(1, max_records, increment):
        page = urllib2.urlopen(base_url + str(page_num)).read()
        print "Getting page: " + base_url + str(page_num)
        soup = bs(page)
        table = soup.findAll('tr', attrs={"class":"viewdata"})
        for row in table:
            date = row.findAll('td')[0].text
            agency = row.findAll('td')[1].text
            state = row.findAll('td')[2].text
            document_type = row.findAll('td')[3].text
            title = row.findAll('td')[4].text
            report_link = document_base_url + row.findAll('td')[4].find('a')['href'].replace('?opendocument', '')
            new_row = [date, agency, state, document_type, title, report_link]
            for i in range(len(new_row)):  # For every value in our newrow
                if hasattr(new_row[i], 'encode'):
                    new_row[i] = new_row[i].encode('utf8')
            df.loc[index] = new_row
            index +=1
    df_unique = df.drop_duplicates()
    df_unique.to_csv('eis_links.csv', encoding='utf-8')

def get_eis_links_from_page(page_num):
    links = []
    base_url = "http://yosemite.epa.gov/oeca/webeis.nsf/viEIS01!OpenView&Start="
    page = urllib2.urlopen(base_url + str(page_num)).read()
    soup = bs(page)
    table = soup.findAll('tr', attrs={"class":"viewdata"})
    for row in table:
        report_link = document_base_url + row.findAll('td')[4].find('a')['href'].replace('?opendocument', '')
        links.append(report_link)
    return links

def already_indexed(eis_number):
    if models.Report.objects(eis_number=eis_number):
        return True
    else:
        return False


def get_metadata(file_url):
    c = httplib.HTTPConnection('yosemite.epa.gov:80')
    print "Getting " + file_url
    c.request('HEAD', file_url)
    try:
        r = c.getresponse()
    except Exception, e:
        print "Error, could not get link: " + str(e)
    if r.status == 200:
        header_response = r.getheaders()
        header = {}
        for h in header_response:
            header[h[0]] = h[1]
    else:
        print "Error, could not get link: " + str(r.status)
    return header

def save_documents_to_report(document_links, eis_number):
    r = models.Report.objects.get(eis_number=eis_number)
    for d in document_links:
        h = get_metadata(d['link'])
        rf = models.ReportFile()
        rf.content_length = h['content-length']
        rf.content_type = h['content-type']
        rf.date_retrieved = h['date']
        rf.file_url_epa = d['link']
        rf.title = d['title']
        last_modified = h['last-modified']
        base_url = 'https://s3.amazonaws.com/epaeis/'
        file_name = urllib2.unquote(d['link'])
        file_name = file_name[file_name.index('$file')+6:].replace(".pdf","")
        file_name = re.sub(r'([^\s\w])+', '', file_name).replace(" ", "-").lower() + ".pdf"
        file_name = r.date_uploaded.strftime('%m-%d-%Y') + "/" + r.eis_number + "/" + file_name
        file_name = base_url + file_name
        rf.file_url_s3 = file_name
        r.report_files.append(rf)
        r.save()

def save_comment_letter_to_report(document_links, eis_number):
    r = models.Report.objects.get(eis_number=eis_number)
    for d in document_links:
        h = get_metadata(d['link'])
        rf = models.CommentLetter()
        rf.content_length = h['content-length']
        rf.content_type = h['content-type']
        rf.date_retrieved = h['date']
        rf.file_url_epa = d['link']
        rf.title = d['title']
        last_modified = h['last-modified']
        base_url = 'https://s3.amazonaws.com/epaeis/'
        file_name = urllib2.unquote(d['link'])
        file_name = file_name[file_name.index('$file')+6:].replace(".pdf","")
        file_name = re.sub(r'([^\s\w])+', '', file_name).replace(" ", "-").lower() + ".pdf"
        file_name = r.date_uploaded.strftime('%m-%d-%Y') + "/" + r.eis_number + "/" + file_name
        file_name = base_url + file_name
        rf.file_url_s3 = file_name
        r.comment_letters.append(rf)
        r.save()

def update_documents_to_mongo():
    max_pages = int(get_max_records()/29)+1
    i = 1
    while i <= max_pages:
        links = get_eis_links_from_page(i)
        for link in links:
            page = urllib2.urlopen(link).read()
            soup = bs(page)
            table = soup.findAll('td')
            eis_number = table[3].text
            print "Getting #" + eis_number
            if not already_indexed(eis_number):
                r = models.Report()
                r.eis_number = eis_number
                r.title = table[1].text
                if table[5].text == 'Multi':
                    r.state.append('00')
                else:
                    r.state.append(table[5].text)
                r.document_type = table[7].text
                r.agency.append(table[9].text)
                r.federal_register_date = table[11].text
                r.date_uploaded = table[11].text
                r.contact_name = table[13].text
                r.comment_due_review_date = table[15].text
                r.contact_phone = table[17].text
                r.amended_notice_date = table[19].text
                r.amended_notice = table[21].text
                r.supplemental_info = table[23].text
                r.website = table[25].text.rstrip()
                r.comment_letter_date = table[27].text.rstrip()
                r.rating = table[29].text
                r.save()
                #sometimes you have comment letters which changes the order of the table rows
                check_row = table[30]
                comment_letter_links = []
                document_links = []
                exclude_links = ['http://www.epa.gov/epahome/pdf.html',
                                 'http://www.epa.gov/compliance/contact/nepa.html#commentform']
                if check_row.find(text=re.compile(r'\bComment\sLetter\(s\)')):
                    base_file_url = 'http://yosemite.epa.gov/oeca/webeis.nsf/'
                    file_links = table[30].findAll('a')
                    for l in file_links:
                        if l['href'] not in exclude_links:
                            cl_l = document_base_url + urllib2.quote(l['href'].encode('utf8').replace('?OpenElement', '').replace('../',''))
                            cl_t = l.getText()
                            comment_letter_links.append({"link":cl_l, "title":cl_t})
                    file_links = table[31].findAll('a')
                    for l in file_links:
                        d_l = document_base_url + urllib2.quote(l['href'].encode('utf8').replace('?OpenElement', '').replace('../',''))
                        d_t = l.getText()
                        document_links.append({"link":d_l, "title":d_t})
                else: 
                    file_links = table[30].findAll('a')
                    for l in file_links:
                        d_l = document_base_url + urllib2.quote(l['href'].encode('utf8').replace('?OpenElement', '').replace('../',''))
                        d_t = l.getText()
                        document_links.append({"link":d_l, "title":d_t})
                save_documents_to_report(document_links, r.eis_number)
                save_comment_letter_to_report(comment_letter_links, r.eis_number)
                print "Saved: " + r.eis_number
                i += 1
            else:
                print "Already indexed: " + eis_number
                i += 1
                i = max_pages + 1
                print "Reports updated on mongo"
                break

def upload_files_to_s3():
    i = 0
    reports = models.Report.objects()
    for r in reports:
        for f in r.report_files:
            k = Key(bucket)
            file_name = urllib2.unquote(f.file_url_epa)
            file_name = file_name[file_name.index('$file')+6:].replace(".pdf","")
            file_name = re.sub(r'([^\s\w])+', '', file_name).replace(" ", "-").lower() + ".pdf"
            file_name = r.date_uploaded.strftime("%m-%d-%Y") + "/" + r.eis_number + "/" + file_name
            if bucket.get_key(file_name):
                print "File exists, skipping"
            else:
                try:
                    print "Saving: " + file_name
                    f = urllib2.urlopen(link)
                    data = f.read()
                    f.close()
                    k = bucket.new_key(file_name)
                    k.set_contents_from_string(data)
                    print i
                    i +=1
                except Exception, e:
                    print "Could not get because: " + str(e)

def url_to_filename(url, date, eis):
    file_name = urllib2.unquote(url)
    file_name = file_name[file_name.index('$file')+6:].replace(".pdf","")
    file_name = re.sub(r'([^\s\w])+', '', file_name).replace(" ", "-").lower() + ".pdf"
    file_name = date.strftime("%m-%d-%Y") + "/" + eis + "/" + file_name
    return file_name

def convert_files_to_text():
    index_missing = 0
    columns = ['eis', 'link', 'error']
    df_missing_files = pd.DataFrame(columns=columns)
    reports = models.Report.objects().only('report_files', 'date_uploaded', 'eis_number')
    for r in reports:
        for f in r.report_files:
            file_name = url_to_filename(f.file_url_epa, r.date_uploaded, r.eis_number)
            print "Processing: " + file_name
            if bucket.get_key(file_name):
                key = bucket.get_key(file_name)
                if not bucket.get_key(key.name.replace(".pdf", ".txt")):
                    res = key.get_contents_to_filename('myfile.pdf')
                    try:
                        subprocess.check_output(['pdftotext', "myfile.pdf"])
                    except Exception, e:
                        print "Could not convert PDF: " + str(e)
                        new_missing_row = [r.eis_number, file_name, str(e)]
                        df_missing_files.loc[index_missing] = new_missing_row
                        index_missing +=1
                    try:
                        os.remove('myfile.pdf')
                        new_key = Key(bucket)
                        new_key.key = key.name.replace(".pdf", ".txt")
                        f = open("myfile.txt", 'r')
                        new_key.set_contents_from_file(f)
                        f.close()
                        os.remove("myfile.txt")
                    except Exception, e:
                        print "Could not convert PDF: " + str(e)
                        new_missing_row = [r.eis_number, file_name, 'Not found in S3']
                        df_missing_files.loc[index_missing] = new_missing_row
                        index_missing +=1
                else:
                    print "Text file already exists"
            else:
                print "File not found in S3: " + r.eis_number + ": " + file_name
                new_missing_row = [r.eis_number, file_name, 'Not found in S3']
                df_missing_files.loc[index_missing] = new_missing_row
                index_missing +=1




main()















