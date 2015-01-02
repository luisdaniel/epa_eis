#Import stuff
import json
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
MONGO = keys['mongo_uri']

#connect to mongo
connect('db', host=MONGO)

base_url = "http://yosemite.epa.gov/oeca/webeis.nsf/viEIS01!OpenView&Start="
document_base_url="http://yosemite.epa.gov/oeca/webeis.nsf/"

def main():
	print "Fetching Report Links from EPA site"
	get_eis_links_from_epa()

	print "Cross checking for missing reports in Mongo"
	new_available = cross_check_with_mongo()

	if new_available:
		get_missing_data_for_mongo()
		add_reports_to_mongo()
		get_info_for_missing_report_files()
		update_files_to_mongo()
		letters_available = get_info_for_missing_comment_letters()
		if letters_available:
			update_comment_letters_to_mongo()
		else:
			print "No Comment Letters to add"
		upload_files_to_s3()
		print "Done."
	else:
		print "There are no new reports to add"

def get_max_records():
    page = urllib2.urlopen(base_url + str(1)).read()
    soup = bs(page)
    records = soup.findAll(text=re.compile(r'\bdocuments\swere\s\sretrieved'))[0]
    max_records = [int(s) for s in records.split() if s.isdigit()][0]
    return max_records


def get_eis_links_from_epa():
    data = {
        'date': [],
        'agency': [],
        'state': [],
        'document_type': [],
        'title': [],
        'report_link':[]
    }
    increment = 29
    max_records = get_max_records()
    for page_num in range(1, max_records, increment):
        page = urllib2.urlopen(base_url + str(page_num)).read()
        print "Getting page: " + base_url + str(page_num)
        soup = bs(page)
        table = soup.findAll('tr', attrs={"class":"viewdata"})
        for row in table:
            data['date'].append(row.findAll('td')[0].text)
            data['agency'].append(row.findAll('td')[1].text)
            data['state'].append(row.findAll('td')[2].text)
            data['document_type'].append(row.findAll('td')[3].text)
            data['title'].append(row.findAll('td')[4].text)
            data['report_link'].append(document_base_url + row.findAll('td')[4].find('a')['href'].replace('?opendocument', ''))
    dataFrame = pd.DataFrame(data)
    df_unique = dataFrame.drop_duplicates()
    df_unique.to_csv('eis_links.csv', encoding='utf-8')

def cross_check_with_mongo():
    missing_data = {'date':[], 'title':[], 'report_link':[]}
    with open('eis_links.csv', mode='r') as infile:
        reader = csv.reader(infile)
        for row in reader:
            if row[6] != 'title':
                report = models.Report.objects(title=row[6].strip()).first()
                if not report:
                    missing_data['date'].append(row[2])
                    missing_data['title'].append(row[6].strip())
                    missing_data['report_link'].append(row[4])
    dataFrame = pd.DataFrame(missing_data)
    dataFrame.to_csv('missing_reports_from_mongo.csv', encoding='utf-8')
    if dataFrame.shape[0]:
    	return True
    else:
    	return False

def get_missing_data_for_mongo():
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
    #date = row[1]
    #report_link = row[2]
    #title = row[3]
    with open('missing_reports_from_mongo.csv', mode='r') as infile:
        reader = csv.reader(infile)
        for row in reader:
            if row[3] != 'title':
                print "Getting: " + row[2]
                page = urllib2.urlopen(row[2]).read()
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
                data['title'].append(row[3])
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
    dataFrame.to_csv('reports_to_be_added_to_mongo.csv', encoding='utf-8')

def add_reports_to_mongo():
    data = pd.read_csv('reports_to_be_added_to_mongo.csv')
    data = data.fillna('')
    for row in data.T.iteritems():
        if not models.Report.objects(eis_number=row[1]['eis_number']):
            r = models.Report()
            r['agency'] = [row[1]['agency']]
            r['amended_notice'] = row[1]['amended_notice'].strip()
            r['amended_notice_date'] = row[1]['amended_notice_date'].strip()
            r['comment_due_review_date'] = row[1]['comment_due_review_date'].strip()
            r['comment_letter_date'] = row[1]['comment_letter_date'].strip()
            r['contact_name'] = row[1]['contact_name'].strip()
            r['contact_phone'] = row[1]['contact_phone'].strip()
            r['date_uploaded'] = row[1]['date_uploaded'].strip()
            r['document_type'] = row[1]['document_type'].strip()
            r['eis_number'] = str(row[1]['eis_number'])
            r['federal_register_date'] = row[1]['federal_register_date'].strip()
            r['rating'] = row[1]['rating'].strip()
            r['report_link'] = row[1]['report_link'].strip()
            r['state'] = [row[1]['state']]
            r['supplemental_info'] = row[1]['supplemental_info'].strip()
            r['title'] = row[1]['title'].strip()
            r['website'] = row[1]['website'].strip()
            r.save()

def get_metadata(file_url):
    c = httplib.HTTPConnection('yosemite.epa.gov:80')
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

def format_url_for_s3(file_url, eis, date):
    base_url = 'https://s3.amazonaws.com/epaeis/'
    file_name = urllib2.unquote(file_url.strip().replace("?OpenElement","")).replace('../',document_base_url)
    file_name = file_name[file_name.index('$file')+6:].replace(".pdf","")
    file_name = re.sub(r'([^\s\w])+', '', file_name).replace(" ", "-").lower() + ".pdf"
    file_name = datetime.strptime(date, "%m/%d/%Y").strftime('%m-%d-%Y') + "/" + str(eis) + "/" + file_name
    file_name = base_url + file_name
    return file_name

def get_info_for_missing_report_files():
    files = {
        'eis_number':[],
        'content_length':[],
        'content_type':[],
        'date_retrieved':[],
        'title':[],
        'file_url_epa':[],
        'file_url_s3':[],
        'last_modified':[]
    }
    data = pd.read_csv('reports_to_be_added_to_mongo.csv')
    data = data.fillna('')
    c = httplib.HTTPConnection('yosemite.epa.gov:80')
    for row in data.T.iteritems():
        eis = row[1]['eis_number']
        new_links = row[1]['report_files'].split('||')
        new_titles = row[1]['report_files_titles'].split('||')
        report = models.Report.objects(eis_number=str(eis)).first()
        if report:
            existing_links = [f.file_url_epa for f in report.report_files]
            for i, l in enumerate(new_links):
                link = urllib2.quote(l.strip().replace("?OpenElement","")).replace('../',document_base_url)
                if link and link not in existing_links:
                    try:
                        epa_link = format_url_for_s3(link, eis, row[1]['federal_register_date'])
                        header = get_metadata(link)
                        files['eis_number'].append(str(eis))
                        files['content_length'].append(header['content-length'])
                        files['content_type'].append(header['content-type'])
                        files['date_retrieved'].append(datetime.now())
                        files['title'].append(new_titles[i])
                        files['file_url_epa'].append(link)
                        files['file_url_s3'].append(epa_link)
                        files['last_modified'].append(header['last-modified'])
                    except Exception, e:
                        print "Could not get: " + link
                        print "Because: " + str(e)
        else: 
            print "Cannot find: " + str(eis)
    dataFrame = pd.DataFrame(files)
    dataFrame.to_csv('file_metadata_to_mongo.csv', encoding='utf-8')
    if dataFrame.shape[0]:
        return True
    else:
        return False

def update_files_to_mongo():
    data = pd.read_csv('file_metadata_to_mongo.csv')
    data = data.fillna('')
    for row in data.T.iteritems():
        eis = row[1]['eis_number']
        report = models.Report.objects(eis_number=str(eis)).first()
        if report:
            print "Adding: " + row[1]['file_url_s3']
            rf = models.ReportFile()
            rf.content_length = row[1]['content_length']
            rf.content_type = row[1]['content_type']
            rf.date_retrieved = row[1]['date_retrieved']
            rf.file_url_epa = row[1]['file_url_epa']
            rf.file_url_s3 = row[1]['file_url_s3']
            rf.last_modified = row[1]['last_modified']
            rf.title = row[1]['title']
            rf.converted_to_text = False
            report.report_files.append(rf)
            report.save()

def get_info_for_missing_comment_letters():
    files = {
        'eis_number':[],
        'content_length':[],
        'content_type':[],
        'date_retrieved':[],
        'title':[],
        'file_url_epa':[],
        'file_url_s3':[],
        'last_modified':[]
    }
    data = pd.read_csv('reports_to_be_added_to_mongo.csv')
    data = data.fillna('')
    c = httplib.HTTPConnection('yosemite.epa.gov:80')
    for row in data.T.iteritems():
        eis = row[1]['eis_number']
        new_links = row[1]['comment_letters'].split('||')
        new_titles = row[1]['comment_letters_titles'].split('||')
        report = models.Report.objects(eis_number=str(eis)).first()
        if report:
            existing_links = [f.file_url_epa for f in report.comment_letters]
            for i, l in enumerate(new_links):
                link = urllib2.quote(l.strip().replace("?OpenElement","")).replace('../',document_base_url)
                if link and link not in existing_links:
                    try:
                        epa_link = format_url_for_s3(link, eis, row[1]['federal_register_date'])
                        header = get_metadata(link)
                        files['eis_number'].append(str(eis))
                        files['content_length'].append(header['content-length'])
                        files['content_type'].append(header['content-type'])
                        files['date_retrieved'].append(datetime.now())
                        files['title'].append(new_titles[i])
                        files['file_url_epa'].append(link)
                        files['file_url_s3'].append(epa_link)
                        files['last_modified'].append(header['last-modified'])
                    except Exception, e:
                        print "Could not get: " + link
                        print "Because: " + str(e)
        else: 
            print "Cannot find: " + str(eis)
    dataFrame = pd.DataFrame(files)
    dataFrame.to_csv('comment_letter_metadata_to_mongo.csv', encoding='utf-8')
    if dataFrame.shape[0]:
        return True
    else:
        return False

def update_comment_letters_to_mongo():
    data = pd.read_csv('comment_letter_metadata_to_mongo.csv')
    data = data.fillna('')
    for row in data.T.iteritems():
        eis = row[1]['eis_number']
        report = models.Report.objects(eis_number=str(eis)).first()
        if report:
            print "Adding: " + row[1]['file_url_s3']
            rf = models.CommentLetter()
            rf.content_length = row[1]['content_length']
            rf.content_type = row[1]['content_type']
            rf.date_retrieved = row[1]['date_retrieved']
            rf.file_url_epa = row[1]['file_url_epa']
            rf.file_url_s3 = row[1]['file_url_s3']
            rf.last_modified = row[1]['last_modified']
            rf.title = row[1]['title']
            rf.converted_to_text = False
            report.comment_letters.append(rf)
            report.save()

def upload_files_to_s3():
    k = Key(bucket)
    data = pd.read_csv('reports_to_be_added_to_mongo.csv')
    data = data.fillna('')
    files_to_add = list(set([str(row[1]['eis_number']) for row in data.T.iteritems()]))
    for eis in files_to_add:
        report = models.Report.objects(eis_number=eis).first()
        if report:
            links_s3 = [f.file_url_s3.replace("https://s3.amazonaws.com/epaeis/","") for f in report.report_files]
            links_epa = [f.file_url_epa for f in report.report_files]
            links_s3 = links_s3 + [f.file_url_s3.replace("https://s3.amazonaws.com/epaeis/","") for f in report.comment_letters]
            links_epa = links_epa + [f.file_url_epa for f in report.comment_letters]
            for i, l in enumerate(links_s3):
                if bucket.get_key(links_s3[i]):
                   print "File exists, skipping"
                else:
                    try:
                        print "Saving: " + links_s3[i]
                        f = urllib2.urlopen(links_epa[i])
                        data = f.read()
                        f.close()
                        k = bucket.new_key(links_s3[i])
                        k.set_contents_from_string(data)
                    except Exception, e:
                        print "Could not get because: " + str(e)

main()









