from bs4 import BeautifulSoup as bs
import re
import urllib2
import httplib
import csv
import json
import datetime
import pandas as pd
import os

columns = ['content_length', 'last_modified', 'date_retrieved', 'content_type', 'file_url', 'eis_url', 'eis_number']
df = pd.DataFrame(columns=columns)
columns2 = ['date', 'eis', 'link', 'error']
df_missing_files = pd.DataFrame(columns=columns2)

def get_file_metadata():
    index = 0
    index_missing = 0
    with open('reports.csv', mode='r') as infile:
        reader = csv.reader(infile)
        c = httplib.HTTPConnection('yosemite.epa.gov:80')
        for row in reader:
            if row[21] != 'list_of_links' and row[20] !=0:
                links = row[21].split()
                for link in links:
                    print "Getting #" + str(index)+ ": " + link
                    headers = ''
                    c.request('HEAD', link)
                    try:
                        r = c.getresponse()
                    except Exception, e:
                        print "Error, could not get link: " + str(e)
                        new_missing_row = [row[1], row[7], link, str(e)]
                        df_missing_files.loc[index_missing] = new_missing_row
                        index_missing +=1
                        continue
                    if r.status == 200:
                        headers = r.getheaders()
                    else:
                        print "Error, could not get link: " + str(r.status)
                        new_missing_row = [row[1], row[7], link, str(r.status)]
                        df_missing_files.loc[index_missing] = new_missing_row
                        index_missing +=1
                        continue
                    r.read()
                    #save headers 
                    header_names = ['content-length', 'last-modified', 'date', 'content-type']
                    header_values = {}
                    new_row = []
                    for h in headers:
                        for n in header_names:
                            if n == h[0]:
                                header_values[n] = h[1]
                    for h in header_names:
                        new_row.append(header_values[h])
                    new_row.append(link)
                    new_row.append(row[6])
                    new_row.append(row[7])
                    for i in range(len(new_row)):  # For every value in our newrow
                        if hasattr(new_row[i], 'encode'):
                            new_row[i] = new_row[i].encode('utf8')
                    df.loc[index] = new_row
                    index +=1

get_file_metadata()


df.to_csv('file_metadata_1.csv', encoding='utf-8')
if df_missing_files.shape[0]:
	df_missing_files.to_csv('missing_file_metadata.csv', encoding='utf-8')

