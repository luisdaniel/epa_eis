from bs4 import BeautifulSoup as bs
import re
import urllib2
import csv
import json
import datetime
import pandas as pd
import os
from boto.s3.connection import S3Connection
from boto.s3.key import Key

json_data=open('k.json')
keys = json.load(json_data)
json_data.close()

conn = S3Connection(keys['aws_key'], keys['aws_secret'])
bucket = conn.get_bucket('epaeis')
bucket.list()

columns = ['content_length', 'last_modified', 'date_retrieved', 'content_type', 'file_url', 'eis_url']
df = pd.DataFrame(columns=columns)
columns2 = ['date', 'eis', 'link', 'error']
df_missing = pd.DataFrame(columns=columns2)

index = 0
missing_links = []
index_missing = 0
with open('reports.csv', mode='r') as infile:
    reader = csv.reader(infile)
    for row in reader:
        if row[19] != 'list_of_links':
            links = row[19].split()
            for link in links:
                print "Getting #" + str(index)+ ": " + link
                site = urllib2.urlopen(link)
                meta = site.info()
                content_length = meta.getheaders("Content-Length")[0]
                last_modified = meta.getheaders("Last-Modified")[0]
                date_retrieved = meta.getheaders("Date")[0]
                content_type = meta.getheaders("Content-Type")[0]
                file_url = link
                eis_url = row[6]
                new_row = [content_length, last_modified, date_retrieved, content_type, file_url, eis_url]
                for i in range(len(new_row)):  # For every value in our newrow
                    if hasattr(new_row[i], 'encode'):
                        new_row[i] = new_row[i].encode('utf8')
                df.loc[index] = new_row
                index +=1
                if link:
                    try:
                        k = Key(bucket)
                        file_name = urllib2.unquote(link)
                        file_name = file_name[file_name.index('$file')+6:].replace(".pdf","")
                        file_name = re.sub(r'([^\s\w])+', '', file_name).replace(" ", "-").lower() + ".pdf"
                        file_name = row[1].replace("/", "-") + "/" + row[7] + "/" + file_name
                        print "Getting: " + file_name
                        f = urllib2.urlopen(link)
                        data = f.read()
                        f.close()
                        k = bucket.new_key(file_name)
                        k.set_contents_from_string(data)
                    except Exception, e:
                        print "Could not get because: " + str(e)
                        new_missing_row = [row[1], row[7], link, str(e)]
                        df_missing.loc[index_missing] = new_missing_row
                        index_missing +=1

df['content_length'] = df['content_length'].astype(int)
df.to_csv('file_meta_data.csv', encoding='utf-8')
df_missing.to_csv('missing_files.csv', encoding='utf-8')