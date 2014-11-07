from bs4 import BeautifulSoup as bs
import re
import urllib2
import httplib
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

columns = ['date', 'eis', 'link', 'error']
df_missing_files = pd.DataFrame(columns=columns)

def save_files():
    index_missing = 0
    with open('reports.csv', mode='r') as infile:
        reader = csv.reader(infile)
        for row in reader:
            if row[19] != 'comment_letter_links' and row[19] != '':
                print "Getting links for: " + row[5]
                links = row[19].split()
                for link in links:
                    try:
                        k = Key(bucket)
                        file_name = urllib2.unquote(link)
                        file_name = file_name[file_name.index('$file')+6:].replace(".pdf","").replace(".PDF", "")
                        file_name = re.sub(r'([^\s\w])+', '', file_name)
                        file_name = file_name.strip().replace(" ", "-").lower() + ".pdf"
                        file_name = row[1].replace("/", "-") + "/" + row[7] + "/" + file_name
                        print "Saving: " + file_name
                        if bucket.get_key(file_name):
                            print "File exists, skipping"
                        else:
                            f = urllib2.urlopen(link)
                            data = f.read()
                            f.close()
                            k = bucket.new_key(file_name)
                            k.set_contents_from_string(data)
                    except Exception, e:
                        print "Could not get because: " + str(e)
                        new_missing_row = [row[1], row[7], link, str(e)]
                        df_missing_files.loc[index_missing] = new_missing_row
                        index_missing +=1

save_files()

if df_missing_files.shape[0]:
    df_missing_files.to_csv('missing_comment_letters_to_save.csv', encoding='utf-8')