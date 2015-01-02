#PDF Miner
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
from cStringIO import StringIO

#S3
from boto.s3.connection import S3Connection
from boto.s3.key import Key

#pandas
import pandas as pd

#misc
import urllib2
import json
import csv
import re
import os
import subprocess


json_data=open('k.json')
keys = json.load(json_data)
json_data.close()
conn = S3Connection(keys['aws_key'], keys['aws_secret'])
bucket = conn.get_bucket('epaeis')
bucket.list()


columns = ['eis', 'link', 'error']
df_missing_files = pd.DataFrame(columns=columns)


def convert_pdf_to_txt(path):
    rsrcmgr = PDFResourceManager()
    retstr = StringIO()
    codec = 'utf-8'
    laparams = LAParams()
    device = TextConverter(rsrcmgr, retstr, codec=codec, laparams=laparams)
    fp = file(path, 'rb')
    interpreter = PDFPageInterpreter(rsrcmgr, device)
    password = ""
    maxpages = 0
    caching = True
    pagenos=set()
    for page in PDFPage.get_pages(fp, pagenos, maxpages=maxpages, password=password,caching=caching, check_extractable=True):
        interpreter.process_page(page)
    fp.close()
    device.close()
    str = retstr.getvalue()
    retstr.close()
    return str

def url_to_filename(url, date, eis):
    file_name = urllib2.unquote(url)
    file_name = file_name[file_name.index('$file')+6:].replace(".pdf","")
    file_name = re.sub(r'([^\s\w])+', '', file_name).replace(" ", "-").lower() + ".pdf"
    file_name = date.replace("/", "-") + "/" + eis + "/" + file_name
    return file_name

def convert_files():
    index_missing = 0
    with open('reports.csv', mode='r') as infile:
        reader = csv.reader(infile)
        for row in reader:
            if row[21] != 'list_of_links' and row[20] !=0:
                links = row[21].split()
                for link in links:
                    try:
                        file_name = url_to_filename(link, row[1], row[7])
                        print "Processing: " + file_name
                        if bucket.get_key(file_name):
                            key = bucket.get_key(file_name)
                            if not bucket.get_key(key.name.replace(".pdf", ".txt")):
                                res = key.get_contents_to_filename('myfile.pdf')
                                try:
                                	subprocess.check_output(['pdftotext', "myfile.pdf"])
                                except Exception, e:
									print "Could not convert PDF: " + str(e)
									new_missing_row = [row[7], link, str(e)]
									df_missing_files.loc[index_missing] = new_missing_row
									index_missing +=1
                                os.remove('myfile.pdf')
                                new_key = Key(bucket)
                                new_key.key = key.name.replace(".pdf", ".txt")
                                f = open("myfile.txt", 'r')
                                new_key.set_contents_from_file(f)
                                f.close()
                                os.remove("myfile.txt")
                            else:
                                print "Text file already exists"
                        else:
                            print "File not found in S3: " + row[7] + ": " + file_name
                            new_missing_row = [row[7], link, 'Not found in S3']
                            df_missing_files.loc[index_missing] = new_missing_row
                            index_missing +=1
                    except Exception, e:
                        print "Error: " + str(e)
                        new_missing_row = [row[7], link, str(e)]
                        df_missing_files.loc[index_missing] = new_missing_row
                        index_missing +=1

convert_files()

if df_missing_files.shape[0]:
    df_missing_files.to_csv('missing_files_to_save.csv', encoding='utf-8')




