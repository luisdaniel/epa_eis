#!/usr/bin/env python
# -*- coding: utf8 -*- 
import os, sys
from mongoengine import *
from datetime import datetime

class Report(Document):
	agency = ListField(StringField(max_length=400)) #lead agency listed, can be "MULTI"
	amended_notice = StringField(max_length=1000) #Ammended notice. 
	amended_notice_date = DateTimeField() #Amended Notice Date
	comment_due_review_date = DateTimeField() #Comment or Review Due Date
	comment_letter_date = DateTimeField() #Date of comment letter
	comment_letters = ListField(EmbeddedDocumentField('CommentLetter')) #comment letters
	contact_name = StringField(max_length=100) #Contact name
	contact_phone = StringField(max_length=35) #Contact Phone number xxx-xxx-xxxx
	date_uploaded = DateTimeField() #date uploaded to EPA EIS website, usually every 5 days
	document_type = StringField(max_length=40) #Draft, Final, etc.
	eis_number = StringField(max_length=10) #not reliably unique on site, but should be once dupes removed
	federal_register_date = DateTimeField() #Federal Register Date
	rating = StringField(max_length=10) #Dunno what this is, but it's short
	report_files = ListField(EmbeddedDocumentField('ReportFile')) #Actual Report PDF
	report_link = StringField(max_length=100) #URL to EPA's report page. These seem to change
	search_keywords = ListField(StringField(max_length=30)) #keywords for searching. Rough search for now
	state = ListField(StringField(max_length=2)) #state, when multiple, input as "00"
	supplemental_info = StringField(max_length=1000) #Supplemental Info
	title = StringField(max_length=600) #Title of report
	website = StringField() #website


class CommentLetter(EmbeddedDocument):
	content_length = IntField() #Size of the document
	content_type = StringField() #according to HTTP header, should be PDF
	date_retrieved = DateTimeField() #Date I retrieved this from EPA
	file_url_epa = StringField() #link to file on EPA's site
	file_url_s3 = StringField() #link to file on S3 bucket
	last_modified = DateTimeField() #last modified, according to HTTP header
	converted_to_text = BooleanField()

class ReportFile(EmbeddedDocument):
	content_length = IntField() #Size of the document
	content_type = StringField() #according to HTTP header, should be PDF
	date_retrieved = DateTimeField() #Date I retrieved this from EPA
	file_url_epa = StringField() #link to file on EPA's site
	file_url_s3 = StringField() #link to file on S3 bucket
	last_modified = DateTimeField() #last modified, according to HTTP header
	title = StringField() #title of report file
	converted_to_text = BooleanField()


#Will try to store PDF's as their own documents. 
class EIS_File(Document):
	content_length = IntField() #Size of the document
	content_type = StringField() #according to HTTP header, should be PDF
	date_retrieved = DateTimeField() #Date I retrieved this from EPA
	file_url_epa = StringField() #link to file on EPA's site
	file_url_s3 = StringField() #link to file on S3 bucket
	last_modified = DateTimeField() #last modified, according to HTTP header
	file_title = StringField() #title of report file
	eis_number = StringField()
	report_title = StringField()


#------------------------------------------NOTES------------------------------
#Sometimes you have multiple agencies and multiple states invovled.
#For now multi states and agencies are listed as "00" or "Multi" respectively
#But the possibility is left open for appending agencies and states to the 
#field.

#There is a handful of repeated reports. Can be found with same title and EIS #
#No conflicting info, so can be merged. 
