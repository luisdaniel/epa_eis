EPA Environmental Impact Satement Scrapers
=========

These are the files I used to scrape the [EPA's EIS site](http://yosemite.epa.gov/oeca/webeis.nsf/viEIS01?OpenView). 

A bunch of files, but let me try to make sense of them.

Files
----
_____

**server_scripts Folder**

* **check_for_new_reports.py** - Checks the EPA EIS website for new reports. Gets a list of all linkts to all reports, then crosschecks with the MongoDB to see which reports are missing. Then it will save those reports into the MongoDB, download the PDF files and store them in an Amazon S3 bucket
* **convert_pdfs_to_reports.py** - The script will try to convert into a txt file any PDF on the Amazon S3 bucket which has not been converted.
* **index_missing_files.py** - This script will take all missing reports and index them into ElasticSearch
* **create_full_csv_of_reports.py** - will take all links from the EPA EIS page and scrape all the info (whether it's already stored or not) and store it into a CSV.
* **fresh_reindex.py** - takes the information in the MongoDB and the files in the S# bucket and re-indexes them all on ElasticSearch.


**IPython Notebook Files  **  

* **scrape_links_from_EPA.ipynb** - Get's the title and links from the [main list of reports](http://yosemite.epa.gov/oeca/webeis.nsf/viEIS01?OpenView) (same link as above). These links might expire, but running this script to gather new links takes less than a minute. 
* **scrape_info_from_links.ipynb** - Takes each link scraped in the previous script, and gathers all the info on the [report from the table shown](http://yosemite.epa.gov/oeca/webeis.nsf/EIS01/413CF7C3534AE28F85257D8900216E03?opendocument).  These reports may contain report files and comment letter files.
* **get_file_metadata.ipynb** - From the report file links gathered, this script makes an HTTP request for just the headers (does not open file).
* **get_comment_letter_metadata.ipynb** - Same as above, but with comment letters. 
* **save_files_to_s3.ipynb** - Saves report files to an S3 bucket named 'epaeis'. 
* **save_comment_letters_s3.ipynb** - Saves comment letters to the same S3 bucket.
* **update_documents.ipynb** - This notebookd contains most of the code that created the scripts in the server_scripts folder.

**Python Files**   
Sometimes the scripts make time intensive requests (like saving files to S3) so some scripts were copied into python files and executed from an EC2 instance and left running overnight.  

* **get_file_metadata.py** - Does the same thing as `get_file_metadata.ipynb`. This is quick, no need to run from server, can be run from iPython Notebook. 
* **get_files.py** - This downloads the report PDF's and saves it to an S3 bucket. Takes some hours to complete. 
* **get_comment_letters.py** - The same, but for comment letter PDF's.

**CSV's**

* **eis_links.csv** - The output of `scrape_links_from_EPA.ipynb` Contains: 
	* date, agency,	state, document_type, title, report_link
* **reports.csv** - output of `scrape_info_from_links.ipynb`. Contains: 
	* date,	agency,	state,	document_type,	title,	report_link,	eis_number,	federal_register_date,	contact_name,	comment_due_review_date,	contact_phone,	amended_notice_date,	amended_notice,	supplemental_info,	website,	comment_letter_date,	rating,	num_comment_letter,	comment_letter_links,	num_files,	list_of_links
* **file_metadata.csv** - The output of `get_file_metadata.ipynb` Contains:
	* content_length,	last_modified,	date_retrieved,	content_type,	file_url,	eis_url,	eis_number,
* **comment_letters_metadata.csv** - The output of `get_comment_letter_metadata.ipynb` Contains: 
	* content_length,	last_modified,	date_retrieved,	content_type,	file_url,	eis_url,	eis_number,
* **reports_excel.csv** - modified `reports.csv` in excel to remove a few duplicates entries on the EPA site. The rows were not the same and data from each had to be manually merged into one. There were about 10 duplicates.






