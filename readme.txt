PYHTON ENV LIBRARY DEPENDANCIES:
standard_lib: datetime, time, multiprocessing
conda_lib: scrapy, twisted, boto3, botocore

DIRECTIONS TO RUN:
download the .py file
run from command line with 'python lavamap_books.py' to run scrape process

NOTES:
-hardcoded process for 50 catalogue pages of website
-S3 keys are passed via boto/AWS config file. Script assumes this info is configured for the user. 