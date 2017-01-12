#read in csv file
#parse csv file
#extend with search option of other txt file when present
#get first column
#add value of column to standard ftp site
#use urllib to retrieve .tar.gz file
#extract .tar.gz file
#use XML_parser.py afterwards

"""Want to make the program multithreaded:
-make directory when needed as output directory
-a file with pmids that are already handled
-one thread wil download the file .
-crawl per ftp_url and call up class for parsing the xml
-convert into classes when possible

"""



import sys
import os
import csv
import urllib.request
import tarfile
from io import BytesIO
import pymysql
import re
import logging
from lxml import etree


def download(tarfile_url, pmid):
    logger = logging.getLogger("main.download")
    logger.info("Downloading article tar.gz file and extract nxml file.")

    ftpstream = urllib.request.urlopen(tarfile_url)
    tmpfile = BytesIO()
    while True:
        s = ftpstream.read(16384)
        if not s:
            break
        tmpfile.write(s)
    ftpstream.close()

    tmpfile.seek(0)

    thetarfile = tarfile.open(fileobj=tmpfile, mode="r:gz")
    get_nxmlfile(thetarfile, pmid)

    thetarfile.close()
    tmpfile.close()


def get_nxmlfile(tar_object, pmid):
    logger = logging.getLogger("main.get_nxmlfile")
    logger.info("Extracting nxml file.")
    for filename in tar_object.getnames():
        if ".nxml" in filename:
            #print(filename)
            saveFile = open("output/"+str(pmid)+".nxml","w")
            f = tar_object.extractfile(filename)
            xml_file = f.read()
            xml_file = xml_file.decode('ascii')
            saveFile.write(xml_file)
            saveFile.close()
            logger.info("Written away file "+pmid)
            logger.info("------------------------")
            #print(xml_file)
            #parse_xml_file(xml_file)
            #print(os.listdir("output"))


def main():
    base = "ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/"
    oa_index_file = "input/oa_file_list.csv"

    #####logger#####
    logger = logging.getLogger("main")
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler("Retrieve_XML.log")
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    logger.info("======================")
    logger.info("Program started")
    ################

    with open(oa_index_file, 'rt') as csvfile:
        csv_file = csv.reader(csvfile, delimiter=',')
        header = csv_file.__next__()
        logger.info("opening CSV file")

        for row in csv_file:
            url = row[0]
            pmid = row[4]
            full_url = str(base + url)
            logger.info(full_url)
            logger.info(pmid)

            ftpstream = base+url
            download(ftpstream, pmid)

    logger.info("DONE")

if __name__ == '__main__':
    main()