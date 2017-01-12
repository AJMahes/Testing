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
import threading
from queue import Queue
from general import *
from lxml import etree


class RetrieveXML:

    base = "ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/"
    oa_index_file = "input/oa_file_list2.csv"

    #####logger#####
    logger = logging.getLogger("RetrieveXML")
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler("Retrieve_XML_class.log")
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    logger.info("======================")
    logger.info("Program started")
    ################

    @staticmethod
    def open_XML():
        logger = logging.getLogger("RetrieveXML.open_XML")
        pmid_url= {}

        with open(RetrieveXML.oa_index_file, 'rt') as csvfile:
            csv_file = csv.reader(csvfile, delimiter=',')
            header = csv_file.__next__()
            logger.info("opening CSV file")

            for row in csv_file:
                url = row[0]
                pmid = row[4]
                full_url = str(RetrieveXML.base + url)
                pmid_url[pmid] = full_url
                logger.info(full_url)
                logger.info(pmid)

                ftpstream = full_url
                #RetrieveXML.download(ftpstream, pmid)
        return pmid_url
        logger.info("DONE")

    @staticmethod
    def download(tarfile_url, thread_name):
        logger = logging.getLogger("RetrieveXML.download")
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
        RetrieveXML.get_nxmlfile(thetarfile, thread_name)

        thetarfile.close()
        tmpfile.close()

    @staticmethod
    def get_nxmlfile(tar_object, thread_name):
        logger = logging.getLogger("RetrieveXML.get_nxmlfile")
        logger.info("Extracting nxml file.")
        for filename in tar_object.getnames():
            if ".nxml" in filename:
                print(filename)
                print(thread_name + " now working on " + filename)
                logger.info(thread_name + " now working on " + filename)
                """
                saveFile = open("output/"+str(filename), "w")
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
                """
#create worker threads
def create_workers():
    for _ in range(THREADS):
        t = threading.Thread(target=work)
        t.daemon = True
        t.start()


def work():
    while True:
        url = queue.get()
        if url is None:
            break
        RetrieveXML.download(url, threading.current_thread().name)
        queue.task_done()


#each link is a job
def create_jobs():
    for k, v in pmid_urls_dict.items():
        queue.put(v)
    queue.join()


print("start")
THREADS = 8
queue = Queue
R = RetrieveXML()
pmid_urls_dict = R.open_XML()
print(pmid_urls_dict)
create_jobs(pmid_urls_dict)
create_workers()
create_jobs()
print("done")