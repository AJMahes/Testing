import sys
import os
import pymysql
import re
import logging
from lxml import etree

"""
Author: Amrish Mahes

fast iter function from:
http://lxml.de/parsing.html#modifying-the-tree
Based on Liza Daly's fast_iter
http://www.ibm.com/developerworks/xml/library/x-hiperfparse/
See also http://effbot.org/zone/element-iterparse.htm

Script that iterates over each element in the xml file.
coupling the id to the text. Subsequently saved in a table in SQL.
Skipping the ids that do not have access to full text as xml format.

Creates the table pmcid_text with the columns as shown below:

    pmc_id | full_text

Where the pmc_id contains the pmc id/article id of the full text and the column full_text contains the
full text corresponding to the pmc id.


omzetten naar class zodat elk aparte pmid kan geparst worden
1 thread per pmid
"""


def fast_iter(context, func, *args, **kwargs):
    """
    http://lxml.de/parsing.html#modifying-the-tree
    Based on Liza Daly's fast_iter
    http://www.ibm.com/developerworks/xml/library/x-hiperfparse/
    See also http://effbot.org/zone/element-iterparse.htm
    """

    for event, elem in context:
        func(elem, *args, **kwargs)
        # It's safe to call clear() here because no descendants will be
        # accessed
        elem.clear()
        # Also eliminate now-empty references from the root node to elem
        for ancestor in elem.xpath('ancestor-or-self::*'):
            while ancestor.getprevious() is not None:
                del ancestor.getparent()[0]
    del context


def process_element(elem):
    """This function will process the element. For each element it is checked whether it is has a article-id
    or body element. When a article-id is found it will search for a pmc id. This makes it possible
    to find the article back later on. Each id is also checked for in the list of faulty ids.
    These faulty ids is found using the grepper.sh bash script. If found in the faulty ids variable it will skip this
    article containing the pmc id. """

    logger = logging.getLogger("main.process_element")
    global difference
    global teller_id

    for child in elem:
        if child.tag == "article-id":
            try:
                if child.items()[0][1] == "pmc":  # [('pub-id-type', 'pmc')]
                    pmc_id = child.text

                    faulty = check_faulty(pmc_id)

                    if faulty is not True:
                        logger.info("-----Start processing article element------------------")
                        logger.info("processing pmc id:         %s" % str(pmc_id))
                        logger.info("difference from pmc id:    %s" % str(difference))

                        if difference > 0:
                            teller_id += difference+1
                            # +1 because you want the next element of body and not the previous. As first the article id
                            # element is processed and then (+1) for the full text element.
                        else:
                            teller_id += 1

                        difference = 0
                        dict_id[teller_id] = pmc_id
                        logger.info("article id key number: %s" % str(teller_id))
                        logger.info("------------Processed article element------------------")

            except IndexError:
                pass

    if "body" in (elem.tag):

        logger.info("processing body element")
        text = ''.join(elem.itertext())

        global teller_text
        teller_text += 1

        difference = teller_text - teller_id  # assumption is that there are more full text body's then pmc ids

        logger.info("difference %s " % str(difference))
        dict_text[teller_text] = text

        logger.info("body key number : %s " % str(teller_text))
        logger.info('------------processed body element---------------------')


def faulty_ids(file_faulty_ids):
    """Makes a list for article ids that do not have access to full text articles """

    logger = logging.getLogger("main.faulty_ids")
    logger.info("Creating list of faulty ids")

    faulty_ids_list = []
    with open(file_faulty_ids, "r") as f:
        for line in f:
            article_id = re.findall(r'>(.+?)<', line)
            article_id = str(article_id).strip("['']")
            faulty_ids_list.append(article_id)

    return faulty_ids_list


def check_faulty(pmc_id):

    logger = logging.getLogger("main.faulty_ids")

    if pmc_id in faulty_ids_list:
        faulty_ids_list.remove(pmc_id)

        logger.info("pmc id %s is a faulty id and will not be processed." % pmc_id)

        return True


def make_connection(user, password, db):
    """For making a connection with mysql database"""

    connection = pymysql.connect(host='localhost',
                                 user=user,
                                 password=password,
                                 db=db,
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor, autocommit=True)

    logger = logging.getLogger("main.connection")
    logger.info("Made connection with the database.")

    return connection


def create_table_sql(connection, table):
    """SQL code for creating a table for pmc-id and full text"""

    logger = logging.getLogger("main.create_table_sql")
    logger.info("Creating a table in SQL for inserting the pmc id and full text.")

    with connection.cursor() as cursor:
        sql = ("CREATE TABLE IF NOT EXISTS %s (pmc_id INT NOT NULL, full_text LONGTEXT, PRIMARY KEY (pmc_id))" % table)
        cursor.execute(sql)


def column_change_utf(connection, dbname):
    """Making sure all the columns are utf8mnt format """
    # http://dba.stackexchange.com/questions/8239/how-to-easily-convert-utf8-tables-to-utf8mb4-in-mysql-5-5

    logger = logging.getLogger("main.column_change_utf")
    logger.info("Making sure all the columns are utf-8.")

    with connection.cursor() as cursor:
        cursor.execute("ALTER DATABASE " + dbname + " CHARACTER SET 'utf8mb4' COLLATE 'utf8mb4_unicode_ci'")
        cursor.execute(
            "SELECT DISTINCT (table_name) FROM information_schema.columns WHERE table_schema = '%s'" % dbname)
        results = cursor.fetchall()

        for row in results:
            for key, value in row.items():
                cursor.execute("ALTER TABLE `" + value + "` convert to character set DEFAULT COLLATE DEFAULT")


def write_to_sql(pmc_id, full_text, connection, table):
    """Actually writing it into the database."""

    logger = logging.getLogger("main.write_to_sql")
    logger.info("Inserting pmc id %s with corresponding full text in SQL database" % pmc_id)

    with connection.cursor() as cursor:
        sql = "INSERT INTO "+table+" (pmc_id, full_text) VALUES (%s, %s)"
        cursor.execute(sql, (pmc_id, full_text))


def coupler(connection, table):
    """Uses the global variables dict_id and dict_text. Here it searches if the key of dict_id
    can be found in dict_text. When this is possible it will retrieve the article_id and text
    for writing it away into the sql database."""

    logger = logging.getLogger("main.coupler")
    logger.info("Coupling of the article id and the full text for inserting into the SQL table.")

    for key in dict_id.keys():
        if key in dict_text.keys():
            text = dict_text[key]
            article_id = dict_id[key]
            check_text(article_id, text, connection, table)


def check_text(article_id, text, connection, table):
    if "Abstract not submitted for online publication" in text:
        pass
    else:
        write_to_sql(article_id, text, connection, table)


def main():
    file_XML = sys.argv[1]
    file_faulty_ids = sys.argv[2]  # faulty_ids.txt
    SQL_account_file = sys.argv[3]
    table_name = sys.argv[4]
    global faulty_ids_list, teller_id, teller_text, dict_id, dict_text, difference
    difference = 0
    teller_id = 0
    dict_id = {}
    teller_text = 0
    dict_text = {}

    #####logger#####
    logger = logging.getLogger("main")
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler("XML_Parser.log")
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    logger.info("Program started")
    ################

    logger.info("Retrieve user name, password and database name.")
    with open(SQL_account_file) as f:
        lines = f.read().splitlines()
    username = lines[0]
    password = lines[1]
    database = lines[2]

    logger.info("Check whether the file faulty_ids.txt is empty or not.")
    if os.stat(file_faulty_ids).st_size == 0:
        logger.info("faulty_ids.txt is empty! ")
        faulty_ids_list = []
    else:
        logger.info("Started function: faulty_ids")
        faulty_ids_list = faulty_ids(file_faulty_ids)

    connection = make_connection(username, password, database)
    create_table_sql(connection, table_name)
    column_change_utf(connection, database)

    context = etree.iterparse(file_XML, tag=('body', 'article-meta'), events=('end',))

    logger.info("Started function: fast_iter")
    logger.info("Searching for pmc id and full text")
    fast_iter(context, process_element)

    logger.info("Started function: coupler")
    coupler(connection, table_name)

    logger.info("DONE")


if __name__ == '__main__':
    main()
