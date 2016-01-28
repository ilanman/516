#!/usr/bin/python

""" This file connects to Postgres """
import xmltodict
import psycopg2


def run_query(conn, chunk, table):
    """
    Upsert into postgres tables - article, inproceedings, authorship
    Parse tags using xmltodict() library
    """
    conn.autocommit = True
    cur = conn.cursor()

    try:

        for j in xmltodict.parse(chunk).items():
            dict_version = dict(j[1].items())

            # some titles are formatted as OrderedDict().
            # If so, pick out the "text" element, which is the 2nd (index 1)
            if not isinstance(dict_version['title'], unicode):
                title = dict_version['title'].items()[1][1]
            else:
                title = dict_version.get('title', None)

            if table == 'article':
                insert_article_table(dict_version, title, cur)

            elif table == 'inproceedings':
                insert_inproceedings_table(dict_version, title, cur)

            elif table == 'authorship':
                insert_author_table(dict_version, cur)

    except Exception as error:
        print chunk
        print error
      #  ERROR_COUNT += 1
      #  print "Error count: ", ERROR_COUNT


# helper functions to insert
def insert_inproceedings_table(dict_version, title, cur):
    """
    clean tags and insert into inproceedings
    """

    if not isinstance(dict_version['booktitle'], unicode):
        booktitle = dict_version['booktitle'].items()[1][1]
    else:
        booktitle = dict_version.get('booktitle', None)

    # ON CONFLICT DO NOTHING --> don't insert row
    cur.execute("""  INSERT INTO inproceedings (pubkey, booktitle, title, year) \
                     VALUES ('{}', '{}', '{}', '{}') \
                     ON CONFLICT (pubkey) DO NOTHING; """.
                format(dict_version.get('@key'),
                       str(booktitle).replace("'", "")[:499],
                       str(title).replace("'", "")[:499],
                       dict_version.get('year')))


def insert_article_table(dict_version, title, cur):
    """
    insert into article table
    truncate titles to under 500 characters
    """

    cur.execute("""  INSERT INTO article (pubkey, title, journal, year)
                     VALUES ('{}', '{}', '{}', '{}') \
                     ON CONFLICT (pubkey) DO NOTHING; """.
                format(dict_version.get('@key'),
                       str(title).replace("'", "")[:499],
                       str(dict_version.get('journal')).replace("'", ""),
                       dict_version.get('year')))


def insert_author_table(dict_version, cur):
    """
    upsert into the author table
    need to clean it first because it may be a list or a string or a unicode
    """

    author = dict_version['author']

    if isinstance(author, list):  # if list of authors

        for i in author:

            cur.execute("""  INSERT INTO authorship (pubkey, authorname)
                             VALUES ('{}', '{}') \
                             ON CONFLICT (pubkey, authorname) DO NOTHING; """.
                        format(dict_version.get('@key'),
                               str(i).replace("'", "").replace(";", "")))

    elif isinstance(author, str) or isinstance(author, unicode):

        author = dict_version.get('author', None)

        cur.execute("""  insert into authorship values \
                        ('{}', '{}') ON CONFLICT (pubkey, authorname) DO NOTHING; \
                         """.
                    format(dict_version.get('@key'),
                           str(author).replace("'", "").replace(";", "")))

    else:
        author = author.items()[1][1]

        cur.execute("""  insert into authorship values \
                        ('{}', '{}') ON CONFLICT (pubkey, authorname) DO NOTHING; \
                         """.
                    format(dict_version.get('@key'),
                           str(author).replace("'", "").replace(";", "")))


def parse_file(xml, tag):
    """
    Parsing function for appropriate tags
    Split data between tags into its own element of a list of tags
    """

    chunk = []
    beg, end = 0, 0

    for enum, i in enumerate(xml):

        if '<{}'.format(tag) in i:
            beg = enum

        elif '</{}'.format(tag) in i:
            end = enum + 1
            chunk.append(",".join(xml[beg:end]))

    return chunk


def read_xml(filename):
    """
    function to read xml file and store as list of string
    """

    xml_file = []

    with open(filename) as myfile:
        xml_file.append(myfile.readlines())

    return xml_file[0][2:]


def loop_parsed_file(conn, parsed_file, table):
    """
    for every element (which is a list of tags between <article> and </article>) run insert query
    """

    for num_completed, chunk in enumerate(parsed_file):

        run_query(conn, chunk, table)

        if num_completed % 20000 == 0:
            print "Records uploaded: ", num_completed

    return


if __name__ == "__main__":

    # postgres credentials
    HOSTNAME = 'localhost'
    USERNAME = 'ilanman'
    PASSWORD = 'Charlie1234'
    DATABASE = 'dblp'
    ERROR_COUNT = 0

    print "Connecting to Postgres..."

    CONNECTION = psycopg2.connect(host=HOSTNAME, user=USERNAME, password=PASSWORD, dbname=DATABASE)
    FILENAME = 'dblp-2015-12-01_parsed.xml'
    # "inproceedings" or "article". For author table, need to run twice, once parsing
    # article and once parsing inproceedings. Should come up with better way.

    TAG = 'inproceedings'
    TABLE = 'authorship'  # article or inproceedings or authorship

    print "Read XML file: {}".format(FILENAME)
    XML_FILE = read_xml(FILENAME)
    print "Parse file for {} tag".format(TAG)
    PARSED_FILE = parse_file(XML_FILE, TAG)
    print "Loop through parsed file and run upsert query for table: ", TABLE
    loop_parsed_file(CONNECTION, PARSED_FILE, TABLE)

    CONNECTION.close()
