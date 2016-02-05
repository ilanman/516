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
    cur.execute("""  INSERT INTO inproceedings2 (pubkey, booktitle, title, year) \
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

    cur.execute("""  INSERT INTO article2 (pubkey, title, journal, year)
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

            cur.execute("""  INSERT INTO authorship2 (pubkey, authorname)
                             VALUES ('{}', '{}') \
                             ON CONFLICT (pubkey, authorname) DO NOTHING; """.
                        format(dict_version.get('@key'),
                               str(i).replace("'", "").replace(";", "")))

    elif isinstance(author, str) or isinstance(author, unicode):

        author = dict_version.get('author', None)

        cur.execute("""  INSERT INTO authorship2 (pubkey, authorname)
                         VALUES ('{}', '{}') \
                         ON CONFLICT (pubkey, authorname) DO NOTHING; """.
                    format(dict_version.get('@key'),
                           str(author).replace("'", "").replace(";", "")))

    else:
        author = author.items()[1][1]

        cur.execute(""" INSERT INTO authorship2 (pubkey, authorname)
                        VALUES ('{}', '{}') \
                        ON CONFLICT (pubkey, authorname) DO NOTHING; """.
                    format(dict_version.get('@key'),
                           str(author).replace("'", "").replace(";", "")))


def parse_file(xml, tags):
    """
    Parsing function for appropriate tags
    Split data between tags into its own element of a list of tags

    for every tag in the list, parse those tags that correspond to it
    this is only relevant for tags that contain BOTH article and inproceedings
    tag could be either ["article"] or ["inproceedings"] or ["article","inproceedings"]

    """

    chunk = []
    beg, end = 0, 0

    for tag in tags:
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


def get_table_and_tag():
    """
    Get the table and tag from the user
    Return the table to upsert, and tag to parse for
    Table must be either article, inproceedings or authorship
    """

    table = ""

    while table != 'quit':

        filename = raw_input("Please enter XML file to be uploaded: ")

        table = raw_input("Please enter table to upsert (must be one of 'article', 'inproceedings', or 'authorship') or 'quit' to exit: ")

        if table == 'article':
            return filename, ['article'], 'article'
        elif table == 'inproceedings':
            return filename, ['inproceedings'], 'inproceedings'
        elif table == 'authorship':
            return filename, ['article', 'inproceedings'], 'authorship'
        else:
            table = raw_input("Please enter table to upsert (must be one of 'article', 'inproceedings', or 'authorship') or 'quit' to exit: ")

    quit()

if __name__ == "__main__":

    HOSTNAME = 'localhost'
    USERNAME = 'dblpuser'
    PASSWORD = ''
    DATABASE = 'dblp'

    print "Connecting to Postgres..."

    CONNECTION = psycopg2.connect(host=HOSTNAME, user=USERNAME, password=PASSWORD, dbname=DATABASE)

    FILENAME, TAG_LIST, TABLE = get_table_and_tag()

    print "Reading XML file: {} ...".format(FILENAME)
    XML_FILE = read_xml(FILENAME)

    print "Parsing XML file for {} tag ...".format(",".join(TAG_LIST))
    PARSED_FILE = parse_file(XML_FILE, TAG_LIST)

    print "Looping through parsed file and upserting records into {} table".format(TABLE)
    loop_parsed_file(CONNECTION, PARSED_FILE, TABLE)

    CONNECTION.close()
