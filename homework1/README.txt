Steps to load XML file into relation database:

1) Parse XML file by running "parse_file.sh". This will create a new file, which will be used as an input into the next step.
2) Run python file "db_connection.py" 3 times - once for each table (article, inproceedings, authorship)
  a) For each run, go into the file and change the TABLE variable to "article", or "inproceedings", or "authorship"
  b) For each run, go into the file and change the TAG variable to "article" or "inproceedings", depending on the table you want to upload. In the case of "authorship", you must run the file twice, once with TAG="article" and once with TAG="inproceedings". The second run will append records.