#!/bin/sh
# Author : Ilan Man
# Date: 02/22/2016
# CPS516: Parsing XML files
# Notes: 1) remove & characters, 
#		 2) add newline between tags </article><article>, </inproceedings><article>, </proceedings><article>
#		 3) Dump results in DESTINATION

echo "This script will parse a text file and produce an output file."
echo "Enter file to be parsed: "
read ORIGINALFILE
echo "Enter destination file: "
read DESTINATION

sed -e 's/&//g' \
-e 's/<\/article>/&\'$'\n'/g \
-e 's/<\/inproceedings>/&\'$'\n'/g \
-e 's/<\/proceedings>/&\'$'\n'/g \
$ORIGINALFILE > $DESTINATION