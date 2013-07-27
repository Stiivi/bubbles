#!/bin/bash
#
# Derive other CSV files from base CSV file
#

BASE=fruits-sk.csv

# Latin2 encoding – to test encodings
iconv --from utf-8 --to latin2 fruits-sk.csv > fruits-sk-latin2.csv

# Remove header
sed 1d fruits-sk.csv > fruits-sk-noheader.csv

