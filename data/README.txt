README.txt

:Author: Ryan Mackenzie White
:Email: ryan.white4@canada.ca
:Date: 2018-03-19 14:28

The datasets in this directory were generated from FEBRL, 
    see the fork on github: https://github.com/ryanmackenziewhite/FEBRL-0.4.2/dsgen/README.txt

        
---- Quoted from FEBRL/dsgen/README.txt
The example data sets in this directory were created using the
'generate.py' database generator and the frequency files given in
this directory. For more information on 'generate.py' please see
the Chapter 'Auxiliary Programs' in the Febrl manual.

1) dataset1.csv

   python generate.py dataset1.csv 500 500 1 1 1 uniform

   This data set contains 1,000 records (500 original and 500
   duplicates), with exactly one duplicate per original record
   and one modification (in one field) per record.

2) dataset2.csv

   python generate.py dataset2.csv 4000 1000 5 2 2 poisson

   This data set contains 5,000 records (4,000 originals and 1,000
   duplicates), with a maximum of five duplicates based on one original
   record (and a poisson distribution of duplicate records), and with
   maximum of two modifications in a field and in the full record.
   Distribution of duplicates:
     10 original records have 5 duplicate records
     37 original records have 4 duplicate records
     97 original records have 3 duplicate records
    182 original records have 2 duplicate records
    147 original records have 1 duplicate record
    527 original records have no duplicate records

3) dataset3.csv

   python generate.py dataset3.csv 2000 3000 5 4 6 zipf

   This data set contains 5,000 records (2,000 originals and 3,000
   duplicates), with a maximum of five duplicates based on one original
   record (and a Zipf distribution of duplicate records), and with
   maximum of four modifications in a single field and a maximum of
   six modifications in the full record.
   Distribution of duplicates:

    178 original records have 5 duplicate records
    184 original records have 4 duplicate records
    188 original records have 3 duplicate records
    233 original records have 2 duplicate records
    344 original records have 1 duplicate record
    873 original records have no duplicate records

4) dataset4a.csv and dataset4b.csv

   python generate.py dataset4.csv 5000 5000 1 2 4 uniform

   Generated as one data set with 10,000 records (5,000 originals
   and 5,000 duplicates, with one duplicate per original), and with
   maximum of two modifications in a single field and a maximum of
   four modifications in the full record. The original records then
   have been split from the duplicate records, into

   dataset4a.csv (containing the 5,000 original records)
   dataset4b.csv (containing the 5,000 duplicate records)

   These two data sets can be used for testing linkage procedures.

   Current dataset4.csv contains both records mixed into single file.
   For validation purposes, obtained dataset4a and dataset4b from
   Python Record Linkage Toolkit until I can properly seperate
   the records.
