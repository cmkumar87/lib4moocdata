#http://strawberryperl.com/lib4moocdata
Library for processing MOOC data dumps.  Currently limited to Coursera data.

Coursera data export
--------------------
Coursera exports data from its MOOCs after compeltion for use by the university that is hosting it on its platform. These data dumps are .sql exports from MySQL databases.
A typical data export consists of the following .sql files

1) <Full_Coursename>(<coursecode>)_SQL_anonymized_forum.sql
2) <Full_Coursename>(<coursecode>)_SQL_hash_mapping.sql
3) <Full_Coursename>(<coursecode>)_SQL_anonymized_general.sql
4) <Full_Coursename>(<coursecode>)_SQL_unanonymizable.sql

Besides, a txt file with clickstream data is also provided. We do not process them yet in this library
5) <coursecode>_clickstream_export.gz

Prerequisites
-------------
To use the library to process and analyse your data you will first need to install the MySQL database and ingest the .sql files into the database.
Command to ingest .sql files using MySQL command line interface (CLI):
mysql\> source \<path to .sql file\>/\<name of the.sql file\>

Note that Coursera supplies a sql export for every course. This means DDL statements across the files from different courses will be redundant. More importatnly there is no field for coursecode in any of the tables. So, you have to either:
i) create a separate MySQL database for each course dump (1 per each course iteration) or
ii) add a 'coursecode' field to every table and issue update statements to populate the coursecode field after running the *.sql import

Installation
------------
The scripts require you to have installed Perl 5 and some dependant perl packages.

<b>For Windows users </b>
Install Strawberyy Perl from here 


