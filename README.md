# lib4moocdata
Library for processing MOOC data dumps.  Currently limited to Coursera data.

Coursera data export
--------------------
Coursera exports data from its MOOCs after compeltion for use by the university that is hosting it on its platform. These data dumps are .sql exports from MySQL databases.
A typical data export consists of the following .sql files

Prerequisites
-------------
To use the library to process and analyse your data you will first need to install the MySQL database and ingest the .sql files into the database.
Command to ingest .sql files using MySQL command line interface (CLI):
mysql\> source \<path to .sql file\>/\<name of the.sql file\>

Note that Coursera supplies a sql export for every course. This means DDL statements across the files from different courses will be redundant. More importatnly there is no field for coursecode in any of the tables. So, you have either:
i) create a separate MySQL database for each course dump (1 per each course iteration) or
ii) add a 'coursecode' field to every table and issue update statemnets to populate the coursecode value after running the .sql import


