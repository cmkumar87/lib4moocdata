# lib4moocdata
Library for processing MOOC data dumps.  Currently limited to Coursera data.

Coursera data export
--------------------
Coursera exports data from its MOOCs after compeltion for use by the university that is hosting it on its platform. These data dumps are .sql exports from MySQL databases.
A typical data export consists of the following .sql files

Prerequisite
------------
To use the library to process and analyse your data you will first need to install a MySQL database and ingest the .sql files into them.
Command to ingest files using MySQL command line interface (CLI):

mysql\> source \<path to .sql file\>/\<name of the.sql file\>


