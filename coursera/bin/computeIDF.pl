#!/usr/bin/perl -w
use strict;
require 5.0;

##
#
# Author : Muthu Kumar C
# Created in June, 2014
#
##

use DBI;
use FindBin;
use Getopt::Long;
use Lingua::EN::Ngram;
use utf8::all;

my $path;	# Path to binary directory
#set bin directory path
BEGIN{
	if ($FindBin::Bin =~ /(.*)/) 
	{
		$path  = $1;
	}
}

use lib "$path/../lib";
use Preprocess;
use Model;
use Utility;

### USER customizable section
$0 =~ /([^\/]+)$/; my $progname = $1;
my $outputVersion = "1.0";
### END user customizable section

sub License{
	print STDERR "# Copyright 2014 \251 Muthu Kumar C\n";
}

sub Help{
	print STDERR "Usage: $progname -h\t[invokes help]\n";
  	print STDERR "       $progname [-df|-idf] [-q]\n";
	print STDERR "Options:\n";
	print STDERR "\t-df populate termIDF table with terms and df values\n";
	print STDERR "\t-idf update termIDF table entries with idf values\n";
	print STDERR "\t-q \tQuiet Mode (don't echo license).\n";
	print STDERR "\t-debug \tPrint additional debugging info to the terminal.\n";
}

my $dbname	= undef;
my $help	= 0;
my $quite	= 0;
my $course	= undef;
my $idf		= 0;
my $df		= 0;

$help = 1 unless GetOptions(
				'dbname=s'	=> \$dbname,
				'course=s'	=> \$course,
				'df'		=> \$df,	
				'idf'		=> \$idf,
				'h' 		=> \$help,
				'q' 		=> \$quite
			);
		
if ( $help ){
	Help();
	exit(0);
}

if (!$quite){
	License();
}

if(!defined $dbname){
	print "\n Exception: dname not defined"; exit(0);
}

my $datahome	= "$path/../data";
my $dbh 		= Model::getDBHandle($datahome,undef,undef,$dbname);

if(!defined $dbh){
	print "\n Exception dbhandle not defined"; exit(0);
}

my @courses;
if(defined $course){
	push (@courses, $course);
}
else{
	my $coursesquery 		= "select distinct courseid from forum ";
	my $courses_arrayref	= $dbh->selectcol_arrayref($coursesquery) 
									or die "Query failed! $! \n $coursesquery !";
	foreach my $course (@$courses_arrayref){
		push (@courses, $course);
	}
}

my $termDFquery		 = "select termid, term, courseid, sum(df) sumdf from termDF ";
$termDFquery		.=  " where courseid = ? ";
$termDFquery		.=  " group by termid";
my $termDFquerysth	 = $dbh->prepare($termDFquery) or die "df prepare failed $!";

my $inserttermIDFquery 	 = "insert into termIDF (termid,term,df,courseid) ";
$inserttermIDFquery		.= "values(?,?,?,?)";
my $inserttermIDFsth = $dbh->prepare($inserttermIDFquery)
										or die "prepare for insert faield $!";

my $log_file_name = "$progname"."_$course";
if ($df){
	$log_file_name	.= "_df";
}
elsif ($idf){
	$log_file_name	.= "_idf";
}
else{
	print "Exception: Must specify either df or idf option in the commandline";
	Help();
	exit(0);
}

open (my $log ,">$path/../logs/$log_file_name.log")
			or die "cannot open file $path/../logs/$log_file_name.log for writing";

if(@courses eq 0){
	print $log "\n No courses selected. Exiting..."; exit(0);
}

if($df){
	foreach my $courseid (@courses){
		my $dfterms;
		print $log "Executing $termDFquery \n with args:course-$courseid";
		$termDFquerysth->execute($courseid);
		$dfterms = $termDFquerysth->fetchall_arrayref();
		if (scalar @$dfterms == 0) {
			print $log "\n $courseid doesn\'t have terms. 
						Please check if your courseid $courseid is correct.";
		}
		else{
			print $log "\n Found ". scalar(@$dfterms) ." terms in termdf table for $courseid";
		}
		
		#for performance reasons defer commit by tuning off autocommit
		$dbh->{AutoCommit} = 1;
		$dbh->begin_work;
		foreach my $termrow (@$dfterms){
			my $termid		= $termrow->[0];
			my $term 		= $termrow->[1];
			my $courseid	= $termrow->[2];
			my $sumdf		= $termrow->[3];
			print $log "\n Insert termid-$termid \t term-$term \t sumdf-$sumdf course-$courseid";
			$inserttermIDFsth->execute($termid,$term,$sumdf,$courseid) 
								or die "insert failed $! \n $inserttermIDFquery";
		}
		#issue commit manually
		$dbh->commit;
	}
}

if ($idf){
	my $termIDFquery = "select termid, courseid, df from termIDF ";
	if (defined $course){
		$termIDFquery = Model::appendListtoQuery($termIDFquery,\@courses, ' courseid ', ' where ');
	}
	print "\nExecuting... $termIDFquery";
	print $log "\nExecuting... $termIDFquery";
	
	my $terms = $dbh->selectall_arrayref($termIDFquery);

	my $num_threads = $dbh->selectall_hashref("select courseid, sum(numthreads) 
												from forum group by courseid", 'courseid');
	my $updateIDF = $dbh->prepare("Update termIDF set idf = ? where termid = ? and courseid = ?") 
													or die "failed to prepare $!";

	#defer commit by tuning off autocommit
	$dbh->{AutoCommit} = 1;
	$dbh->begin_work;											
	foreach my $termrow (@$terms){
		my $termid = $termrow->[0];
		my $courseid = $termrow->[1];
		my $df = $termrow->[2];
		# log() calculates natural logarithm
		# but we need log base 10
		my $idf = log($num_threads->{$courseid} / $df)/log(10);
		print $log "\n Updating IDF to $idf for $courseid \t $termid";
		$updateIDF->execute($idf,$termid,$courseid);
	}
	#commit
	$dbh->commit;
}

print $log "\n #Done#";
Utility::exit_script($progname,\@ARGV);
print "\n #Done#"