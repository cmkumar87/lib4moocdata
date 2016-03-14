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
  	print STDERR "       $progname [-stem -q -debug]\n";
	print STDERR "Options:\n";
	print STDERR "\t-q \tQuiet Mode (don't echo license).\n";
	print STDERR "\t-debug \tPrint additional debugging info to the terminal.\n";
}

my $dbname	= undef;
my $help	= 0;
my $quite	= 0;
my $debug	= 0;
my $test	= 0;
my $idf;
my $df;
my $corpus;

$help = 1 unless GetOptions(
				'dbname'	=> \$dbname,
				'df'		=> \$df,
				'idf'		=> \$idf,
				'corpus=s'	=> \$corpus,
				'test'		=> \$test,
				'debug'		=> \$debug,
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
	my $courses_arrayref	= $dbh->selectcol_arrayref($forumidsquery) 
									or die "Query failed! $! \n $forumidsquery !";
	@courses				= @$courses_arrayref;
}

my $termDFquery		 = "select termid, term, courseid, sum(df) sumdf from termDF ";
$termDFquery		.=  " where courseid = ? ";
$termDFquery		.=  " group by termid";
my $termDFquerysth	 = $dbh->prepare($termDFquery) or die "df prepare failed $!";

my $inserttermIDF 	 = "insert into termIDF (termid,term,df,courseid) ";
$inserttermIDF 		.= "values(?,?,?,?)";
my $inserttermIDFsth = $dbh->prepare($inserttermIDF)
										or die "prepare for insert faield $!";
										
open (my $log ,">$path/../log/$progname"."_$courseid.log")
			or die "cannot open file $path/../log/$progname"."_$courseid.log for writing";

if(@courses eq 0){
	print $log "\n No courses selected. Exiting..."; exit(0);
}

if($df){
	foreach my $courseid (@courses){
		my $dfterms;
		$termDFquerysth->execute($courseid);
		$dfterms = $termDFquerysth->fetchall_arrayref();
		if (scalar @$dfterms == 0) {
			print $log "$courseid doesn\'t have terms";
		}
		
		#defer commit by tuning off autocommit
		$dbh->{AutoCommit} = 1;
		$dbh->begin_work;
		foreach my $termrow (@$dfterms){
			my $termid		= $termrow->[0];
			my $term 		= $termrow->[1];
			my $courseid	= $termrow->[2];
			my $sumdf		= $termrow->[3];
			$inserttermIDFsth->execute($termid,$term,$sumdf,$courseid) 
											or die "insert failed $!";
		}
		#commit
		$dbh->commit;
	}
}

if ($idf){
	my $termIDFquery = "select termid, courseid, df from termIDF ";
	if (defined $courseid){
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
		print $log "\n Updating IDF for $courseid \t $termid";
		$updateIDF->execute($idf,$termid,$courseid);
	}
	#commit
	$dbh->commit;
}

print $log "\n #Done#"
Utility::exit_script($progname,\@ARGV);
print "\n #Done#"