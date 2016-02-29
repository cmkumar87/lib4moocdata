#!/usr/bin/perl -w
use strict;
require 5.0;

use FindBin;
use Getopt::Long;

my $path;	# Path to binary directory
BEGIN{
	if ($FindBin::Bin =~ /(.*)/) 
	{
		$path  = $1;
	}
}

use lib "$path/../lib";
use Model;

my $help	= 0;
my $quite	= 0;
my $debug	= 0;
my $test	= 0;

my $mode = 'update';
my $dataset;
my $maxid;
my @courses;

$help = 1 unless GetOptions(
				'mode=s'	=>  \$mode,
				'data=s'	=>	\$dataset,
				'maxid=i'	=>	\$maxid,
				'test'		=>	\$test,
				'debug'		=>	\$debug,
				'h' 		=>	\$help,
				'q' 		=>	\$quite
			);
if ( $help ){
	Help();
	exit(0);
}
			
my $dbh = Model::getDBHandle("$path/../dbfiles",1,undef);
if (!defined $maxid){
	my $q = "select max(docid) from thread";
	$maxid = @{$dbh->selectcol_arrayref($q)}[0];
}

if ($test){
	@courses = ('TOYCOURSE');
}
else{
	@courses = ('classicalcomp-001');
}
my $forumq = "select courseid, id from forum ";
$forumq = Model::appendListtoQuery($forumq,\@courses,' courseid ',' where ');
$forumq .= "and forumname not in ('Homework')";
	
my @forums = @{$dbh->selectall_arrayref($forumq)};

my $threadqry = "select id from thread where courseid = ? and forumid =?";
my $sth = $dbh->prepare($threadqry) or die "$threadqry prepare faield $!";

my $uqry = "Update thread set docid = ?  where courseid = ? and forumid = ? and id = ?";
my $usth = $dbh->prepare($uqry) or die "update prepare faield $!";

my $delsth = $dbh->prepare("Update thread set docid = 0 where courseid = ? and forumid = ?");

my $count = $maxid + 1;
foreach (@forums){
	my $courseid = $_->[0];
	my $forumid = $_->[1];	
	print "Selecting threads from $courseid $forumid \n";
	$sth->execute($courseid,$forumid);
	if ($mode eq 'del'){
		$delsth->execute($courseid,$forumid);
		print "resetting docids for $courseid \t $forumid \n";
	}
	else{
		my @threads = @{$sth->fetchall_arrayref()};
		foreach my $threadrow (@threads){
				$usth->execute($count,$courseid,$forumid,$threadrow->[0]);
				$count++;
		}
		print "$count $courseid \t $forumid \n";
	}
}