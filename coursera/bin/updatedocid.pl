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

### USER customizable section
$0 =~ /([^\/]+)$/; my $progname = $1;
my $outputVersion = "1.0";
### END user customizable section

sub License{
	print STDERR "# Copyright 2014 \251 Muthu Kumar C\n";
}

sub Help{
	print STDERR "Usage: $progname -h\t[invokes help]\n";
  	print STDERR "       $progname [-mode -course -maxid -q ]\n";
	print STDERR "Options:\n";
	print STDERR "\t-q \tQuiet Mode (don't echo license).\n";
}

my $help	= 0;
my $quite	= 0;

my $mode	= 'update';
my $dataset;
my $maxid;
my $courseid;
my @courses;

$help = 1 unless GetOptions(
				'mode=s'	=>  \$mode,
				'course=s'	=>	\$courseid,
				'maxid=i'	=>	\$maxid,
				'h' 		=>	\$help,
				'q' 		=>	\$quite
			);
if ( $help ){
	Help();
	exit(0);
}
			
my $dbh = Model::getDBHandle($db_path,1,undef,$dbname);

if (!defined $maxid){
	my $q = "select max(docid) from thread";
	$maxid = @{$dbh->selectcol_arrayref($q)}[0];
}

open (my $log ,">$path/../log/$progname.log")
						or die "cannot open file $path/../log/$progname.log for writing";

my $forum_query = "select courseid, id from forum ";
if(defined $courseid){
	push(@courses,$courseid);
	$forum_query = Model::appendListtoQuery($forum_query,\@courses,' courseid ',' where ');
}
my @forums	= @{$dbh->selectall_arrayref($forum_query)};

if ( !defined @forums || scalar @forums eq 0 ){
	print "Exception: Failed to get data from forum table. Check db!";
	print $log "Exception: Failed to get data from forum table. Check db!";
}

my $threadqry	= "select id from thread where courseid = ? and forumid = ?";
my $sth			= $dbh->prepare($threadqry) or die "Prepare faield $! \n $threadqry";

my $uqry	= "Update thread set docid = ?  where courseid = ? and forumid = ? and id = ?";
my $usth	= $dbh->prepare($uqry) or die "Update prepare faield $! \n $uqry";

my $delqry	= "Update thread set docid = 0 where courseid = ? and forumid = ?";
my $delsth	= $dbh->prepare("$delqry");

my $count	= $maxid + 1;
foreach (@forums){
	my $courseid	= $_->[0];
	my $forumid		= $_->[1];	
	print $log "Selecting threads from course-$courseid forum-$forumid \n";
	$sth->execute($courseid,$forumid);
	if ($mode eq 'del'){
		$delsth->execute($courseid,$forumid);
		print $log "resetting docids for course-$courseid \t forum-$forumid \n";
	}
	else{
		my @threads = @{$sth->fetchall_arrayref()};
		foreach my $threadrow (@threads){
			$usth->execute($count,$courseid,$forumid,$threadrow->[0]);
			$count++;
		}
		print $log "$count $courseid \t $forumid \n";
	}
}

print $log "##Done##";
print "##Done##";