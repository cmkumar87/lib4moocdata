#!/usr/bin/perl -w
use strict;
#use warnings qw(FATAL utf8);
require 5.0;

##
#
# Author : Muthu Kumar C
# Read threads a MySQL database from a Coursera data dump
# Created in Fall, 2015
#
##

use DBI;
use FindBin;
use Getopt::Long;
use Encode;
use HTML::Entities;

my $path;	# Path to binary directory

BEGIN{
	if ($FindBin::Bin =~ /(.*)/) 
	{
		$path  = $1;
	}
}

use lib "$path/../lib";
use Model;
use Preprocess;
### USER customizable section
$0 =~ /([^\/]+)$/; my $progname = $1;
my $outputVersion = "1.0";
### END user customizable section

sub License{
	print STDERR "# Copyright 2015 \251 Muthu Kumar Chandrasekaran <cmkumar087\@gmail.com>\n";
}

sub Help{
	print STDERR "Usage: $progname -h\t[invokes help]\n";
  	print STDERR "       $progname [-q -debug]\n";
	print STDERR "Options:\n";
	print STDERR "\t-q \tQuiet Mode (don't echo license).\n";
	print STDERR "\t-debug \tPrint additional debugging info to the terminal.\n";
}

my $help				= 0;
my $quite				= 0;
my $debug				= 0;
my $dbname				= undef;
$help = 1 unless GetOptions(
				'dbname=s'		=> \$dbname,	#reason, classiccomp, coursera_dump
				'debug'			=> \$debug, 
				'h' 			=> \$help,
				'q' 			=> \$quite
			);

if(!$quite){
	License();
}
			
if ( $help ){
	Help();
	exit(0);
}

my $dbh		= Model::getDBHandle(undef,1,'mysql',$dbname);

if(!defined $dbh){
	print "\n Exception. Db handle is undefined"; die;
}
elsif(!defined $dbname){
	print "\n Exception. dbname is undefined"; die;
}

chdir("$path/..");
print "\n Making directory $dbname"."_pdtbinput \n";
system(" md $dbname"."_pdtbinput ");
chdir("$dbname"."_pdtbinput");
system("cd");

my $forums	= $dbh->selectcol_arrayref("select distinct id from forum_forums");

open( my $log, ">$path/../logs/$progname.err.log") 
		or die "\n Cannot open $path/../logs/$progname.err.log";
foreach my $forum_id ( sort @$forums){
	# we are only interested in threads from lecture, exam, errata 
	# and homework (aka. assignment) threads
	if( $forum_id == 0 || $forum_id == -2 || $forum_id == 10001) {	next;	}
	
	my $threads	= $dbh->selectall_hashref("select * from forum_threads where forum_id = $forum_id", 'id');
	
	if (keys %$threads < 1){ 	
		print $log "\n No threads found in $forum_id. Skipping $forum_id.";
		next;	
	}
	print "\n Processing $forum_id from $dbname \n ";
	system(" md $forum_id");
	chdir("$forum_id");
	system("cd");	
	
	foreach my $thread_id (keys %$threads){
		my $forum_id = $threads->{$thread_id}{'forum_id'};
		my $posts 	 = $dbh->selectall_hashref("select * from forum_posts where thread_id = $thread_id", 'id');

		open (my $FH, ">$thread_id.txt") 
							or die "\n Cannot open file pdtbinput at $path \n $!";	
		foreach my $post_id (sort {$a<=>$b} keys %$posts){
			my $post_text = $posts->{$post_id}{'post_text'};
			$post_text =~ s/\<((br)|(BR))\s*\/>/ /g;
			$post_text =~ s/<.*?>/ /g;
			$post_text =~ s/\n|\r/ /g;
			$post_text = Preprocess::replaceURL($post_text);
			$post_text = Preprocess::replaceTimeReferences($post_text);
			$post_text = Preprocess::replaceMath($post_text);
			$post_text =  decode_entities($post_text);
			print $FH "$post_text\n\n";
			
			my $cmnts =  $dbh->selectall_hashref("select * from forum_comments where thread_id = $thread_id and post_id = $post_id", 'id');
			foreach my $id (sort {$a<=>$b} keys %$cmnts){
				my $cmnt_text = $cmnts->{$id}{'comment_text'};
				$cmnt_text =~ s/\<((br)|(BR))\s*\/>/ /g;
				$cmnt_text =~ s/<.*?>/ /g;			
				$cmnt_text =~ s/\n|\r/ /g;
				$cmnt_text = Preprocess::replaceURL($cmnt_text);
				$cmnt_text = Preprocess::replaceTimeReferences($cmnt_text);
				$cmnt_text = Preprocess::replaceMath($cmnt_text);
				$cmnt_text =  decode_entities($cmnt_text);
				print $FH "$cmnt_text\n\n";
			}
		}
		close $FH;
	}
	chdir("..");
}

close $log;