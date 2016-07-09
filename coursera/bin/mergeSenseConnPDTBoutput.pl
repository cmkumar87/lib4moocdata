#!/usr/bin/perl -w
use strict;
#use warnings qw(FATAL utf8);
require 5.0;

##
#
# Author : Muthu Kumar C
# Created in Fall, 2015
# Script to produce *.spans2 file from pdtb output files
# they append the actual connectives to the spans file
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
	print STDERR "Usage: $progname -dbname -course -h\t[invokes help]\n";
  	print STDERR "       $progname [-q -debug]\n";
	print STDERR "Options:\n";
	print STDERR "\t-dbname \t MySQl database name\n";
	print STDERR "\t-course \t Coursera courseid \n";
	print STDERR "\t-q \tQuiet Mode (don't echo license).\n";
	print STDERR "\t-debug \tPrint additional debugging info to the terminal.\n";
}

my $help				= 0;
my $quite				= 0;
my $debug				= 0;
my $dbname				= undef;
my $dbuname				= 'root';
my $dbpass				= '';
my $forumid 			= undef;
my $courseid			= undef;

$help = 1 unless GetOptions(
				'dbname=s'		=> \$dbname,	#reasonpersuasion-001, classiccomp, coursera_dump
				'course=s'		=> \$courseid,
				'dbuname=s'		=> \$dbuname,
				'dbpass=s'		=> \$dbpass,
				'forum=i'		=> \$forumid,
				'debug'			=> \$debug, 
				'h' 			=> \$help,
				'q' 			=> \$quite
			);
			
if ( $help ){
	Help();
	exit(0);
}

my $dbh		= Model::getDBHandle(undef,1,'mysql',$dbname);

if(!defined $dbh){
	print "Exception. Db handle is undefined"; exit(0);
}
elsif(!defined $dbname){
	print "Exception. dbname is undefined"; exit(0);
}

chdir("$path/..");
system("mkdir logs");

open( my $log, ">$path/../logs/$progname.log") 
		or die "\n Cannot open $path/../logs/$progname.log";

if(defined $dbname && !defined $courseid){
	$courseid = $dbname;
}
elsif(defined $courseid && !defined $dbname ){
	$dbname	= $courseid;
}

if(!defined $courseid && !defined $dbname){
	print $log "Exception. courseid is undefined";
	print "Exception. courseid is undefined";
	print $log "Exception. dbname is undefined"; 
	print "Exception. dbname is undefined"; 
	exit(0);
}

chdir("$courseid"."_pdtbinput");
system("cd");

#hashmap of removed files
my %removed_files = ();
open( my $rem_fh, "<$path/../data/Removed_files_$courseid".".txt") 
		or die "\n Cannot open $path/../data/Removed_files_$courseid.txt";
while (my $line = <$rem_fh>){
	chomp $line;
	if ($line =~ /^$/){ next; }
	if ($line =~ /^\s*$/){ next; }
	if ($line =~ /^Folder.*$/){ next; }
	$line	=~ s/^\s*(.*)?\.txt$/$1/;
	$removed_files {$line} = 1;
}
close $rem_fh;

my $forums;
# my $forums	= $dbh->selectcol_arrayref("select distinct id from forum_forums");

if(!defined $forumid){		
	opendir (my $dh, "$path/../$courseid"."_pdtbinput") 
			or die "Cannot open directory $path/../$courseid"."_pdtbinput";
	while(readdir $dh) {
		if ( $_ =~ /^\.+/ || -f $_){
			next;
		}		
		push (@$forums, $_);
	}
	closedir $dh;
}
else{
	push (@$forums, $forumid);
}

if(!defined $forums || scalar @$forums eq 0){
	print "Exception: forums empty. Chck db!";
	exit(0);
}

foreach my $forum_id ( sort @$forums){
	if (!defined $forum_id){
		print $log "Exception. forum_id is undefined for $courseid"; 
		print "Exception. forum_id is undefined for $courseid"; 
	}
	
	# we are only interested in threads from lecture, exam, errata 
	# and homework (aka. assignment) threads
	## to do need to make a forum.ignorelist file and delete the foll line of code
	if( $forum_id == 0 || $forum_id == -2 || $forum_id == 10001 || $forum_id == 4) {	next;	}
	
	# my $threads	= $dbh->selectall_hashref("select * from forum_threads where forum_id = $forum_id", 'id');
	my $threads;
	opendir (my $dh, "$path/../$courseid"."_pdtbinput/$forum_id") 
			or die "Cannot open directory $path/../$courseid"."_pdtbinput/$forum_id";
	print "\n Opening directory $path/../$courseid"."_pdtbinput/$forum_id";
	print $log "\n Opening directory $path/../$courseid"."_pdtbinput/$forum_id";
	while(readdir $dh){
		if ( $_ =~ /^\.+/ || $_ =~ /output/){
			next;
		}
		push (@$threads, $_);
	}
	closedir $dh;
	
	if (!defined $threads || scalar @$threads < 1){ 	next;	}
	
	foreach my $thread_id (@$threads){
		#check for removed threads / files
		#these are threads / files omitted due to some unparsable text in them
		#failing in the PDTB parser pipeline
	
		$thread_id =~ s/^([0-9]+)\.txt$/$1/;

		# if($thread_id eq 115){ print "\n here.. \t $removed_files{$thread_id}"; exit(0)}
		
		if (exists $removed_files{$thread_id}){
			print "\n Skipping file $thread_id.txt";
			next;
		}
		
		my $posts			= $dbh->selectall_hashref("select * from forum_posts where thread_id = $thread_id", 'id');
		my $txt_file_path	= "$path/../$courseid"."_pdtbinput/$forum_id";
		my $out_file_path	= "$txt_file_path/output";
		
		print $log "\n Doing thread-$thread_id in forum-$forum_id";
		
		open (my $SENSEFILE, "<$out_file_path/$thread_id.txt.exp.out") 
						or die "\n Cannot open read file pdtbinput at $out_file_path/$thread_id.txt.exp.out \n $!";
		my $sense_counter  = 0;
		my %senses = ();
		while (my $line = <$SENSEFILE>){
			my @fields				= split (/\s+/, $line) ;
			my $relation			= $fields[4];
			$senses{$sense_counter} = $relation;
			$sense_counter ++;
		}
		close $SENSEFILE;
		
		my $post_spans = getspans($thread_id,$txt_file_path);
		my %post_ids = ();
		my $post_counter = 1;
		foreach my $post_id (sort {$a<=>$b} keys %$posts){
			$post_ids{$post_counter} = $post_id;
			$post_counter ++;
			
			my $cmnts =  $dbh->selectall_hashref("select * from forum_comments where thread_id = $thread_id and post_id = $post_id", 'id');
			foreach my $id (sort {$a<=>$b} keys %$cmnts){
				$post_ids{$post_counter} = $id;
				$post_counter ++;
			}
		}
		if (keys %post_ids ne keys %$post_spans){
			print "\n Exception: post span mismatch: $forum_id \t $thread_id ". (keys %post_ids) . "\t". (keys %$post_spans);
			print $log "\n Exception: post span mismatch: $forum_id \t $thread_id ". (keys %post_ids) . "\t". (keys %$post_spans);
			foreach my $post_counter (sort {$a<=>$b} keys %post_ids){
				print $log "\n $post_counter \t $post_ids{$post_counter} \t $post_spans->{$post_counter}{'bol'} \t $post_spans->{$post_counter}{'eol'}";
			}
			exit(0);			
		}
		
		my %connective_label = ();
		my $conn_counter = 0;
		open (my $CONNFILE, "<$out_file_path/$thread_id.txt.conn.out") 
								or die "\n Cannot open read file $thread_id.txt.conn.out at $out_file_path \n $!";
								
		while (my $line = <$CONNFILE>){
			my @fields = split (/\s+/, $line) ;
			$connective_label{$conn_counter} = $fields[2];
			$conn_counter ++;
		}
		close $CONNFILE;

		$conn_counter = 0;
		$sense_counter = 0;
		open (my $CONNSPANFILE, "<$out_file_path/$thread_id.txt.conn.spans") 
								or die "\n Cannot open read file $thread_id.txt.conn.spans at $out_file_path \n $!";
		
		open (my $FHOUT, ">$out_file_path/$thread_id.txt.conn.spans2") 
								or die "\n Cannot open write file $thread_id.txt.conn.spans2 at $out_file_path \n $!";	
				
		while (my $line = <$CONNSPANFILE>){
			$line =~ s/\n/ /g;
			$line =~ s/\r/ /g;
			$line =~ s/\;/ /g;
			my @fields = split (/\s+/, $line) ;
			
			my $span_string = $fields[0];
			my @spans 		= split (/\.\./, $span_string);
			foreach my $post_counter (sort {$a <=> $b} keys %$post_spans){
				my $bol = $post_spans->{$post_counter}{'bol'};
				my $eol = $post_spans->{$post_counter}{'eol'};
				if($debug){
					print $log "\n bol-$bol \t eol-$eol \t $spans[0] \t $spans[1] \t $post_counter \t $post_ids{$post_counter}";
				}
				if ($spans[0] >= $bol && $spans[1] <= $eol){
					print $FHOUT "$post_ids{$post_counter} \t $span_string";
					last;
				}
			}
			
			print $FHOUT	"\t $fields[2]";
			
			if($connective_label{$conn_counter} eq 1){
				print $FHOUT "\t\t $connective_label{$conn_counter} \t $senses{$sense_counter}";
				$sense_counter++;
			}
			else{
				print $FHOUT "\t\t $connective_label{$conn_counter}";
			}

			print $FHOUT "\n";
			$conn_counter ++;
		}

		close $CONNSPANFILE;
		close $FHOUT;
	}
	chdir("..");
}

close $log;

sub getspans{
	my ($thread_id,$txt_file_path) = @_;
	my %post_spans = ();
	open (my $ORIGFILE, "<$txt_file_path/$thread_id.txt") 
					or die "\n Cannot read file $txt_file_path/$thread_id.txt $!";
	my $offset = 0;
	my $post_counter = 1;
	my $even_line = 1;
	
	while (my $line = <$ORIGFILE>){
		if( !$even_line ){
			$post_counter ++; 
			$even_line = 1;
		}
		else{
			#even line
			my $bol  = $offset;
			my $eol  = $bol + length($line);
			$post_spans{$post_counter} = +{'bol' => $bol,'eol' => $eol};
			$even_line = 0;
		}
		$offset += length($line);
		$offset += 1; # for carriage return \n or \r
	}	
	close $ORIGFILE;
	return \%post_spans;
}