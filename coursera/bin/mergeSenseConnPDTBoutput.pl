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
				'dbname=s'		=> \$dbname,	#reasonpersuasion-001, classiccomp, coursera_dump
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
chdir("$dbname"."_pdtbinput");
system("cd");

#hashmap of removed files
my %removed_files = ();
open( my $rem_fh, "<$path/../data/Removed_files.txt") 
		or die "\n Cannot open $path/../data/Removed_files.txt";
while (my $line = <$rem_fh>){
	chomp $line;
	if ($line =~ /^$/){ next; }
	if ($line =~ /^\s*$/){ next; }
	if ($line =~ /^Folder.*$/){ next; }
	$line	=~ s/^(.*)?\.txt$/$1/;
	$removed_files {$line} = 1;
}
close $rem_fh;

open( my $log, ">$path/../logs/$progname.log") 
		or die "\n Cannot open $path/../logs/$progname.log";
		
my $forums	= $dbh->selectcol_arrayref("select distinct id from forum_forums");

if(!defined $forums || scalar @$forums eq 0){
	print "Exception: forums empty. Chck db!";
	exit(0);
}

foreach my $forum_id ( sort @$forums){
	# we are only interested in threads from lecture, exam, errata 
	# and homework (aka. assignment) threads
	if( $forum_id == 0 || $forum_id == -2 || $forum_id == 10001) {	next;	}
	my $threads	= $dbh->selectall_hashref("select * from forum_threads where forum_id = $forum_id", 'id');
	
	if (keys %$threads < 1){ 	next;	}
	
	foreach my $thread_id (keys %$threads){
		#check for removed files
		if (exists $removed_files{$thread_id}){
			next;
		}
		
		my $forum_id = $threads->{$thread_id}{'forum_id'};
		my $posts 	 = $dbh->selectall_hashref("select * from forum_posts where thread_id = $thread_id", 'id');
		
		my $txt_file_path = "$path/../$dbname"."_pdtbinput/$forum_id";
		my $out_file_path = "$txt_file_path/output";
		
		print $log "\n Doing thread-$thread_id in forum-$forum_id";
		
		open (my $SENSEFILE, "<$out_file_path/$thread_id.txt.exp.out") 
								or die "\n Cannot open read file pdtbinput at $out_file_path \n $!";
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
					or die "\n Cannot open read file $txt_file_path/$thread_id.txt $!";
	my $offset = 0;
	my $post_counter = 1;
	my $odd_line = 1;
	
	while (my $line = <$ORIGFILE>){
		if( !$odd_line ){
			$post_counter ++; 
			$odd_line = 1;
		}
		else{
			my $bol  = $offset;
			my $eol  = $bol + length($line);
			$post_spans{$post_counter} = +{'bol' => $bol,'eol' => $eol};
			$odd_line = 0;
		}
		$offset += length($line);
	}	
	close $ORIGFILE;
	return \%post_spans;
}

