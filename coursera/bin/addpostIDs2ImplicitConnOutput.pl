#!/usr/bin/perl -w
use strict;
#use warnings qw(FATAL utf8);
require 5.0;

##
#
# Author : Muthu Kumar C
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
	print STDERR "\t-dbname \t MySQl database name\n";
	print STDERR "\t-q \tQuiet Mode (don't echo license).\n";
	print STDERR "\t-debug \tPrint additional debugging info to the terminal.\n";
}

my $help				= 0;
my $quite				= 0;
my $debug				= 0;
my $dbname				= undef;
my $courseid			= undef;

$help = 1 unless GetOptions(
				'dbname=s'		=> \$dbname,
				'course=s'		=> \$courseid,
				'debug'			=> \$debug, 
				'h' 			=> \$help,
				'q' 			=> \$quite
			);
			
if ( $help ){
	Help();
	exit(0);
}

my $dbh		= Model::getDBHandle(undef,1,'mysql',$dbname);
# my $dbh 	= Model::getDBHandle("$path/../data",1,undef,$dbname);

chdir("$path/..");
system("mkdir logs");

open( my $log, ">$path/../logs/$progname.log") 
		or die "\n Cannot open $path/../logs/$progname.log";

open( my $skipfilelog, ">$path/../logs/skipped.implicit.err.log") 
		or die "\n Cannot open $path/../logs/skipped.implicit.err.log";
		
if(!defined $dbh){
	print $log "Exception. Db handle is undefined";
	print "Exception. Db handle is undefined"; exit(0);
}

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
	$line	=~ s/^(.*)?\.txt$/$1/;
	$removed_files {$line} = 1;
}
close $rem_fh;

# my $forums	= $dbh->selectcol_arrayref("select id from forum where courseid = \'$courseid\'");
my $forums		= $dbh->selectcol_arrayref("select distinct id from forum_forums");
foreach my $forum_id ( sort @$forums){
	if (!defined $forum_id){
		print $log "Exception. forum_id is undefined for $courseid"; 
		print "Exception. forum_id is undefined for $courseid"; 
	}
	
	# we are only interested in threads from lecture, exam, errata 
	# and homework (aka. assignment) threads
	## to do need to make a forum.ignorelist file and delete the foll line of code
	if( $forum_id == 0 || $forum_id == -2 || $forum_id == 10001 || $forum_id == 4 ) {	next;	}
	
	my $threads		= $dbh->selectall_hashref("select * from forum_threads where forum_id = $forum_id", 'id');
	# my $threads	= $dbh->selectall_hashref("select * from thread where forumid = $forum_id and courseid = \'$courseid\'", 'id');
	print "\n Processing $path/../$courseid"."_pdtbinput/$forum_id";
	print $log "\n Processing $path/../$courseid"."_pdtbinput/$forum_id";
	
	if (keys %$threads < 1){
		print "\n Skipping $path/../$courseid"."_pdtbinput/$forum_id since no threads were found.";
		print $log "\n Skipping $path/../$courseid"."_pdtbinput/$forum_id since no threads were found";
		next;
	}
	
	foreach my $thread_id (keys %$threads){
		#check for removed threads / files
		#these are threads / files omitted due to some unparsable text in them
		#failing in the PDTB parser pipeline
		if (exists $removed_files{$thread_id}){
			next;
		}
	
		my $posts 	 = $dbh->selectall_hashref("select * from forum_posts where thread_id = $thread_id order by post_time", 'id');
		# my $posts  = $dbh->selectall_hashref("select * from post where thread_id = $thread_id and courseid = \'$courseid\' order by post_order", 'id');
		
		my $txt_file_path = "$path/../$courseid"."_pdtbinput/$forum_id";
		my $out_file_path = "$txt_file_path/output";
		
		#if( -e "$out_file_path/$thread_id.txt.exp2.out" ){ next; }
		print $log "\n Doing $thread_id in $forum_id";
		
		open (my $FHOUT, ">$out_file_path/$thread_id.txt.nonexp2.out") 
								or die "\n Cannot open write file pdtbinput at $out_file_path \n $!";	
		
		#fails and skips if file does not exit
		unless( -e "$out_file_path/$thread_id.txt.nonexp.out" ){
			print $skipfilelog "$forum_id/$thread_id \t File not found! Skipping thread.\n";
			close $FHOUT;
			next;
		}
		
		#logs if file is empty
		if( -z "$out_file_path/$thread_id.txt.nonexp.out" ){
			print $skipfilelog "$forum_id/$thread_id \t Empty file found! A corresponding 0kb file will be output.\n";
			#next;
		}
		
		open (my $SENSEFILE, "<$out_file_path/$thread_id.txt.nonexp.out") 
					or die "\n Cannot read file $out_file_path/$thread_id \n $!";
		my $sense_counter  = 0;
		my %senses = ();
		while (my $line = <$SENSEFILE>){
			$senses{$sense_counter} = $line;
			$sense_counter ++;
		}
		close $SENSEFILE;
		
		my $post_spans = getspans($thread_id,$txt_file_path,$out_file_path);
		my %post_ids = ();
		my $post_counter = 1;
		foreach my $post_id (sort {$a<=>$b} keys %$posts){
			$post_ids{$post_counter} = $post_id;
			$post_counter ++;
			
			my $cmnts =  $dbh->selectall_hashref("select * from forum_comments where thread_id = $thread_id and post_id = $post_id order by id", 'id');
			# my $cmnts =  $dbh->selectall_hashref("select * from comment where thread_id = $thread_id and post_id = $post_id and courseid = \'$courseid\' order by id", 'id');
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
		
		# my %connective_label = ();
		# my $conn_counter = 0;
		# open (my $SPANFILE, "<$out_file_path/$thread_id.txt.conn.out") 
								# or die "\n Cannot open read file pdtbinput at $out_file_path \n $!";
								
		# while (my $line = <$SPANFILE>){
			# my @fields = split (/\s+/, $line) ;
			# $connective_label{$conn_counter} = $fields[2];
			# $conn_counter ++;
		# }
		# close $SPANFILE;

		$sense_counter = 0;
		open (my $SPANFILE2, "<$out_file_path/$thread_id.txt.nonexp.res") 
								or die "\n Cannot open read file pdtbinput at $out_file_path \n $!";
				
		while (my $line = <$SPANFILE2>){
			# if($connective_label{$conn_counter} eq 0){ $conn_counter ++; next; }
			my @fields 		= split (/\|/, $line) ;
			
			my $span_string = $fields[22];

			my @spans 		= split (/\.\./, $span_string);
			
			my $search_flag = 0;
			foreach my $post_counter (sort {$a <=> $b} keys %$post_spans){
				my $bol = $post_spans->{$post_counter}{'bol'};
				my $eol = $post_spans->{$post_counter}{'eol'};
				if(!defined $spans[0]){	print $log "\n #BLANK \t $senses{$sense_counter}"; last;}
				if ($spans[0] >= $bol && $spans[1] <= $eol){
					print $FHOUT "$post_ids{$post_counter} \t $senses{$sense_counter}";
					$search_flag = 1;
					last;
				}
			}
			if( $search_flag eq 0 && defined $spans[0]){
				print $log  "\n NOTFOUND \t $senses{$sense_counter}";
			}
			
			$sense_counter++;
		}

		close $SPANFILE2;
		close $FHOUT;
	}
	chdir("..");
}

print $log "\n ##Done##";
close $log;

print "\n ##Done##";

sub getspans{
	my ( $thread_id, $txt_file_path ) = @_;
	my %post_spans = ();
	open ( my $ORIGFILE, "<$txt_file_path/$thread_id.txt" ) 
					or die "\n Cannot read file $txt_file_path/$thread_id.txt $!";
	my $offset 			= 0;
	my $post_counter	= 1;	
	
	my $even_line = 1;
	while ( my $line = <$ORIGFILE> ){
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