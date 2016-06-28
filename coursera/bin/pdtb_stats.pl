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
use Preprocess;
### USER customizable section
$0 =~ /([^\/]+)$/; my $progname = $1;
my $outputVersion = "1.0";
### END user customizable section

sub License{
	#print STDERR "# Copyright 2015 \251 Muthu Kumar Chandrasekaran <cmkumar087\@gmail.com>\n";
}

sub Help{
	print STDERR "Usage: $progname -h\t[invokes help]\n";
  	print STDERR "       $progname [-q ]\n";
	print STDERR "Options:\n";
	print STDERR "\t-q \tQuiet Mode (don't echo license).\n";
}

my $help				= 0;
my $quite				= 0;
my $courseid			= undef;
my $dbname				= undef;

$help = 1 unless GetOptions(
				'course=s'		=> \$courseid,
				'h' 			=> \$help,
				'q' 			=> \$quite
			);
			
if ( $help ){
	Help();
	exit(0);
}

open( my $log, ">$path/../logs/$progname.log") 
		or die "\n Cannot open $path/../logs/$progname.log";

if(!defined $courseid){
	print $log "Exception. courseid is undefined";
	print "Exception. courseid is undefined";
	exit(0);
}

my $pdtbfilepath = "$path/../$courseid"."_pdtbinput";

#hashmap of removed files
# my %removed_files = ();
# open( my $rem_fh, "<$path/../data/Removed_files_$courseid".".txt") 
		# or die "\n Cannot open $path/../data/Removed_files_$courseid.txt";
# while (my $line = <$rem_fh>){
	# chomp $line;
	# if ($line =~ /^$/){ next; }
	# if ($line =~ /^\s*$/){ next; }
	# if ($line =~ /^Folder.*$/){ next; }
	# $line	=~ s/^(.*)?\.txt$/$1/;
	# $removed_files {$line} = 1;
# }
# close $rem_fh;

my %pdtbrelation	= ();
my @relations 		= ('expansion','contingency','temporal','comparison');

opendir (my $dh, "$pdtbfilepath")
		or die "can't opendir $pdtbfilepath \n $!";
my @forum_dirs;
while(readdir $dh){
	if ($_ =~ /^\.*$/){	next;	}
	push (@forum_dirs, $_);
}

open(my $pdtbout, ">$path/../results/pdtbrelcount_$courseid.txt")
	or die "\n Cannot open file spans at $path/../results/pdtbrelcount_$courseid.txt \n $!";


foreach my $dir (@forum_dirs){
	opendir (my $dh1, "$pdtbfilepath/$dir/output")
		or die "Can't opendir $pdtbfilepath/$dir/output \n $!";
		
	my @thread_files;
	while(readdir $dh1){
		if ($_ !~ /[0-9]*.txt.exp.out/){	next;	}
		push (@thread_files, $_);
	}
	
	foreach my $file (@thread_files){
		my $threadid = $file;
		my $docid;
		$threadid =~ s/^.*?([0-9]*).*$/$1/;
		$docid = $threadid;
		
		open (my $SENSE_FILE, "<$pdtbfilepath/$dir/output/$file")
				or die "\n Cannot open file spans at $pdtbfilepath/$dir/output/$file \n $!";

		$pdtbrelation{$docid}{'expansion'}		= 0;
		$pdtbrelation{$docid}{'contingency'}	= 0;
		$pdtbrelation{$docid}{'temporal'}		= 0;
		$pdtbrelation{$docid}{'contrast'} 		= 0;
		$pdtbrelation{$docid}{'all'} 			= 0;
		$pdtbrelation{$docid}{'biall'}			= 0;

		#initialization of bi relations to 0
		foreach my $relation1 (@relations){
			foreach my $relation2 (@relations){
				#$pdtbrelation{$docid}{$relation1.$relation2} 		= 0;
				$pdtbrelation{$docid}{$relation1.$relation2."den"}	= 0;
			}
		}

		my $prev_sense = undef;
		while (my $line = <$SENSE_FILE>){
			my @fields = split /\s+/,$line;
			$fields[4] = lc($fields[4]);
			
			if ($fields[4] eq 'expansion'){
				$pdtbrelation{$docid}{'expansion'} ++;
			}
			elsif ($fields[4] eq 'contingency'){
				$pdtbrelation{$docid}{'contingency'} ++;
			}
			elsif ($fields[4] eq 'temporal'){
				$pdtbrelation{$docid}{'temporal'} ++;
			}
			elsif ($fields[4] eq 'comparison'){
				$pdtbrelation{$docid}{'contrast'} ++;
			}
			else{
				die "\n Unknown pdtb relation  $fields[4] of $threadid of forum $dir";
			}
			$pdtbrelation{$docid}{'all'}++;
			
			if(defined $prev_sense){
				$pdtbrelation{$docid}{ $prev_sense.$fields[4]."den"}++;
				$pdtbrelation{$docid}{'biall'}++;
			}
			$prev_sense = $fields[4];
		}
		close $SENSE_FILE;

		# make pdtb densities: birelations
		if($pdtbrelation{$docid}{'biall'} > 0){
			foreach my $relation (sort keys %{$pdtbrelation{$docid}}){
				if(	$relation =~ /den$/){
					$pdtbrelation{$docid}{ $relation} = 
					$pdtbrelation{$docid}{ $relation }/$pdtbrelation{$docid}{'biall'};
				}
			}
		}

		# make pdtb densities: uni relations
		if($pdtbrelation{$docid}{'all'} > 0){
			$pdtbrelation{$docid}{'expden'}		= $pdtbrelation{$docid}{'expansion'}	/$pdtbrelation{$docid}{'all'};
			$pdtbrelation{$docid}{'contden'}	= $pdtbrelation{$docid}{'contingency'} /$pdtbrelation{$docid}{'all'};
			$pdtbrelation{$docid}{'tempden'}	= $pdtbrelation{$docid}{'temporal'}	/$pdtbrelation{$docid}{'all'};
			$pdtbrelation{$docid}{'compden'}	= $pdtbrelation{$docid}{'contrast'}	/$pdtbrelation{$docid}{'all'};	
		}
		else{
			$pdtbrelation{$docid}{'expden'}		= 0;
			$pdtbrelation{$docid}{'contden'}	= 0;
			$pdtbrelation{$docid}{'tempden'}	= 0;
			$pdtbrelation{$docid}{'compden'}	= 0;
		}

		# Analysis
		# print "\n $pdtbrelation{$docid}{'expansion'} \t $pdtbrelation{$docid}{'contingency'} \t $pdtbrelation{$docid}{'temporal'}";
		print $pdtbout "\n $dir \t $threadid\t" . $pdtbrelation{$docid}{'all'}. "\t"
		. $pdtbrelation{$docid}{'expansion'} . "\t" .$pdtbrelation{$docid}{'contingency'} . "\t" 
		. $pdtbrelation{$docid}{'temporal'} . "\t" . $pdtbrelation{$docid}{'contrast'}	. "\t" ;
	}
}