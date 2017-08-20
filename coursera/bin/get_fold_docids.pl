#!/usr/bin/perl -w
use strict;
require 5.0;

##
#
# Author : Muthu Kumar C
# Recreate thread from database
# Created in Mar, 2014
#
##

use DBI;
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

my $data_path = "$path/../data/";

my $help	= 0;
my $quite	= 0;
my $coursecode = undef;
my $in1 = undef;
my $in2 = undef;

sub Help {
	print STDERR "Usage: -h\t[invokes help]\n";
}

$help = 1 unless GetOptions(
				'course=s'	=>	\$coursecode,
				'in1=s'		=>  \$in1,
				'in2=s'		=>  \$in2,
				'h' 		=>	\$help,
				'q' 		=>	\$quite
			);
		
if ( $help ){
	Help();
	exit(0);
}

my $experimentpath = "$path/../experiments/AAAI_17_replication";

my $train_basename	= (split(/_training_/,$in1))[0];
my $test_basename	= (split(/_test_/,$in2))[0];

open (my $out, ">$experimentpath/$coursecode/$test_basename"."_$coursecode"."_folds.txt") 
		or die "cannot open $experimentpath/$coursecode"."_folds.txt";

## testing folds
foreach my $fold (0..4){
	my $filename	= $train_basename."_training_".$fold."_".$coursecode.".txt";
	open (FOLDFILE, "<$experimentpath/$coursecode/$filename")
		or die "cannot open $experimentpath/$coursecode/$filename";
	print $out "training:$fold:";
	while (<FOLDFILE>){
		my @fields = split (/\s+/,$_);
		print $out "$fields[0],";
	}
	print $out "\n";
}

## training folds
foreach my $fold (0..4){
	my $filename	= $test_basename."_test_".$fold."_".$coursecode.".txt";
	open (FOLDFILE, "<$experimentpath/$coursecode/$filename")
		or die "cannot open $experimentpath/$coursecode/$filename";
	print $out "test:$fold:";
	while (<FOLDFILE>){
		my @fields = split (/\s+/,$_);
		print $out "$fields[0],";
	}
	print $out "\n";
}
