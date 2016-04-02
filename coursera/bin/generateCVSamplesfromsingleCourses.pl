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
use utf8::all;
use File::Remove 'remove';

my $path;	# Path to binary directory

BEGIN{
	if ($FindBin::Bin =~ /(.*)/) 
	{
		$path  = $1;
	}
}

use lib "$path/../lib";
use FeatureExtraction;
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
  	print STDERR "       $progname -n -dbname -course [-allf -uni -cutoff -stem
										-tftype	-tprop -forumtype		
										-courseref	-affir		
										-nums -pdtb -agree  -q	]\n";
	print STDERR "Options:\n";
	print STDERR "\t-folds number of cross-validation folds \n";
	print STDERR "\t-dbname database name \n";
	print STDERR "\t-cutoff \t Include only terms that have occured in atlest n documents. \n";
	print STDERR "\t-tftype[bool|pure|log] \n";
	print STDERR "\t-q \t Quiet Mode (don't echo license).\n";
}

my $help				= 0;
my $quite				= 0;
my $debug				= 0;
my $dbname				= undef;
my $courseid			= undef;

my $freqcutoff 			= undef;
my $stem				= 0;
my $tftype				= 'none';
my $term_length_cutoff	= 2;
my $idftype				= 'none';

my $allfeatures 		= 0;
my $numposts			= 0;
my $tprop				= 0;
my $numw				= 0;
my $numsentences 		= 0;

my $forumtype			= 0;
my $agree				= 0;

my $courseref			= 0;
my $nonterm_courseref 	= 0;
my $affirmations 		= 0;
my $pdtb				= 0;
my $unigrams			= 0;

my $oversample			= 0;
my $num_folds			= undef;
my $print_format		= 'none';
my $start_index			= 0;
my $end_index			= undef; # takes the value of #folds if left undefined

my $outfile;
my $tftab;

$help = 1 unless GetOptions(
				'allf'			=>	\$allfeatures,
				'dbname=s'		=>	\$dbname,
				'course=s'		=>	\$courseid,
				'folds=i'		=>	\$num_folds,
				'sindex=i'		=>	\$start_index,	# index to start the cv loop at
				'eindex=i'		=>	\$end_index,	# index to end the cv loop at
				'osample'		=>	\$oversample,
				#FEATURES
				'uni'			=>	\$unigrams,
				'cutoff=i'		=>  \$freqcutoff,
				'lencut=i'		=>	\$term_length_cutoff,
				'tftype=s'		=>	\$tftype,
				'idftype=s'		=>	\$idftype,
				#NON-UNIGRAM FEATURES
				'forumtype'		=> 	\$forumtype,
				'affir'			=>	\$affirmations,
				'courseref'		=>	\$courseref,	
				'nont_course'	=>	\$nonterm_courseref,
				'tprop'			=>	\$tprop,
				'nums'			=>	\$numsentences,
				'pdtb'			=>	\$pdtb,
				'agree'			=>	\$agree,
				'stem'			=>	\$stem,
				#features end here
				'debug'			=>	\$debug,
				'h' 			=>	\$help,
				'q' 			=>	\$quite
			);
		
if ( $help ){
	Help();	
	exit(0);
}

if ( !defined $num_folds){
	print "\n number of foldes is undefined";
	Help();	
	exit(0);
}
	
if (!$quite){
	License();
}

my $courses;
if(defined $courseid){
	push (@$courses, $courseid);
}
else{
	print "\n Exception! courseid not defined. Exiting.";
	Help();
	exit(0);
}

my $error_log_file	= "$path/../logs/$progname"."_$courseid".".err.log";
my $pdtbfilepath	= "$path/../$courseid"."_pdtbinput";
my $log_file_name = "$progname"."_$courseid";
open (my $log ,">$path/../logs/$log_file_name.log")
				or die "cannot open file $path/../logs/$log_file_name.log for writing";


if($allfeatures){
	print $log "\n May include non-unigram features: tftype: $tftype idftype:$idftype\n";
}
elsif($unigrams){
	print $log "\n unigram features only: tftype: $tftype idftype:$idftype\n";
}

if(!defined $dbname){
	print $log "\n Exception: dbname not defined";
	print "\n Exception: dbname not defined"; exit(0);
}

my $db_path		= $path."/../data";
my $dbh 		= Model::getDBHandle($db_path,undef,undef,$dbname);

# Next sample from threads of those n courses
my %docid_to_serialid	= ();
my %instreplied			= ();
my $serial_id			= 0;
my $corpus_type			= undef;
		
my @additive_sequence	= (0,1,3,7,15,31,63,127);
my @ablation_sequence	= (-31,47,55,59,61);
my @individual_features = (2,4,8,16,32);
my @combined			= (0,1,3,7,15,31,63,-31,47,55,59,61,2,4,8,16,32,64);
my @unigrams_only		= (0);
my @uni_plus_forumtype	= (0,1);
my @unigrams_plus		= (63);
my @the_rest			= (3,7,15,31);

my @iterations			= (1,2,4,8,16,3,7,15,31,63);

#sanity check
if(!$allfeatures && scalar @iterations > 1){
	print $log "\n\n Did you forget to switch 'allf' on?";
	Help();
	exit(0);
}

mkdir("$path/../experiments");
my $exp_path 		= "$path/../experiments";
$outfile 			= "../experiments/";

mkdir("$path/../tmp_file");
my $tmp_file 		= "$path/../tmp_file/tmp_samples_$courseid";

# CREATE MULTIPLE TEST AND TRAINING DATASETS FROM THE OVERALL
# LIST OF ALL COURSES

my %datasets				= ();
my %course_samples 			= ();
my %allthreads 				= ();
my %held_out_data_keys		= ();

my $num_courses 			= 1;

my $threadsquery = 	"select docid, courseid, id, 
						inst_replied from thread 
						where courseid = ? 
							and forumid = ?";

my $threadssth = $dbh->prepare($threadsquery) or die "prepare failed \n $!\n";


if(!defined $end_index){
	$end_index = $num_folds;
	print $log "\n assigning end index to $num_folds";
}

my $corpus;	

foreach my $course (@$courses){
	push (@$corpus, $course);
}

#add all threads in the corpus 
foreach my $courseid (@$corpus){
	my $forums = $dbh->selectall_arrayref("select id, forumname from forum
											where courseid =\'$courseid\' 
											and forumname in
											('Homework','Lecture','Exam','Errata')"
										  )
							or die "forum query failed";
	foreach my $forumrow (@$forums){
		my $forumidnumber = $forumrow->[0];
		$threadssth->execute($courseid,$forumidnumber) or die "execute failed \n $!";
		my $threads = $threadssth->fetchall_arrayref() or die "thread query failed";

		foreach my $row ( @$threads ){
			my $courseid = $row->[1];
			my $threadid = $row->[2];
			$course_samples{$courseid}{$forumidnumber}{$threadid} = 1;
		}
	}
}

#sanity checks
if (keys %course_samples == 0 ){
	print "\nException: Specified corpus not found!";
	print $log "\nException: Specified corpus not found!";
	exit(0);
}

if (keys %course_samples != scalar @$corpus ){
	print $log "\nException: Only ". (scalar @$corpus)  ." out of ". (keys %course_samples) ." courses found in the corpus! Checking further...";
	foreach my $courseid (@$corpus){
		if (!exists $course_samples{$courseid} ){
			print $log "\nException: $courseid not found. Pls check courseid and the database";
		}
	}
	exit(0);
}

#add the threads (%allthreads)
foreach my $courseid (@$courses){
	my $forums = $dbh->selectall_arrayref("select id, forumname from forum
											where courseid =\'$courseid\' 
											and forumname in
											('Homework','Lecture','Exam','Errata')"
										  )
							or die "forum query failed";
	foreach my $forumrow (@$forums){
		my $forumidnumber	= $forumrow->[0];
		my $forumname 		= $forumrow->[1];
		$threadssth->execute($courseid,$forumidnumber) or die "execute failed \n $!";
		my $threads = $threadssth->fetchall_arrayref() or die "thread query failed";

		foreach my $row ( @$threads ){
			my $courseid = $row->[1];
			my $threadid = $row->[2];
			
			my $inst_replied	= $row->[3];
			my $docid 			= $row->[0];
			
			#Sanity checks
			if (!defined $docid ){
				print $log "DOCID is null for $courseid \t $threadid \t $forumidnumber \t $inst_replied \n";
				exit(0);
			}
			if (!defined $courseid ){
				die "courseid is null for $docid  \n";
			}					
			if (!defined $threadid ){
				die "threadid is null for $docid \n";
			}
			if (!defined $forumname ){
				die "forumaname is null for $docid \n";
			}
			
			$allthreads{$serial_id} = [$courseid,$threadid,$forumname,$forumidnumber,$serial_id];
			$docid_to_serialid{$serial_id} = $docid;	
			if ($inst_replied) { $instreplied{$serial_id} = 1; } 
			$serial_id ++;
		}
		print $log "\n $courseid \t $forumidnumber\t" . (keys %allthreads) ." threads";
	}
}

if (keys %allthreads == 0 ){
	print "\nException: No threads found!";
	exit(0);
}
if ($num_folds > (keys %allthreads)){
	print "\nException: Num_folds:$num_folds is greater number of courses: " . (keys %allthreads) ." \n";
	Help();
	exit(0);
}

my $max_thread_sample_id = keys %allthreads;

my $fold_size = int($max_thread_sample_id / $num_folds);
print $log "\nNum folds: $num_folds \t Fold size: $fold_size \t \n";

####################*******************#######################
for(my $index = $start_index; $index < $end_index; $index ++){
	my $training_set;
	my $test_set;	

	($training_set, $test_set) = getTrainTestCourseSetsCV($index, $fold_size, \%allthreads);
	print "\n generateCVSamplesfromsingleCourses: test and training for fold $index done";
	
	$datasets{"training$index"} =  $training_set;
	$datasets{"test$index"}  	=  $test_set;
}

print $log "\n train and test datasets identified";

foreach my $type ("test","training"){
	for(my $fold = $start_index; $fold < $end_index; $fold ++){
		print "\n iterating for $fold of $type set";
		foreach my $iter (@iterations){
			my($d0,$d1,$d2,$d3,$d4,$d5,$d6,$d7,$d8,$d9,$d10) = getBin($iter);
			print "\n Iteration $iter begins. Set $d0-$d1-$d2-$d3-$d4-$d5-$d6-$d7-$d8-$d9-$d10";
			
			$outfile = $exp_path . "/";
			
			if($unigrams){
				$outfile .= "uni+";
			}
			
			if($allfeatures){
				$forumtype 			= $d0;
				$affirmations 		= $d1;
				$tprop	 			= $d2;
				$numsentences 		= $d3;
				$nonterm_courseref	= $d4;
				$agree				= $d5;
				$pdtb 				= $d6;
				$courseref			= $d7;
			}
			
			# output file			
			$outfile  .=  $d0 	? "forum+"			: "";
			$outfile  .=  $d1 	? "affir+"  		: "";	
			$outfile  .=  $d2 	? "tprop+"			: "";
			$outfile  .=  $d3 	? "nums+"			: "";
			$outfile  .=  $d4 	? "nont_course+"  	: "";
			$outfile  .=  $d5	? "agree+"			: "";
			$outfile  .=  $d6 	? "pdtb+"	 		: "";
			$outfile  .=  $d7	? "course+" 		: "";
			
			$outfile	.=  "_".$type."_".$fold;
			$outfile 	.= "_". $courses->[0] . ".txt";
			
			#feature name file
			my $feature_file = "features";

			$feature_file .= $d0 	? "+forum"  	: "";
			$feature_file .= $d1 	? "+affir"		: "";
			$feature_file .= $d2 	? "+tprop"		: "";
			$feature_file .= $d3 	? "+nums"  		: "";
			$feature_file .= $d4 	? "+nont_course": "";
			$feature_file .= $d5 	? "+agree"		: "";
			$feature_file .= $d6 	? "+pdtb"	 	: "";
			$feature_file .= $d7	? "+course" 	: "";
						
			$feature_file .= "_". $courses->[0] . ".txt";
			
			my %samples = ();
			my @positivethreads;
			my @negativethreads;
			my %samplecount = ('+1' => 0,
							   '-1' => 0
							  );
	
			my $dataset = $datasets{"$type$fold"};
			
			foreach my $serial_id (@$dataset){
				
				if ( !exists $samples{$serial_id} ) {
						if (!exists $instreplied{$serial_id} ){					
							addtosample(\%allthreads, $serial_id, '-1', \@negativethreads, \%samples);
							$samplecount{'-1'}++;
						}
						else{
							addtosample(\%allthreads, $serial_id, '+1', \@positivethreads, \%samples);
							$samplecount{'+1'}++;
						}
				}
				else{
					print $log "\n Exception: Duplicate keys found in $type set in $fold of @$courses[0] . \n Exiting...";
					print "\n Exception: Duplicate keys found in $type set in $fold of @$courses[0] . \n Exiting...";
					exit(0);
				}
			}

			my %threadcats = ();

			if (@positivethreads){
					$threadcats{1} = {
										'threads' 	=> \@positivethreads,
										'post'		=> 'post2',
										'comment'	=> 'comment2',
										'tftab'		=> 'termFreqC14inst'
									 };
			}
			else{
					$threadcats{1} = {
										'threads' 	=> undef,
										'post'		=> 'post2',
										'comment'	=> 'comment2',
										'tftab'		=> 'termFreqC14inst'
									 };
					print "\n No +ve instances sampled";
			}

			if (@negativethreads){
					$threadcats{2} =  {
										'threads' 	=> \@negativethreads,
										'post'		=> 'post',
										'comment'	=> 'comment',
										'tftab'		=> 'termFreqC14noinst'
									 };
			}
			else{
					$threadcats{2} =  {
										'threads' 	=> undef,
										'post'		=> 'post',
										'comment'	=> 'comment',
										'tftab'		=> 'termFreqC14noinst'
									 };
					print "\n No -ve instances sampled";
			}
			
			print "\n +ve samples:  $samplecount{'+1'} ";
			print "\n -ve samples:  $samplecount{'-1'} ";
			
			
			open (my $FH1, ">$tmp_file") or die "cannot open $tmp_file for writing \n $!";
			open (my $FEXTRACT, ">$error_log_file") or die "cannot open features file$!";
			

			FeatureExtraction::generateTrainingFile(	$FH1, $dbh, \%threadcats, 
														$unigrams, $freqcutoff, $stem, $term_length_cutoff, $tftype, $idftype,
														$tprop, $numw, $numsentences, 
														$courseref, $nonterm_courseref, $affirmations, $agree,
														$numposts, $forumtype, 
														$exp_path, $feature_file,
														\%course_samples, $corpus, $corpus_type, $FEXTRACT, $log,
														$debug, $pdtb, $pdtbfilepath, $print_format
													);
			close $FH1;
			open (my $IN, "<$tmp_file") or die "cannot open $tmp_file file for reading \n $!";
			open (my $OUT, ">$outfile") or die "cannot open feature file: $outfile for writing $!";

			Utility::fixEOLspaces($IN,$OUT);

			close $OUT;
			close $IN;

			print $log "\n +ve samples:  $samplecount{'+1'} ";
			print $log "\n -ve samples:  $samplecount{'-1'} ";
			print $log "\n ##Done $iter";
			
			print "\n +ve samples:  $samplecount{'+1'} ";
			print "\n -ve samples:  $samplecount{'-1'} ";
			print "\n ##Done  $iter";
		}
	}
	print $log "\n ##Done $type";
	print "\n ##Done $type";
}

print "\n ##Done##";
print $log "\n ##Done##";
close $log;
Utility::exit_script($progname,\@ARGV);

sub addtosample{
	my ($allthreads, $serial_id, $label, $threads, $samples) = @_;
	if (!defined $serial_id || !defined $label || !defined $threads || !defined $samples ){
		die "Exception: addtosample: docid or label or thread collection not defined.";
	}
	
	my $docid			= $docid_to_serialid{$serial_id};
	my $courseid		= $allthreads->{$serial_id}->[0];
	my $threadid		= $allthreads->{$serial_id}->[1];
	my $forumname		= $allthreads->{$serial_id}->[2];
	my $forumidnumber	= $allthreads->{$serial_id}->[3];
	
	if (!defined $threadid || !defined $courseid || !defined $docid || !defined $forumname){
		die "Exception: addtosample undef $serial_id \t $courseid \t $threadid \t $docid \t $forumname\n";
	}
	
	push (@$threads, [$threadid,$docid,$courseid,$label,$forumname,$forumidnumber,$serial_id]);
	$samples->{$serial_id} = 1;
}

sub getBin{
	my $given_number = shift;
	if(!defined $given_number){
		die "Exception: getBin Arg decimal_number not defined\n";
	}
	
	my $decimal_number = abs($given_number);
	
	my $d0 = $decimal_number%2;
	$decimal_number = $decimal_number/2;
	
	my $d1 = $decimal_number%2;
	$decimal_number = $decimal_number/2;
	
	my $d2 = $decimal_number%2;
	$decimal_number = $decimal_number/2;
	
	my $d3 = $decimal_number%2;
	$decimal_number = $decimal_number/2;
	
	my $d4 = $decimal_number%2;
	$decimal_number = $decimal_number/2;
	
	my $d5 = $decimal_number%2;
	$decimal_number = $decimal_number/2;
	
	my $d6 = $decimal_number%2;
	$decimal_number = $decimal_number/2;
	
	my $d7 = $decimal_number%2;
	$decimal_number = $decimal_number/2;
	
	my $d8 = $decimal_number%2;
	$decimal_number = $decimal_number/2;
	
	my $d9 = $decimal_number%2;
	$decimal_number = $decimal_number/2;
	
	my $d10 = $decimal_number%2;
	$decimal_number = $decimal_number/2;
	
	if ($given_number < 0 ){
		return (0, $d0,$d1,$d2,$d3,$d4,$d5,$d6,$d7,$d8,$d9);
	}
	else{
		return ($d0,$d1,$d2,$d3,$d4,$d5,$d6,$d7,$d8,$d9,$d10);
	}
}

sub getTrainTestCourseSetsCV{
		my($fold, $fold_size, $data) = @_;
		
		my $num_data_points = keys %$data;
		
		my $testset_start	= $fold * $fold_size;
		my $testset_end		= $testset_start + $fold_size-1;
		$testset_end		= ($testset_end > ($num_data_points) )? ($testset_end% ($num_data_points) ) : $testset_end;
		
		my $trainingset_start	= $testset_end+1;
		$trainingset_start		= ($trainingset_start > ($num_data_points-1) )? ($trainingset_start %($num_data_points)) : $trainingset_start;
		my $trainingset_end		= $testset_start-1;
		$trainingset_end		= ($trainingset_end < 0)? ($num_data_points-1) : $trainingset_end;
		
		print $log  "\n TRAINING SET INDICES: $trainingset_start \t $trainingset_end ";
		print $log  "\n TESTING SET INDICES: $testset_start \t $testset_end";
		
		print $log "\nTRAINING COURSES: ";
		my @trainingset; 
		my @testset;
		if($trainingset_start > $trainingset_end){
			foreach my $j ($trainingset_start..($num_data_points-1),0..$trainingset_end){
				push (@trainingset, $j);
				# print $log "$j: $courses->{$j} \t";
			}
		}
		else{
			foreach my $j ($trainingset_start..$trainingset_end){
				push (@trainingset, $j);
				# print $log "$j: $courses->{$j} \t";
			}	
		}
		
		print $log "\nTESTING COURSES: ";
		if($testset_start > $testset_end){
			foreach my $j ($testset_start..($num_courses-1),0..$testset_end){
				push (@testset, $j);
				# print $log "$j: $courses->{$j} \t";
			}
		}
		else{
			foreach my $j ($testset_start..$testset_end){
				push (@testset, $j);
				# print $log "$j: $courses->{$j} \t";
			}
		}
		
	return(\@trainingset,\@testset);
}