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
use DateTime::Format::Epoch;

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
	#print STDERR "# Copyright 2014 \251 Muthu Kumar C\n";
}

sub Help{
	print STDERR "Usage: $progname -h\t[invokes help]\n";
  	print STDERR "       $progname -n [-test -mode -cutoff -stem -tlen -time 
										-tftype	-cutoff	-forumtype	-qpost	
										-numq	-quote	-prof	-course	-hedge	
										-affir	-senti	-stem		
										-tlen	-plen	-nums	-diffw	-q	-debug]\n";
	print STDERR "Options:\n";
	print STDERR "\t-n	\t Number of instances to sample. \n";
	print STDERR "\t-mode \t all: sample  +ve and -ve. \n\t\t ne: sample -ve. \n\t\t po: sample +ve. \n";
	print STDERR "\t-diff \t Weighs terms occurring in earlier posts higher. \n";
	print STDERR "\t-cutoff \t Include only terms that have occured in atlest n documents. \n";
	print STDERR "\t-tftype[bool|pure|log] \n";
	print STDERR "\t-q \t Quiet Mode (don't echo license).\n";
}

my $help				= 0;
my $quite				= 0;
my $debug				= 0;
my $dbname				= undef;
my $corpus_name			= undef;

my $samplemode 			= 'all';
my $freqcutoff 			= undef;
my $stem				= 0;
my $tftype				= 'none';
my $term_length_cutoff	= 2;
my $idftype				= 'none';

my $numposts			= 0;
my $forumtype			= 0;
my $affirmations		= 0;
my $nonterm_courseref 	= 0;
my $tprop				= 0;
my $numw				= 0;
my $numsentences 		= 0;
my $pdtb 				= 0;
my $pdtb_imp			= 0;
my $pdtb_exp			= 0;

my $agree				= 0;

my $courseref			= 0;
my $unigrams			= 0;
my $bigrams				= 0;

my $allfeatures 		= 0;

my $oversample			= 0;
my $num_folds			= undef;
my $hold_out_course		= 0;
my $votes				= 0;
my $print_format		= 'none';
my $start_index			= 0;
my $end_index			= undef; # takes the value of #folds if left undefined
my $intervention_delay  = 0;

my $outfile;
my $tftab;

$help = 1 unless GetOptions(
				'corpus=s'		=> 	\$corpus_name,
				'dbname=s'		=>	\$dbname,
				'folds=i'		=>	\$num_folds,
				'holdc'			=>	\$hold_out_course,
				'sindex=i'		=>	\$start_index,	# index to start the cv loop at
				'eindex=i'		=>	\$end_index,	# index to end the cv loop at
				'osample'		=>	\$oversample,
				'mode=s'		=>	\$samplemode,
				#FEATURES
				'allf'			=>	\$allfeatures,
				'uni'			=>	\$unigrams,
				'cutoff=i'		=>  \$freqcutoff,
				'lencut=i'		=>	\$term_length_cutoff,
				'tftype=s'		=>	\$tftype,
				'idftype=s'		=>	\$idftype,
				'bi'			=>	\$bigrams,
				#NON-UNIGRAM FEATURES
				'forumtype'		=> 	\$forumtype,			
				'courseref'		=>	\$courseref,	
				'nont_course'	=>	\$nonterm_courseref,
				'tprop'			=>	\$tprop,
				'nums'			=>	\$numsentences,
				'pdtb'			=>	\$pdtb,
				'pdtbexp'		=>	\$pdtb_imp,
				'pdtbimp'		=>	\$pdtb_exp,
				'affir'			=>	\$affirmations,
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

if ( !defined $num_folds || !defined $corpus_name){
	print "\n num_folds or corpus_name undefined.";
	Help();
	exit(0);
}

if (!$quite){
	License();
}

my $error_log_file	= "$path/../logs/$progname"."_$corpus_name".".err.log";
my $log_file_name 	= "$progname"."_$corpus_name";
open (my $log ,">$path/../logs/$log_file_name.log")
				or die "cannot open file $path/../logs/$log_file_name.log for writing";
				
if($allfeatures){
	print "May include non-unigram features: tftype: $tftype idftype:$idftype\n";
	print $log "May include non-unigram features: tftype: $tftype idftype:$idftype\n";
}
elsif($unigrams){
	print "unigram features only: tftype: $tftype idftype:$idftype\n";
	print $log "unigram features only: tftype: $tftype idftype:$idftype\n";
}
				
mkdir("$path/../experiments");
my $exp_path 		= "$path/../experiments/";

mkdir("$path/../tmp_file");
my $tmp_file 		= "$path/../tmp_file/tmp_samples_$corpus_name";
my $pdtbfilepath	= "$path/..";
$outfile 			= "../experiments/";

if(!defined $dbname){
	print $log "\n Exception: dbname not defined";
	print "\n Exception: dbname not defined"; exit(0);
}

my $db_path		= "$path/../data";
my $dbh 		= Model::getDBHandle($db_path,undef,undef,$dbname);

my $dt = DateTime->new( year => 1970, month => 1, day => 1 );
my $dateformatter = DateTime::Format::Epoch->new(
				  epoch					=> $dt,
				  unit					=> 'seconds',
				  type					=> 'int',    # or 'float', 'bigint'
				  #skip_leap_seconds	=> 1,
				  start_at				=> 0,
				  local_epoch			=> undef,
			  );

# sample n courses
# my $max_course_sample_id = scalar @courses_master_list;

# Next sample from threads of those n courses
my %docid_to_serialid	= ();
my %instreplied			= ();
my $corpus_type;
my @courses_master_list;

if ($corpus_name eq 'd61'){
	@courses_master_list	= (	
								'acoustics1-001', 
								'advancedchemistry-001',
								'amnhearth-002',
								'analyze-001',
								'androidapps101-001',
								'automata-002',
								'ccss-math1-002',
								'compmethods-003',
								'cosmo-003',
								'crypto-010',
								'diabetes-001',
								'dynamicalmodeling-001',
								'dynamics1-001',
								'edc-002',
								'exdata-002',
								'friendsmoneybytes-004',
								'functionalanalysis-001',
								'gamification-003',
								'ggp-002',
								'globalwarming-002',
								'howthingswork1-002',
								'improvisation-005',
								'informationtheory-001',
								'innovativeideas-009',
								'intrologic-003',
								'maps-002',
								'marriageandmovies-001',
								'modernmiddleeast-001',
								'nanotech-001',
								'netsysbio-001',
								'networksonline-001',
								'neuralnets-2012-001',
								'newnordicdiet-002',
								'nlangp-001',
								'optimization-002',
								'organalysis-003',
								'pgm-003',
								'pkubioinfo-002',
								'reactive-001',
								'repdata-002',
								'sciwrite-2012-001',
								'sna-2012-001',
								'solarsystem-001',
								'statinference-002',
								'virtualassessment-001',
								'warhol-001',
								'matrix-001',
								'ml-005',
								'rprog-003',		
								'calc1-003',
								'smac-001',		
								'compilers-004',			
								'maththink-004',
								'bioelectricity-002',
								'gametheory2-001',
								'musicproduction-006',
								'medicalneuro-002',
								'comparch-002',
								'biostats-005',			
								'bioinfomethods1-001',
								'casebasedbiostat-002'
						);
}
elsif($corpus_name eq 'd14'){
		@courses_master_list = (		
							'ml-005',
							'rprog-003',		
							'calc1-003',
							'smac-001',		
							'compilers-004',			
							'maththink-004',
							'bioelectricity-002',
							'musicproduction-006',
							'medicalneuro-002',
							'comparch-002',
							'gametheory2-001',
							'biostats-005',			
							'bioinfomethods1-001',
							'casebasedbiostat-002'
							);
}
elsif($corpus_name eq 'nus'){
		@courses_master_list = (  'classicalcomp-001'
								 ,'classicalcomp-002'
								 ,'reasonandpersuasion-001'
								 ,'reasonandpersuasion-002'
								)
}
elsif($corpus_name eq 'pitt'){
		@courses_master_list = ( 'accountabletalk-001',
								  'clinicalterminology-001',
								  'clinicalterminology-002',
								  'disasterprep-001',
								  'disasterprep-002',
								  'disasterprep-003',
								  'nuclearscience-001',
								  'nuclearscience-002',
								  'nutritionforhealth-001',
								  'nutritionforhealth-002'
							    )
}
else{
	print "Exception. Unknown $corpus_name. Please enter a valid corpus name (pitt|nus|d14|d61)";
	Help();
	exit(0);
}

my @trainingcourses;
my @testingcourses;

if( $hold_out_course && $corpus_name eq 'd61'){
	push (@trainingcourses, @courses_master_list[0..46]);
	push (@testingcourses, @courses_master_list[47..$#courses_master_list]);
}
else{
	push (@testingcourses, @courses_master_list[0..$#courses_master_list]);
}

my @additive_sequence	= (0,1,3,7,15,31,63,127);
my @ablation_sequence	= (-31,47,55,59,61);
my @individual_features = (2,4,8,16,32,64);
my @combined			= (0,1,3,7,15,31,63,-31,47,55,59,61,2,4,8,16,32,64);
my @unigrams_only		= (0);
my @uni_plus_forumtype	= (0,1);
my @unigrams_plus		= (63);
my @the_rest			= (3,7,15,31);

my @edm 				= (31);
my @proposed			= (32, 64, 63, 95, 127);
my @pdtb_feature		= (64);
#my @iterations			= (0, 31, 32, 64, 63, 95, 127);
my @iterations			= (223, 159, 95, 31, 64);

#sanity check
if(!$allfeatures && scalar @iterations > 1){
	print "\n\n Did you forget to switch 'allf' on?";
	Help();
	exit(0);
}

# CREATE MULTIPLE TEST AND TRAINING DATASETS FROM THE OVERALL
# LIST OF COURSES

my %datasets	= ();
my $num_courses = scalar @courses_master_list;

if ($num_folds > $num_courses){
	print "\nException: Num_folds:$num_folds is greater number of courses: $num_courses\n";
	Help();
	exit(0);
}
my $fold_size = int($num_courses / $num_folds);
print "\nNum folds: $num_folds \t Fold size: $fold_size \t \n";
print $log "\nNum folds: $num_folds \t Fold size: $fold_size \t \n";

if(!defined $end_index){
	$end_index = $num_folds;
	print "\n assigning end index to $num_folds";
	print $log "\n assigning end index to $num_folds";
}

for(my $index = $start_index; $index < $end_index; $index ++){

	my ($training_set, $test_set) = getTrainTestCourseSetsHO($index, \@trainingcourses, \@testingcourses);
	
	$datasets{"test$index"} =  $test_set;
	$datasets{"train$index"} = $training_set;
	print "\n@$test_set";
	print "\n@$training_set";
}

print "\n train and test datasets identified";

my $threadsquery = 	"select docid, courseid, id, 
						inst_replied from thread 
						where courseid = ? 
							and forumid = ?";

my $threadssth = $dbh->prepare($threadsquery) 
					or die "prepare failed \n $!\n";

#hashmap of removed file
my $removed_files;

foreach my $type ("train","test"){
	for(my $fold = $start_index; $fold < $end_index; $fold ++){
		my $courses = $datasets{"$type$fold"};
		my $corpus	= $datasets{"train$fold"};
		$tmp_file 	= "$path/../tmp_file/tmp_samples_$corpus_name"."_$type"."_$fold";		
		
		print "\nsampling $type instances";
		if ($type eq "test"){
			foreach my $course (@$courses){
				push (@$corpus, $course);
			}
		}
		
		$removed_files = readRemovedFiles($courses);
		if(keys %{$removed_files} eq 0){
			print "\n Warning: no removed files found";
		}
		
		my %course_samples 		= ();
		my %allthreads 			= ();
		my %held_out_data_keys	= ();
		my $serial_id			= 0;
		print "\n$type \t $fold ";
		
		#add all threads in the corpus 
		foreach my $courseid (@$corpus){
			my $forums = $dbh->selectall_arrayref("select id, forumname from forum
													where courseid =\'$courseid\' 
													and forumname in ('Homework','Lecture','Exam','Errata')"
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
			die "\nException: Specified corpus not found!";
		}
		
		if (keys %course_samples != scalar @$corpus ){
			warn "\nException: Only ". (scalar @$corpus)  ." out of ". (keys %course_samples) ." courses found in the corpus! Checking further...";
			foreach my $courseid (@$corpus){
				if (!exists $course_samples{$courseid} ){
					warn "\nException: $courseid not found. Pls check courseid and the database";
				}
			}
			die;
		}
		#sanity checks end
		
		#add the threads (%allthreads)
		foreach my $courseid (@$courses){
			my $forums = $dbh->selectall_arrayref("select id, forumname from forum
													where courseid =\'$courseid\' 
													and forumname in
													('Homework','Lecture','Exam','Errata')"
												  )
									or die "forum query failed";
			foreach my $forumrow (@$forums){
				my $forumidnumber = $forumrow->[0];
				my $forumname = $forumrow->[1];
				$threadssth->execute($courseid,$forumidnumber) or die "execute failed \n $!";
				my $threads = $threadssth->fetchall_arrayref() or die "thread query failed";

				foreach my $row ( @$threads ){
					my $courseid = $row->[1];
					my $threadid = $row->[2];
					
					my $inst_replied = $row->[3];
					my $docid = $row->[0];
					
					#Sanity checks
					if (!defined $docid ){
						die "DOCID is null for $courseid \t $threadid \t $forumidnumber \t $inst_replied \n";
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
										
					$allthreads{$serial_id} = [$courseid,$threadid,$forumname,$forumidnumber];
					$docid_to_serialid{$serial_id} = $docid;	
					if ($inst_replied) { $instreplied{$serial_id} = 1; } 
					$serial_id ++;
				}
				print "\n $courseid \t $forumidnumber\t" . (keys %allthreads) ." threads";
			}
		}
		
		#sanity check
		if (keys %allthreads == 0 ){
			die "\nException: No threads found!";
		}
			
		foreach my $iter (@iterations){
			my($d0,$d1,$d2,$d3,$d4,$d5,$d6,$d7,$d8,$d9,$d10) = getBin($iter);
			print "\n Iteration $iter begins. Set $d0-$d1-$d2-$d3-$d4-$d5-$d6-$d7-$d8-$d9-$d10";
			
			$outfile = "$exp_path/";
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
				# $pdtb 				= $d6;
				$pdtb_exp			= $d6;
				$pdtb_imp			= $d7;
			}
			
			# output file
			$outfile  .=  $d0 	? "forum+"			: "";
			$outfile  .=  $d1 	? "affir+"  		: "";	
			$outfile  .=  $d2 	? "tprop+"			: "";
			$outfile  .=  $d3 	? "nums+"			: "";
			$outfile  .=  $d4 	? "nont_course+"  	: "";
			$outfile  .=  $d5	? "agree+"			: "";
			#$outfile  .=  $d6 	? "pdtb+"	 		: "";
			$outfile  .=  $d6 	? "exppdtb+" 		: "";
			$outfile  .=  $d7 	? "imppdtb+" 		: "";
			$outfile  .=  $d7	? "course+" 		: "";
			
			$outfile	.=  "_$type" . "_$fold.txt";
			
			#feature name file
			my $feature_file = "features";
			
			$feature_file .= $d0 	? "+forum"  	: "";
			$feature_file .= $d1 	? "+affir"		: "";
			$feature_file .= $d2 	? "+tprop"		: "";
			$feature_file .= $d3 	? "+nums"  		: "";
			$feature_file .= $d4 	? "+nont_course": "";
			$feature_file .= $d5 	? "+agree"		: "";
			#$feature_file .= $d6 	? "+pdtb"	 	: "";
			$feature_file .= $d6 	? "+exppdtb" 	: "";
			$feature_file .= $d7 	? "+imppdtb" 	: "";
			$feature_file .= $d7	? "+course" 	: "";

			$feature_file .= "_$fold.txt";
			
			my %samples = ();
			my @positivethreads;
			my @negativethreads;
			my %samplecount = ('+1' => 0,
							   '-1' => 0
							  );

			foreach my $serial_id (keys %allthreads){
				if ( !exists $samples{$serial_id} ) {
					if ( $samplemode eq 'all') {
						if (!exists $instreplied{$serial_id} ){					
							addtosample(\%allthreads, $serial_id, '-1', \@negativethreads, \%samples);
							$samplecount{'-1'}++;
						}
						else{
							addtosample(\%allthreads, $serial_id, '+1', \@positivethreads, \%samples);
							$samplecount{'+1'}++;
						}
					}
				}
				
			}

			if(keys %allthreads eq 0){
				print "Exception: no threds sampled!! for $type";
				exit(0);
			}

			if(scalar @positivethreads eq 0 && scalar @negativethreads eq 0){
				print "\n Exception: no pos or neg threads sampled!! for $type";
				exit(0);
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
			
			open (my $FH1, ">$tmp_file") or die "cannot open features file $!";
			open (my $FEXTRACT, ">$error_log_file") or die "cannot open features file$!";
			FeatureExtraction::generateTrainingFile(	$FH1, $dbh, \%threadcats, 
														$unigrams, $freqcutoff, $stem, $term_length_cutoff, $tftype, $idftype,
														$tprop, $numw, $numsentences, 
														$courseref, $nonterm_courseref, $affirmations, $agree,
														$numposts, $forumtype, 
														$exp_path, $feature_file,
														\%course_samples, $corpus, $corpus_type, $FEXTRACT, $log,
														$debug, $pdtb_exp, $pdtb_imp, $pdtbfilepath, $removed_files, $print_format
													);
			close $FH1;
			open (my $IN, "<$tmp_file") or die "cannot open features file $!";
			open (my $OUT, ">$outfile") or die "cannot open features file $!";

			Utility::fixEOLspaces($IN,$OUT);

			close $OUT;
			close $IN;

			print "\n +ve samples:  $samplecount{'+1'} ";
			print "\n -ve samples:  $samplecount{'-1'} ";
			print "\n ##Done##";
		}
	}
}
Utility::exit_script($progname,\@ARGV);

sub getTrainTestCourseSetsHO{
	my($fold, $trainingcourses, $testingcourses) = @_;
	#make local copies
	push (my @trainingcourseslocal, @$trainingcourses);
	push (my @testingcourseslocal,	@$testingcourses);
	
	my @trainingset; 
	my @testset;
	
	#test is just the course of the current index
	push (@testset, $testingcourseslocal[$fold]);
	
	#training is all course but the current index
	#so remove the current index course and push the rest
	splice @testingcourseslocal, $fold, 1;
	push (@trainingset, @trainingcourseslocal, @testingcourseslocal);

	# print "\n\n train set for $fold". (scalar @trainingset);
	# print "\n test set for $fold". (scalar @testset);
	
	return(\@trainingset,\@testset);
}

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

	# if($threadid eq 819 && $courseid eq 'medicalneuro-002'){
		# print "\n $courseid  \t $threadid \t $docid \t $label";
		# exit(0);
	# }

	if (!defined $threadid || !defined $courseid || !defined $docid || !defined $forumname){
		die "Exception: addtosample undef $serial_id \t $courseid \t $threadid \t $docid \t $forumname\n";
	}

	push (@$threads, [$threadid,$docid,$courseid,$label,$forumname,$forumidnumber,$serial_id]);
	$samples->{$serial_id} = 1;
}

sub readRemovedFiles{
	my ($courses) = @_;
	my %removed_files = ();
	foreach my $courseid (@$courses){
		open( my $rem_fh, "<$path/../data/Removed_files_$courseid".".txt") 
				or die "\n Cannot open $path/../data/Removed_files_$courseid.txt";
		while (my $line = <$rem_fh>){
			chomp $line;
			if ($line =~ /^$/){ next; }
			if ($line =~ /^\s*$/){ next; }
			if ($line =~ /^Folder.*$/){ next; }
			$line	=~ s/^(.*)?\.txt$/$1/;
			$removed_files {$courseid}{$line} = 1;
		}
		close $rem_fh;
	}
	return \%removed_files;
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
