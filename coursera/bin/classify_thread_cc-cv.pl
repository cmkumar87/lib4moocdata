#!/usr/bin/perl -w
use strict;
require 5.0;
use Algorithm::LibLinear;
use POSIX;


##
#
# Author : Muthu Kumar C
# Created in Spring, 2014
# Modified in Fall, 2015
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

### USER customizable section
$0 =~ /([^\/]+)$/; my $progname = $1;
###

use lib "$path/../lib";
use Model;
use Utility;

my $datahome = "$path/experiments";

sub Help {
	print STDERR "Usage: $progname -h\t[invokes help]\n";
  	print STDERR "       $progname -folds -in -indir[-cv|holdc -w -test -debug -q quiet]\n";
	print STDERR "Options:\n";	
	print STDERR "\t-folds	\t # of folds for cross validation.\n";
	print STDERR "\t-indir	\t input directory\n";
	print STDERR "\t-in	  	\t input data file name in $datahome .\n";
	print STDERR "\t-i	  	\t do error analysis in interactive mode after classification.\n";
	print STDERR "\t-q    	\t Quiet Mode (don't echo license).\n";
}

my $help		= 0;
my $quite		= 0;
my $debug		= 0;
my $interactive = 0;
my $corpus_name	= undef;
my $num_folds	= undef;
my $indir		= undef;
my $in1 		= undef;
my $in2 		= undef;
my $stem 		= 0;
my $weighing 	= 'none';
my $test 		= 0;
my $pilot 		= 0;
my $dbname		= undef;
my $incourse;

$help = 1 unless GetOptions(
				'dbname=s'	=>	\$dbname,
				'folds=i'	=>	\$num_folds,
				'in1=s'		=>	\$in1,
				'in2=s'		=>	\$in2,
				'indir=s'	=>	\$indir,
				'stem'		=>	\$stem,
				'i'			=>	\$interactive,
				'corpus=s'	=>	\$corpus_name,
				'w=s'		=>	\$weighing,
				'debug'		=>	\$debug,
				'h' 		=>	\$help,
				'q' 		=>	\$quite
			);

if ( ( $help || !defined $num_folds ||  !defined $in1 || !defined $in2) && (!defined $corpus_name) ){
	Help();
	exit(0);
}


# Secure database connection as a global variable
my $dbh		= undef;
# my $dbh 	= Model::getDBHandle("$path/../data/",undef,undef,$dbname);

my $experiments_path	= $path . "/../experiments/$indir";
my $results_path		= $experiments_path."/results";

my $counter = 0;

my %output 			= ();
my %output_details	= ();
my %f1 				= ();
my %f2 				= ();
my %f4 				= ();
my %recall 			= ();
my %precision 		= ();
my %i_denC_train 	= ();
my %i_denC_test		= ();
my %foldsize		= ();
my %svmweights 		= ();
my $weight_optimization_search_step = 0.1;
my $training_time 	= 0;
my $testing_time 	= 0;

my $basename	= (split(/_train_/,$in1))[0];

#learnt model is written to this model file
my $model_file = "$experiments_path/models/$basename.model";
my %foldwise_contingency_matrix = ();
my @courses;
if ($corpus_name eq 'd61'){
	 @courses	= (	
						'acoustics1-001', 
						'advancedchemistry-001',
						'amnhearth-002',
						'analyze-001',
						'androidapps101-001',
						'automata-002',
						###'ccss-math1-002',
						# 'compmethods-003',
						'cosmo-003',
						############### 'crypto-010',
						###'diabetes-001',
						# 'dynamicalmodeling-001',
						###'dynamics1-001',
						'edc-002',
						'exdata-002',
						##############'friendsmoneybytes-004',
						'functionalanalysis-001',
						'gamification-003',
						##############'ggp-002',
						'globalwarming-002',
						##############'howthingswork1-002',
						'improvisation-005',
						'informationtheory-001',
						# 'innovativeideas-009',
						'intrologic-003',
						'maps-002',
						##############'marriageandmovies-001',
						'modernmiddleeast-001',
						##############'nanotech-001',
						##############'netsysbio-001',
						'networksonline-001',
						'neuralnets-2012-001',
						# 'newnordicdiet-002',
						'nlangp-001',
						'optimization-002',
						'organalysis-003',
						'pgm-003',
						###'pkubioinfo-002',
						'reactive-001',
						###'repdata-002',
						'sciwrite-2012-001',
						'sna-2012-001',
						'solarsystem-001',
						##############'statinference-002',
						##############'virtualassessment-001',
						'warhol-001',		
						'matrix-001'
			);
}
elsif ($corpus_name eq 'd14'){ 
	@courses	= (	
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
					# 'biostats-005',			
					'bioinfomethods1-001',
					'casebasedbiostat-002'
				 );
}
elsif($corpus_name eq 'nus'){
		@courses = (  'classicalcomp-001'
								 ,'classicalcomp-002'
								 ,'reasonandpersuasion-001'
								 ,'reasonandpersuasion-002'
								)
}
elsif($corpus_name eq 'pitt'){
		@courses = ( 'accountabletalk-001',
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

my $num_courses = scalar @courses;
if($num_courses == 0){
	die "Exception: zero courses found! Check data file and the course database.\n";
}

print "\n Number of courses idendified in this dataset: $num_courses \n";

if(defined $corpus_name){
	$num_folds = $num_courses;
}
my %docid_to_courseid = ();

open (my $result_file, ">$results_path"."/results_".$basename."_".$corpus_name.".txt") 
	or die "cannot open $results_path/results....txt";
# print header	
print $result_file "\n FOLD \t # of samples \t P \t R \t F_1 \t +Train% \t idenC_train \t idenC_test \t FPR \t";
print $result_file "Train_+ve \t Train_-ve \t Test_+ve \t Test_-ve";
	
open (my $con_matrices_file, ">$results_path"."/matrices_".$basename."_".$corpus_name.".txt") 
	or die "cannot open $results_path/results....txt";
# print header
print $con_matrices_file "Course \t Weight \t True +ve \t True -ve \t False +ve \t False -ve \n";

# open (my $output_fh, ">$results_path"."/results_dtl_".$basename."_".$corpus_name)
	# or die "cannot open $experiments_path/results....txt";

my $terms;
# $terms = Model::getalltermIDF($dbh,undef,0,\@courses);

foreach my $i (0..($num_folds-1)){
	my $weight		= 1;
	my %data_to_shuffle_mapping = ();
	
	print $con_matrices_file "\n $courses[$i] \t";
	my $lastname	= (split(/_train/,$in1))[1];
	$lastname =~ s/(\_).*\.?(txt)/$1$i.$2/;
	
	# print "\n $lastname"; next;
	
	my $training_data_file	= "$experiments_path/".$basename."_train".$lastname;
	my $test_data_file		= "$experiments_path/".$basename."_test".$lastname;
	
	print "\n Training Data File: $training_data_file ";
	print "\n Test Data File: $test_data_file ";
	
	my $training_data; my $test_data; my $ground_truth;
	
	($training_data, $ground_truth)	= readFeatureFile($training_data_file, $ground_truth);
	print  "\n" . (keys %$ground_truth);
	($test_data, $ground_truth)		= readFeatureFile($test_data_file, $ground_truth);
	print  "\n" . (keys %$ground_truth);
	
	foreach my $docid (keys %$training_data){
		$data_to_shuffle_mapping{$counter}	= $docid;		
		$counter++;
	}

	my $number_of_samples = keys %$test_data;
	
	if($number_of_samples == 0){
		print "Exception: zero samples read! Check data file.\n"; exit(0);
	}

	print "#samples: $number_of_samples \t # Folds: $num_folds \n";

	# print $result_file " # sample: $number_of_samples \t #folds: $num_folds \n";	
	open (my $output_fold_fh, ">$results_path"."/results_dtl_".(split (/\./,$in1))[0]."_".$courses[$i].".txt") 
		or die "cannot open $experiments_path/results....txt";
	
	# my ($trainingset, $testset) = getTrainTestCourseSetsCV($i, $part_size, keys %data_to_shuffle_mapping);

	my ($sec,$min,$hour,@rest)	=  localtime(time);
	my $start_timestamp 		= ($hour*60*60)+($min*60)+($sec);
	
	my %trainingset_distribution	= ();
	#create training file
	open TRAIN, ">$experiments_path/DATA.train";
	foreach my $doc_id (keys %$training_data){
		if ($ground_truth->{$doc_id} eq "+1"){
			$trainingset_distribution{'+'} ++;
		}
		else{
			$trainingset_distribution{'-'} ++;
		}
		print TRAIN "$training_data->{$doc_id}\n";
	}
	close TRAIN;

	## Naive SVM class weight computation from training data
	my $init_weight = 1;
	if ( $trainingset_distribution{'+'} > 0 ){
	   $init_weight = ($trainingset_distribution{'-'} / $trainingset_distribution{'+'});
	}
	
	if ($trainingset_distribution{'+'} == 0){
		die "\nNo + ve samples found. \n Setting init weight to $init_weight.";
	}
	
	if ($weighing eq 'opt'){
			$weight =  optimizeWeight(	$init_weight,
										$experiments_path,
										"DATA.validation",
										$training_data,
										$weight_optimization_search_step
									 );
	}
	elsif ($weighing eq 'nve'){
			$weight =  $init_weight;
	}
	elsif ($weighing eq 'none'){
			$weight =  1;
	}
	else{
		print "\n Invalid value for option -w (class weight) \n";
		Help();
		exit(0);
	}

	printf $con_matrices_file "\t %0.3f", $weight;
	
	my $learner 	 = getClassifier($weight);
	my $training_set = Algorithm::LibLinear::DataSet->load(filename => "$experiments_path/DATA.train");
	my $classifier	 = $learner->train(data_set => $training_set);

	print "\n# Features: " . $classifier->num_features ;
	
	my $model_file_fold = $model_file . "_$courses[$i]";
	$classifier->save(filename => $model_file_fold);
	$svmweights{$i} = getSVMweights($model_file_fold);

	my ($sec1,$min1,$hour1,@rest) = localtime(time);
	my $end_timestamp = ($hour1*60*60)+($min1*60)+($sec1);
	my $duration = $end_timestamp - $start_timestamp;
	$training_time += $duration;
	printf "Training time for fold $i:\t%02d second(s) \n", $duration;
	
	($sec,$min,$hour,@rest) =   localtime(time);
	$start_timestamp = ($hour*60*60)+($min*60)+($sec);
	
	my @testset_docids;
	#create test file
	open TEST, ">$experiments_path/DATA.test";
	foreach my $doc_id (keys %$test_data){
		push (@testset_docids, $doc_id);
		print TEST "$test_data->{$doc_id}\n";
	}
	close TEST;
	
	my $test_set = Algorithm::LibLinear::DataSet->load(filename => "$experiments_path/DATA.test");
	my $test_set_array_ref = $test_set->{'data_set'};
		
	my %foldoutput = (); my $j = 0;
	foreach my $test_instance (@$test_set_array_ref) {
		my $label	 = $test_instance->{'label'};
		my $features = $test_instance->{'feature'};
		
		# Determines which (+1 or -1) class should the test instance be assigned to
		# based on its feature vector.
		my $prediction = $classifier->predict(feature => $features);
		my $predict_values = $classifier->predict_values(feature => $features);
		
		$foldoutput{$testset_docids[$j]}{$label} = $prediction;
		$output{$testset_docids[$j]}{$label}	 = $prediction;
		$output_details{$testset_docids[$j]}	 = +{	serialid 		=> $j,
														fold 			=> $i,
														course			=> $courses[$i],
														label 			=> $label, 
														prediction 		=> $prediction, 
														predictvalue	=> $predict_values->[0],
														features		=> $features
													};
		
		$j++;
	}
	$j = 0;
	
	($sec1,$min1,$hour1,@rest) =   localtime(time);
	$end_timestamp = ($hour1*60*60)+($min1*60)+($sec1);
	$duration = $end_timestamp - $start_timestamp;
	# printf "Test time for fold $i\t%02d second(s)", $duration;
	$testing_time += $duration;

	$foldsize{$i} = (keys %foldoutput);
	print "Size of Output for $courses[$i]: $foldsize{$i} \n";

	my $matrix	= getContigencyMatrix(\%foldoutput);
	$foldwise_contingency_matrix{$i} = $matrix;
		
	printContigencyMatrix($matrix, $con_matrices_file);
	#savedetailedouput(\%foldoutput, $test_data, $output_fh, 1);
	savedetailedouput(\%foldoutput, $test_data, $output_fold_fh, 1);
	
	$precision{$i}	= sprintf ("%.3f", getPrecision($matrix) * 100 );
	$recall{$i}		= sprintf ("%.3f", getRecall($matrix) * 100 );
	$f1{$i}			= sprintf ("%.3f", computeF_m($matrix,1) * 100 );
	$f2{$i}			= sprintf ("%.3f", computeF_m($matrix,2) * 100 );
	$f4{$i}			= sprintf ("%.3f", computeF_m($matrix,4) * 100 );
	
	my $num_pos_samples = ($matrix->{'tp'} + $matrix->{'fn'});
	my $num_neg_samples = ($matrix->{'fp'} + $matrix->{'tn'});
	my $fpr;
	
	if($matrix->{'fp'} + $matrix->{'tn'} eq 0 ){
		$fpr 	= 0;
	}
	else{
		$fpr	= sprintf ("%.3f", ($matrix->{'fp'} / ($matrix->{'fp'} + $matrix->{'tn'}) * 100 ));
	}
	
	my $p_at_100r = 0;
	if($num_pos_samples > 0 ){
		$p_at_100r = ($num_pos_samples / ($num_pos_samples + $num_neg_samples));
	}
	my $f1_at_100r	= (2*$p_at_100r*1) / ($p_at_100r+1);
	my $f4_at_100r	= ((1+16)*($p_at_100r*1)) / ((16*$p_at_100r)+1);
	
	$p_at_100r	= sprintf ("%.3f", $p_at_100r * 100 );
	$f1_at_100r	= sprintf ("%.3f", $f1_at_100r * 100 );
	$f4_at_100r	= sprintf ("%.3f", $f4_at_100r * 100 );
	
	if ( $trainingset_distribution{'-'} ne 0){
		$i_denC_train{$i} = sprintf ("%.3f", $trainingset_distribution{'+'} / 
											$trainingset_distribution{'-'} );
	}
	else{
		$i_denC_train{$i} = 1;
	}
	
	if ( $num_neg_samples ne 0){
		$i_denC_test{$i}  = sprintf ("%.3f", $num_pos_samples / $num_neg_samples );
	}
	else{
		$i_denC_test{$i}  = 0;
	}
	my $training_positive = sprintf ("%.3f",($trainingset_distribution{'+'}/
								($trainingset_distribution{'+'} + $trainingset_distribution{'-'}))
							);
		
	print $result_file "\n $courses[$i] \t $number_of_samples \t $precision{$i}\t $recall{$i} \t $f1{$i}";
	
	# print $result_file "\t F_1:$f1{$i} \t F_2:$f2{$i} \t F_4:$f4{$i}";
	
	print $result_file "\t $training_positive ";
	print $result_file "\t $i_denC_train{$i}\t $i_denC_test{$i}\t $fpr\t";
	
	print $result_file "\t $trainingset_distribution{'+'} ";
	print $result_file "\t $trainingset_distribution{'-'} ";
	
	print $result_file "\t $num_pos_samples ";
	print $result_file "\t $num_neg_samples ";
	# print $result_file "\n \t P_at_100R:$p_at_100r\t F1_at_100R:$f1_at_100r\t F4_at_100R:$f4_at_100r \t ";
	print $result_file "\n at_100 \t \t $p_at_100r \t 100 \t $f1_at_100r\t \n";

	print "\n";
}

## Cross validation loop ends

#print "Size of Output: " . $number_of_samples . "\n";

my $p_avg	= sprintf( "%.3f", average(\%precision) );
my $p_rec	= sprintf( "%.3f", average(\%recall) );
my $p_f1	= sprintf( "%.3f", ((2*$p_avg*$p_rec)/(1*$p_avg+$p_rec)));
my $p_f2	= sprintf( "%.3f", ((5*$p_avg*$p_rec)/(4*$p_avg+$p_rec)));
my $p_f4	= sprintf( "%.3f", ((17*$p_avg*$p_rec)/(16*$p_avg+$p_rec)));
#my $numerator =  (1+($beta*$beta)) * ($precision*$recall);
#my $denom = ($beta*$beta*$precision)+$recall;
#my $p_f1	= sprintf( "%.3f", average(\%f1) );
#my $p_f2	= sprintf( "%.3f", average(\%f2) );
#my $p_f4	= sprintf( "%.3f", average(\%f4) );

print $result_file "Macro average over $num_folds folds: " ;
print $result_file "\n----------------------------------------------------------------";
# print $result_file "\n  P    \t   R    \t   F_1  \t   F_2    \t   F_4   \n" ;
# print $result_file "\n$p_avg \t $p_rec \t $p_f1	\t $p_f2 \t $p_f4\n" ;
print $result_file "\n  P    \t   R    \t   F_1  \n" ;
print $result_file "\n$p_avg \t $p_rec \t $p_f1 \n" ;
print $result_file "\n----------------------------------------------------------------\n";

$p_avg	= sprintf( "%.3f", weightedAverage(\%precision,\%foldsize) );
$p_rec	= sprintf( "%.3f", weightedAverage(\%recall,\%foldsize) ); 
$p_f1	= sprintf( "%.3f", ((2*$p_avg*$p_rec)/(1*$p_avg+$p_rec)));
$p_f2	= sprintf( "%.3f", ((5*$p_avg*$p_rec)/(4*$p_avg+$p_rec)));
$p_f4	= sprintf( "%.3f", ((17*$p_avg*$p_rec)/(16*$p_avg+$p_rec)));
#$p_f1	= sprintf( "%.3f", weightedAverage(\%f1,\%foldsize) );
#$p_f2	= sprintf( "%.3f", weightedAverage(\%f2,\%foldsize) );
#$p_f4	= sprintf( "%.3f", weightedAverage(\%f4,\%foldsize) );

print $result_file "\nWeighted Macro Average over $num_folds folds: " ;
print $result_file "\n----------------------------------------------------------------";
# print $result_file "\n  P    \t   R    \t   F_1  \t   F_2    \t   F_4   \n" ;
# print $result_file "\n $p_avg \t $p_rec \t $p_f1	\t $p_f2 \t $p_f4\n" ;
print $result_file "\n  P    \t   R    \t   F_1  \n" ;
print $result_file "\n $p_avg \t $p_rec \t $p_f1 \n" ;
print $result_file "\n----------------------------------------------------------------\n";

$p_avg	= sprintf( "%.3f", microAveragedPrecision(\%foldwise_contingency_matrix) * 100);
$p_rec	= sprintf( "%.3f", microAveragedRecall(\%foldwise_contingency_matrix) * 100);
$p_f1	= sprintf( "%.3f", microAverageF_m(\%foldwise_contingency_matrix,1) * 100); 
$p_f2	= sprintf( "%.3f", microAverageF_m(\%foldwise_contingency_matrix,2) * 100);
$p_f4	= sprintf( "%.3f", microAverageF_m(\%foldwise_contingency_matrix,4) * 100);

print $result_file "\n Micro Average over $num_folds folds: " ;
print $result_file "\n----------------------------------------------------------------";
# print $result_file "\n  P    \t   R    \t   F_1  \t   F_2    \t   F_4   \n" ;
# print $result_file "\n$p_avg \t $p_rec \t $p_f1	\t $p_f2 \t $p_f4\n" ;
print $result_file "\n  P    \t   R    \t   F_1 \n" ;
print $result_file "\n$p_avg \t $p_rec \t $p_f1	\n" ;
print $result_file "\n----------------------------------------------------------------\n";

# compute_weighted_micro_avg(\%foldwise_contingency_matrix, \%foldsize);
$p_avg	= sprintf( "%.3f", microAveragedPrecision(\%foldwise_contingency_matrix) * 100);
$p_rec	= sprintf( "%.3f", microAveragedRecall(\%foldwise_contingency_matrix) * 100);
$p_f1	= sprintf( "%.3f", microAverageF_m(\%foldwise_contingency_matrix,1) * 100); 
$p_f2	= sprintf( "%.3f", microAverageF_m(\%foldwise_contingency_matrix,2) * 100);
$p_f4	= sprintf( "%.3f", microAverageF_m(\%foldwise_contingency_matrix,4) * 100);

print $result_file "\nWeighted Micro Average over $num_folds folds: " ;
print $result_file "\n----------------------------------------------------------------";
# print $result_file "\n  P    \t   R    \t   F_1  \t   F_2    \t   F_4   \n" ;
# print $result_file "\n$p_avg \t $p_rec \t $p_f1	\t $p_f2 \t $p_f4\n" ;
print $result_file "\n  P    \t   R    \t   F_1 \n" ;
print $result_file "\n$p_avg \t $p_rec \t $p_f1	\n" ;
print $result_file "\n----------------------------------------------------------------\n";

close $result_file;

printf "\nTotal time elapsed for training: \t%02d second(s)", $training_time;
printf "\nTotal time elapsed for testing: \t%02d second(s)", $testing_time;
print "\n----------------------------------------------------------------";

if($interactive){
	while (){
		my $switch = getSwitch();
		if ($switch !~ /^([1-6]|(quit)|q)$/){ 
			print "\nInvalid input. Please enter 1, 2, 3, 4, 5 or 6.\n";
			next;
		}
		if( $switch == 1){
			my $matrix = getContigencyMatrix(\%output);
			printContigencyMatrix($matrix);
		}
		elsif( $switch == 2){
			my $courseid = getCourseid(\@courses);
			my $err_type = getErrtypeInput();
			getErrDocId($dbh,$courseid);
			my $docid = getErrDocId($dbh,$courseid, $err_type);
			printFeatureVector($dbh,$docid);
		}
		elsif( $switch == 3){
			my $courseid = getCourseid(\@courses);
			printDocMetaData($dbh,$courseid);
		}
		elsif( $switch == 4){
			my $docid = getdocidasInput();
			printFeatureVector($dbh,$docid);
		}
		elsif( $switch == 5){
			my $fold = getFold();
			my $sign = getSign();
			open (my $weightsfh, ">$experiments_path/results/weights_$sign.csv") 
								or die "cannot open file for print weights";
			printSupportVectors(\%svmweights, $fold,$sign, $weightsfh);
		}
		else{
			exit(0);
		}
	}
}
Utility::exit_script($progname,\@ARGV);
# MAIN ENDS HERE #

sub getSign{
	print  "\n Enter sign + or - or 0: ";
	my $sign = <STDIN>;
	$sign = untaint($sign);
	if ($sign eq '+'){return +1;}
	if ($sign eq '-'){return -1;}
	return 0;
}

sub getFold{
	#my($num_folds) = @_;
	my $num_folds = 14;
	print  "\n Enter a fold between 1 and $num_folds: ";
	my $fold = <STDIN>;
	$fold = untaint($fold);
	return ($fold-1);
}

sub optimizeWeight{
	my($init_weight, $path, $data_filename, $data, $search_step) = @_;
	my $weight	=	my $new_weight	=  sprintf("%.3f",$init_weight);
	my $max_accuracy		= 0;
	my $change				= 0;
	my $no_change_counter	= 0;
	my $num_folds			= 10;
	
	writeDataFile($data_filename, $path, $data);
	
	while ($no_change_counter <= 3){
		my $new_accuracy	= sprintf("%.3f", cross_validate2($data_filename, $path, $data, $new_weight, $num_folds));
		#my $new_accuracy	= sprintf("%.3f", cross_validate($data_filename, $path, $new_weight, $num_folds));
		$change	= sprintf("%.3f",($max_accuracy - $new_accuracy));
		print "\n $new_accuracy \t $change \t $weight	\t$new_weight";
		
		if ($change < 0){
			# we have new maximum
			$max_accuracy	= $new_accuracy;
			$weight			= sprintf("%.3f",$new_weight);
			
			#propose new weight
			$no_change_counter	= 0;
			$new_weight	= ($init_weight > 1)? ($weight + $search_step): ($weight - $search_step);
			$new_weight	= ($new_weight <= 0)? $weight :$new_weight;
		}
		else{
			# my $search_leap	= $no_change_counter+2;
			# $search_step += $search_leap;
			my $inc_new_weight	= ($init_weight > 1)? ($new_weight + $search_step): ($new_weight - $search_step);
			$new_weight	= ($inc_new_weight <= 0)? $new_weight :$inc_new_weight;
			print "\n nochange. proposing $new_weight";
			$no_change_counter ++;
		}
	}
	
	print "\n init weight:	 $init_weight";
	print "\n optimized weight: $weight";	
	
	return $weight;
}

sub cross_validate2{
	my ($data_filename, $path, $data, $weight, $num_folds) = @_;
	
	my %parts = ();
	my %f_1	= ();
	my $part_size = (keys %$data) * (1/$num_folds);
	my $random_number_seed = 458314;	
	srand($random_number_seed);
	
	print "\n Running cv with weight $weight \t $num_folds folds \t with $part_size instances per fold";
	
	foreach my $docid (keys %$data){
		my $assigned = 0;
		 while(!$assigned){
			my $part_index = int(rand($num_folds));
			if ( keys %{$parts{$part_index}} < $part_size){
				 $parts{$part_index}{$docid} = $data->{$docid};
				$assigned = 1;
			}
		}
	}
	
	foreach my $idx ( sort {$a <=> $b} keys %parts){
		my %training_data = (	
								%{$parts{($idx+1)%$num_folds}}
								,%{$parts{($idx+2)%$num_folds}}
								,%{$parts{($idx+3)%$num_folds}}
								,%{$parts{($idx+4)%$num_folds}} 
								,%{$parts{($idx+5)%$num_folds}}
								,%{$parts{($idx+6)%$num_folds}}
								,%{$parts{($idx+7)%$num_folds}}
								,%{$parts{($idx+8)%$num_folds}} 
								,%{$parts{($idx+9)%$num_folds}}
							);
							
		my $test_data = $parts{$idx};
		
		writeDataFile($data_filename."_cv_train", $path, \%training_data);
		my $model = train($data_filename."_cv_train", $path, $weight);
		
		writeDataFile($data_filename."_cv_test", $path, $test_data);
		$f_1{$idx}   = test($data_filename."_cv_test", $path, $model);
	}
	
	return (average(\%f_1));
}

sub getClassifier{
	my ($weight,$solver_type) = @_;
	
	if(!defined $solver_type){
		$solver_type = 'L1R_LR';
	}
	## Instantiate Liblinear SVM with the weight
	# Constructs a model either 
	# a) L2-regularized L2 loss support vector classification.
	# b) L1-regularized Logit model
	my $learner = Algorithm::LibLinear->new(
												epsilon 	=> 0.01,
												#solver 	=> 'L2R_L2LOSS_SVC_DUAL',
												solver	 	=> $solver_type,
												weights 	=> [
																+{ label => +1, weight => $weight,	},
																+{ label => -1, weight => 1,		},
															   ]
											);
}

sub cross_validate{
	my ($filename, $path, $weight, $num_folds) = @_;
	my $data_set	= Algorithm::LibLinear::DataSet->load(filename => "$path/$filename");
	if(!defined $data_set){
		die "\nException: training set undef \n";
	}
	my $classifier	= getClassifier($weight);
	my $accuracy	= $classifier->cross_validation(data_set => $data_set, num_folds => $num_folds);
	return $accuracy;
}

sub train{
	my ($filename, $path, $weight) = @_;
	my $training_set	= Algorithm::LibLinear::DataSet->load(filename => "$path/$filename");
	if(!defined $training_set){
		die "\nException: training set undef \n";
	}
	my $classifier		= getClassifier($weight);
	my $trained_model	= $classifier->train(data_set => $training_set);
	return $trained_model;
}

sub test{
	my ($filename, $path, $model) = @_;
	my $test_set	= Algorithm::LibLinear::DataSet->load(filename => "$path/$filename");
	
	my $test_set_array_ref = $test_set->{'data_set'};
	
	my %output = ();
	my $test_inst_id = 0;
	foreach my $test_instance (@$test_set_array_ref) {
		my $label = $test_instance->{'label'};
		my $features = $test_instance->{'feature'};

		my $prediction = $model->predict(feature => $features);
		my $predict_values = $model->predict_values(feature => $features);
		
		$output{$test_inst_id}{$label} = $prediction;
		$test_inst_id ++;
	}
	
	my $matrix = getContigencyMatrix(\%output);
	
	my $f1	= sprintf ("%.3f", computeF_m($matrix,1) * 100 );
	return $f1;
}

sub writeDataFile{
	my ($filename, $path, $data) = @_;
	
	#create data file
	open CVTRAIN, ">$path/$filename";
	foreach my $j (keys %$data){
		print CVTRAIN "$data->{$j}\n";
	}
	close CVTRAIN;
}
	
sub getCourseid{
	my $courses = shift;
	foreach my $course (@$courses){
		print "$course \t";
	}
	print "\n Enter a course to display: ";
	my $input_course = <STDIN>;
	$input_course =~ s/\s*(.*)\s*/$1/;
	return $input_course;
}

sub compute_weighted_micro_avg{
	my ($foldwise_contingency_matrix, $foldsize) = @_;
	my $number_of_samples = 0;
	foreach my $fold (keys %$foldsize){
		$number_of_samples += $foldsize->{$fold};
	}
	
	foreach my $fold ( sort {$a<=>$b} (keys %$foldsize) ){
		my $matrix = $foldwise_contingency_matrix->{$fold};
		my $weight = $foldsize->{$fold}/$number_of_samples;
		foreach my $label (keys %$matrix){
			$matrix->{$label} *= $weight;
		}
	}
}

sub microAverageF_m{
	my($foldwise_matrix, $beta) = @_;
	my $tp; my $fp;
	my $precision = microAveragedPrecision($foldwise_matrix);
	my $recall = microAveragedRecall($foldwise_matrix);
	my $numera =  (1+($beta*$beta)) * ($precision*$recall);
	my $denom = ($beta*$beta*$precision)+$recall;
	my $f_m = ($denom == 0) ? 0: ($numera/$denom);
	
	return $f_m;
}

sub computeF_m{
	my($matrix, $beta) = @_;
	my $precision = getPrecision($matrix);
	my $recall = getRecall($matrix);
	my $numera =  (1+($beta*$beta)) * ($precision*$recall);
	my $denom = ($beta*$beta*$precision)+$recall;
	my $f_m = ($denom == 0) ? 0: ($numera/$denom);
	return $f_m;
}

sub getContigencyMatrix{
	my $output = shift ;
	my %matrix = ();
	
	$matrix{'tp'} = 0;
	$matrix{'fp'} = 0;
	$matrix{'tn'} = 0;
	$matrix{'fn'} = 0;
	$matrix{'+'}  = 0;
	$matrix{'-'}  = 0;

	for my $id (keys %$output){
		for my $label (keys %{$output->{$id}}){	
			if( $label eq 1 ){
				if ( $output->{$id}{$label} eq 1){
					$matrix{'tp'} ++;
				}
				else{
					$matrix{'fn'} ++;
				}
			}
			else{
				if ( $output->{$id}{$label} eq 1){
					$matrix{'fp'} ++;
				}
				else{
					$matrix{'tn'} ++;
				}
			}
		}
	}
	return \%matrix;
}

sub printContigencyMatrix{
	my($matrix, $FH) = @_;
	print "\n------------------------------\n";
	print "\tActual +\tActual -\n";
	print "------------------------------\n";
	print "Predicted +|\t$matrix->{'tp'}|\t$matrix->{'fp'}|\n";
	print "Predicted -|\t$matrix->{'fn'}|\t$matrix->{'tn'}|\n";
	print "------------------------------\n";
	
	if (defined $FH){
		# print $FH "\n------------------------------\n";
		# print $FH "\tActual +\tActual -\n";
		# print $FH "------------------------------\n";
		print $FH "\t$matrix->{'tp'} \t$matrix->{'fp'}";
		print $FH "\t$matrix->{'fn'} \t $matrix->{'tn'}";
		# print $FH "------------------------------\n";
	}
}

sub getPrecision{
	my($matrix) = @_;
	my $denom = ($matrix->{'tp'} + $matrix->{'fp'} );
	my $p = ($denom == 0) ? 0: ($matrix->{'tp'})/$denom;
	return $p;
}

sub microAveragedPrecision{
	my($foldwise_matrix) = @_;
	my $tp; my $fp;
	foreach my $fold (keys %$foldwise_matrix){
		$tp += $foldwise_matrix->{$fold}{'tp'};
		$fp += $foldwise_matrix->{$fold}{'fp'};
	}
	my $denom = ($tp + $fp );
	my $p = ($denom == 0) ? 0: ($tp)/$denom;
	return $p;
}

sub getRecall{
	my($matrix) = @_;
	my $denom = ($matrix->{'tp'}+$matrix->{'fn'} );
	my $r = ($denom == 0) ? 0: ($matrix->{'tp'})/$denom;
}

sub microAveragedRecall{
	my($foldwise_matrix) = @_;
	my $tp; my $fn;
	foreach my $fold (keys %$foldwise_matrix){
		$tp += $foldwise_matrix->{$fold}{'tp'};
		$fn += $foldwise_matrix->{$fold}{'fn'};
	}
	my $denom = ($tp + $fn );
	my $r = ($denom == 0) ? 0: ($tp)/$denom;
	return $r;
}

sub average{
	my($hash) = @_;
	my $average = 0;
	foreach (keys %$hash){		
		$average += $hash->{$_} ;
		#print "\n Hash value: $_ \t $hash->{$_}";
	}
	$average = $average/ (keys %$hash);
	$average = (sprintf "%.3f",$average);
	#print "\n Avg: $average";
	return $average;
}

sub weightedAverage{
	my($hash,$size) = @_;
	my $average = 0;
	my $number_of_samples = 0;
	my $weight_sum = 0;
	
	foreach my $fold (keys %$size){
		$number_of_samples += $size->{$fold};
	}
	
	foreach (keys %$hash){
		my $weight = $size->{$_}/$number_of_samples;
		$weight_sum += $weight;
		$average += ($weight * $hash->{$_}) ;
		#print "\n Hash value: $_ \t $hash->{$_} \t size:$size->{$_} \t $number_of_samples \t $weight";
	}
	#print "\n Avg: $average";
	print "\n Weight sum: $weight_sum";
	$average = (sprintf "%.3f",$average);
	return $average;
}

## Analysis functions

sub getSwitch{
	print  "\n 1. Contingency Matrix";
	print  "\n 2. Error analysis: Analyse docids by error type";
	print  "\n 3. Print Error thread metadata to file";
	print  "\n 4. Error analysis: print feature vector";
	print  "\n 5. Print Ranked features";
	print  "\n 6. Quit";
	print  "\n Enter an analysis option: ";
	my $switch = <STDIN>;
	$switch = untaint($switch);	
	return $switch;
}

sub getSVMweights{
	my $file = shift;
	my %weights = ();
	
	open MODEL, $file or die "Cannot open $model_file";
	while (<MODEL>){
		if ($. < 7) { next; }
		chomp;
		$_ =~ s/^\s*(.*)\s*$/$1/g;
		$weights{$.} = sprintf("%.3f",$_);
	}
	close MODEL;
	
	return \%weights;
}

sub getErrtypeInput{
	print  "\n 1. False Positives (TYPE I Error) ";
	print  "\n 2. False Negatives (TYPE II Error) ";	
	print  "\n 3. Quit";
	print  "\n Enter the type of error to analyse: ";
	
	my $err_type  = <STDIN>;
	# input validation
	$err_type =~ s/\s*(.*)\s*/$1/;
	if ($err_type !~ /^[123]$/){  
		print "\nInvalid input. Please enter 1, 2 or 3.";
		getErrtypeInput();
	}
	elsif ($err_type =~ /^3$/){
		exit(0);
	}
	return $err_type;
}

sub getErrDocId{
	my($dbh,$input_course,$err_type) = @_;
	
	print "In getErrDocId \n";
	if (!defined $input_course){
		die "Exception: input course not defined\n";
	}
	
	if (!defined $err_type){
		die "Exception: input error type not defined\n";
	}
	
	if (!defined $dbh){
		die "Exception: database not connected\n";
	}
	
	if(keys %output eq 0){
		die "print output is empty\n";
	}
	
	my %err_docs = ();
	# temporary hack -- relabelling
	# open FH, ">experiments/relabel/$in"."relabel" or die "failed to open rewrite file";
	system("chdir $experiments_path");
	if(! -d  "error_analysis"){
		system("mkdir error_analysis");
	}
	
	foreach my $doc_id (sort {$output{$a} <=> $output{$b}} (keys %output)){
		if(!defined $doc_id ){	
			warn "doc_id is not defined\n";
		}
		
		my $courseid = $docid_to_courseid{$doc_id};
		if ( $courseid ne $input_course ){	next;	}
		
		my $prediction_pair = $output{$doc_id};
		foreach (keys %$prediction_pair){
			my $label = $_;
			my $prediction = $prediction_pair->{$label};
			$err_docs{ $doc_id } = 1;
			
			# if( $label == $prediction ) { 
			#	temporary hack -- relabelling
			#	print FH "$data_to_shuffle_mapping{$shuff_id}\t$data{$data_to_shuffle_mapping{$shuff_id}}\n";
			#	next; 
			# }
			
			#true positive docids are printed here
			if ( $err_type == 0 && $label == 1 && $prediction == $label ){
				print "$doc_id \t";
			}
			
			#false positives
			if ( $err_type == 2 && $label == -1 && $prediction != $label){
				$err_docs{ $doc_id } = 1;				
				# temporary hack -- relabeling
				# my @line = (split /\t/, $data{$docid});				
				# my $curlabel = $line[1];
				# my $dataline = join ("\t", @line[1..$#line]);
				# $dataline =~ s/^\s*(.*)\s*$/$1/;				
				# print FH "$docid\t+1\t$dataline\n";				
				print "$doc_id \t";
			}
			
			#false negatives
			if( $err_type == 1 && $label == 1 && $prediction != $label){
				# temporary hack -- relabeling
				# print FH "$docid\t$data{$docid}\n";
				print "$doc_id \t";
			}
		}
	}
	#close FH;
	
	my $docid = getdocidasInput();
	
	if ( !exists $err_docs{$docid} ){ 
		print ("\nInvalid input.\n"); getErrDocId($dbh,$input_course,$err_type); 
	}
	
	return ($docid);
}

sub getdocidasInput{
	print  "\nEnter a DocId to see its feature vector: ";
	my $docid = <STDIN>;
	$docid = untaint($docid);
	return $docid;
}

sub printDocMetaData{
	my($dbh,$input_course) = @_;
	
	print "In getErrDocId \n";
	if (!defined $input_course){
		print "Exception: input course not defined\n";
		exit(0);
	}
		
	if (!defined $dbh){
		print "Exception: database not connected\n";
		exit(0);
	}
	
	if(keys %output eq 0){
		print "print output is empty\n";
		exit(0);
	}
	
	my %err_docs = ();
	system("chdir $experiments_path");
	if(! -d  "error_analysis"){
		system("mkdir error_analysis");
	}
	
	open TP, ">$experiments_path/error_analysis/$in1$input_course"."_TP" 
						or die "failed to open file";	
	open FP, ">$experiments_path/error_analysis/$in1$input_course"."_FP" 
						or die "failed to open file";	
	open FN, ">$experiments_path/error_analysis/$in1$input_course"."_FN" 
						or die "failed to open file";

	
	foreach my $doc_id (sort {$output{$a} <=> $output{$b}} (keys %output)){
		if(!defined $doc_id ){	
			warn "doc_id is not defined\n";
		}
		
		my( $threadid, $courseid, $forumid ) = Model::getthread($dbh,$doc_id);
		my $forumname = @{Model::getforumname($dbh, $forumid, $courseid)}[0];
		
		if ( $courseid ne $input_course ){	next;	}
		
		my $prediction_pair = $output{$doc_id};
		foreach (keys %$prediction_pair){
			my $label = $_;
			my $prediction = $prediction_pair->{$label};
			$err_docs{ $doc_id } = 1;

			#true positive docids are printed here
			if ( $label == 1 ){
				if ( $prediction == $label){
					print TP "$courseid \t $doc_id \t $forumname\n";
				}
				#false negatives
				elsif( $prediction != $label){
					print FN "$courseid \t $doc_id \t$threadid \t $forumname\n";
				}			
			}
			
			#false positives
			if ( $label == -1 && $prediction != $label){
				$err_docs{ $doc_id } = 1;				
				print FP "$courseid \t $doc_id \t$threadid \t $forumname\n";
			}
			
		}
	}
	close FN;
	close FP;
	close TP;
}

sub printFeatureVector{
	my($dbh,$docid) = @_;
	
	my($threadid,$courseid) = Model::getthread($dbh,$docid);
	print "\n--------------------------------------------------------";
	print "\n THREAD: $threadid \t DOCID: $docid \t COURSE : $courseid\n";
	
	my $termfreq;
	$termfreq = Model::getterms($dbh,$threadid,$courseid,$docid);

	my %termindex =();
	
	foreach my $termid (keys %$termfreq){		
		#my $termidf = Model::gettermIDF($dbh,$termid,$stem);
		#my $tfidf = $termfreq->{$termid}{'sumtf'} * $termidf;
		#push (my @termrow, ($termid, $_->[1], $_->[2], $termidf, $tfidf));
		#push @termarray, \@termrow;
		$termindex{$termid} = +{id		=>	$termid,
								term	=>	$termfreq->{$termid}{'term'},
								tf		=>	$termfreq->{$termid}{'sumtf'}
								#idf		=>	$termidf,
								#tfidf	=>	$tfidf
							}
	}
	displayFeatureVector($docid,\%termindex);
}

sub displayFeatureVector{
	my ($docid,$termindex) = @_;
	
	if (!defined $termindex){
		die "Exception: termindex not defined.";
	}
	
	if ( keys %{$termindex} == 0 ){
		die "Exception: termindex is empty.";
	}
	
	my $fold = $output_details{$docid}{'fold'};
	my $serialid = $output_details{$docid}{'serialid'};
	
	print "\nFold: $fold \t Target: $output_details{$docid}->{'label'} \t" .
		  "Prediction: $output_details{$docid}->{'prediction'}\t".
		  "Predict Value: ".sprintf("%.3f",$output_details{$docid}{'predictvalue'}) ."\n";
	
	my $nontermfeaturefile = 'features+';
	
	my @temp_in = split(/\+/,$in1);
	$nontermfeaturefile .= join("+", @temp_in[1..$#temp_in] );
	
	print "Opening $nontermfeaturefile ...\n";
	
	open (my $fh, "<$experiments_path/$nontermfeaturefile")
								or die "cannot open $experiments_path/$nontermfeaturefile";
	my %nontermfeatures = ();
	while(<$fh>){
		my $line = $_;
		my $id  = (split (/\t/,$line))[0];
		my $feature_name = (split (/\t/,$line))[1];
		$nontermfeatures{$id} = $feature_name;
	}
	
	print "\n----------------------------------------------------------------------------------------------";
	print "\nTermid\t\t Term \t\t\t tf \t idf \t tf-idf \t scaled \t svmweight \t product\n";
	print "\n----------------------------------------------------------------------------------------------";
	my $predictvaluesum = 0;
	my $termvaluesum = 0;
	my $nontermvaluesum = 0;
	foreach my $termid ( sort { $a <=> $b } ( keys ($output_details{$docid}->{'features'}) ) ){
		my $record = $termindex->{$termid};
		my $featureweight = $output_details{$docid}{'features'}{$termid};
		if ( defined $record->{'term'} ){
			# padding space onto term
			foreach ( length($record->{'term'})-1..20 ){
				$record->{'term'} = $record->{'term'} . " ";
			}

			print "\n$termid\t\t$record->{'term'}\t"
					."$record->{'tf'}"
					#\t$record->{'idf'}\t$record->{'tfidf'}";
					."\t\t";
			$termvaluesum += $svmweights{$fold}{$termid+6} * $featureweight ;
		}
		else{
			$nontermvaluesum += $svmweights{$fold}{$termid+6} * $featureweight ;
			print "\n$termid\t\t$nontermfeatures{$termid}\t\t\t\t\t\t\t\t";
		}
		
		print "$featureweight \t $svmweights{$fold}{$termid+6}\t\t". 
		sprintf "%.3f",($svmweights{$fold}{$termid+6}*$featureweight) ."\n";

		$predictvaluesum += $svmweights{$fold}{$termid+6} * $featureweight ;
	}
	print "\n----------------------------------------------------------------------------------------------";
	print "\nSum of term feature weights: $termvaluesum \n";
	print "\nSum of non-term feature weights: $nontermvaluesum \n";
	print "\nSum of feature weights: $predictvaluesum \n";
	close $fh;
}

sub makehashcopy{
	my ($hash2d) = @_;
	my %copy = ();
	
	foreach my $k1 (keys %$hash2d){
		foreach my $k2 (keys %$hash2d->{$k1}){
			$copy{$k1}{$k2} = $hash2d->{$k1}{$k2};
		}
	}
	return \%copy;
}

sub printSupportVectors{
	my($weights, $fold, $sign, $fh)  = @_;
	
	my $svmweights = makehashcopy($weights);
	print "\nargs: $fold \t $sign";
	#sanity check
	if (keys %$svmweights != keys %$weights){
		die "\n printSupportVectors: incorrect or empty copy of hashref $weights created";
	}
	
	print "\n svmweights keys " . (keys %{$svmweights{$fold}});
		
	if ($sign == 0){
		foreach my $feature (keys %{$svmweights{$fold}}){
			$svmweights{$fold}{$feature} = abs($svmweights{$fold}{$feature});
		}
	}
	
	foreach my $feature (sort {$svmweights{$fold}{$b} <=> $svmweights{$fold}{$a}} keys %{$svmweights{$fold}} ){
		if ($svmweights{$fold}{$feature} == 0){
			next;
		}
		elsif( $svmweights{$fold}{$feature} < 0 && $sign == 1){
			next;
		}
		elsif( $svmweights{$fold}{$feature} > 0 && $sign == -1){
			next;
		}
		
		$svmweights{$fold}{$feature}	= sprintf("%.3f", $svmweights{$fold}{$feature});
		
		my $termid = $feature - 6;
		$terms->{$termid}{'term'}	= sprintf("%-30s",$terms->{$termid}{'term'});
		print $fh "\n $termid \t $terms->{$termid}{'term'} \t\t $terms->{$termid}{'termid'} \t $svmweights{$fold}{$feature}";
	}
}

sub untaint{
	my $input = shift;
	chomp $input;
	$input =~ s/\s*(.*)\s*/$1/g;
	return $input;
}

sub savedetailedouput{
	my ($foldoutput, $data, $fh, $level) = @_;
		print $fh "Id \t Ground_Truth \t Prediction";
	foreach my $id ( keys %{$foldoutput} ){
		if(!defined $level){
			print $fh "\n $data->{$id}\t";
		}else{
			print $fh "\n $id \t";
		}
		foreach my $label ( keys %{$foldoutput->{$id}} ){
			print $fh "$label\t$foldoutput->{$id}{$label}";
		}
	}
}

sub deduplicate_array{
	my $arrayref = shift;
	my @array = @{$arrayref};
	my %hash   = map { $_, 1 } @array;
	my @unique = keys %hash;
	return \@unique;
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
		
		print "\n TRAIN: $trainingset_start \t $trainingset_end ";
		print "\n TEST: $testset_start \t $testset_end";
		
		#print "\nTRAINING COURSES: ";
		my @trainingset; 
		my @testset;
		if($trainingset_start > $trainingset_end){
			foreach my $j ($trainingset_start..($num_data_points-1),0..$trainingset_end){
				push (@trainingset, $j);
				#print "$j: $courses->{$j} \t";
			}
		}
		else{
			foreach my $j ($trainingset_start..$trainingset_end){
				push (@trainingset, $j);
				#print "$j: $courses->{$j} \t";
			}	
		}
		
		#print "\nTESTING COURSES: ";
		if($testset_start > $testset_end){
			foreach my $j ($testset_start..($num_courses-1),0..$testset_end){
				push (@testset, $j);
				#print "$j: $courses->{$j} \t";
			}
		}
		else{
			foreach my $j ($testset_start..$testset_end){
				push (@testset, $j);
				#print "$j: $courses->{$j} \t";
			}
		}
		
	return(\@trainingset,\@testset);
}

sub readFeatureFile{
	my ($in, $ground_truth)	=	@_;
	
	print "\n Reading $in";
	my %data = ();
	open DATA, "<$in" or die "Cannot open $in";

	while (<DATA>){
		chomp($_);
		$_ =~ s/\s*$//g;
		my @line = (split /\t/, $_);
		$line[0] =~ s/\s*$//g;
		my $docid = $line[0];
		
		my $dataline = join ("\t", @line[1..$#line]);
		$dataline =~ s/^\s*(.*)\s*$/$1/;
		
		# extract label and record as ground truth
		my $label	= $line[1];
		$label		=~ s/\s+//g;
		$ground_truth->{$docid} = $label;
		
		if( !exists $data{$docid} ){
			$data{$docid} = $dataline;
		}else{
			# print "docid: $docid \n";		#  . (split /\t/, $dataline)[0]; 
			#print "existing: $data{$docid}\n";	# . (split /\t/, $data{$docid})[0] ."\n"; 
		}
	}
	close DATA;
	return (\%data, $ground_truth);
}

=pod
# Executes cross validation.
#my $accuracy = $learner->cross_validation(data_set => $data_set, num_folds => 5);
#print "ACC: $accuracy\n";
=cut