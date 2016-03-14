#!/usr/bin/perl -w
use strict;
require 5.0;

##
#
# Author : Muthu Kumar C
# compute tf and df for the corpus of threads
# Created in Mar, 2014
#
##
## Example Run: 
##	perl compute_term_weights.pl -dbname <database_name> -uni -mode none -idf -thread inst
##

# Run without -stem flag first and then with -stem flag
# to get a vector with all + stemmed versions

use DBI;
use FindBin;
use Getopt::Long;
use utf8::all;

my $path;	# Path to binary directory

BEGIN{
	if ($FindBin::Bin =~ /(.*)/) 
	{
		$path  = $1;
	}
}

use lib "$path/../lib";
use Model;
use Utility;
use FeatureExtraction;

### USER customizable section
$0 =~ /([^\/]+)$/; my $progname = $1;
my $outputVersion = "1.0";
### END user customizable section

sub License{
	print STDERR "# Copyright 2014 \251 Muthu Kumar C\n";
}

sub Help{
	print STDERR "Usage: $progname -h\t[invokes help]\n";
  	print STDERR "       $progname [-idf -tf -thread[all|inst] -stem -uni -bi -q -debug]\n";
	print STDERR "Options:\n";
	print STDERR "\t-q \tQuiet Mode (don't echo license).\n";
	print STDERR "\t-debug \tPrint additional debugging info to the terminal.\n";
	print STDERR "\t-thread \t all threads or just those where isntructors/TAs replied.\n";
	print STDERR "\t-idf \t When set calculates idf and overwrites existing IDF values in IDF table.\n";
	print STDERR "\t-stem \tStem terms to the root for which weights are calcualted\n";
	print STDERR "##Run without -stem flag first and then with -stem flag";
	print STDERR "##\not get a vector with all + stemmed versions##";
}

my $help			= 0;
my $quite			= 0;
my $debug			= 0;
my $mode 			= 'inc';
my $dbname;
my $threadid		= 0;
my $forumtype;
my $countUnigrams	= 1;
my $countBigrams	= 0;
my $stem			= 0;
my $tf				= 0;
my $idf				= 0;
my $stopword		= 0;
my $threadtype		= undef;
my $threadcount		= 0;
my $course			= undef;

#database table names with defaults
my $tftab			= 'termFreqC14inst';
my $idftable		= 'termDF';
my $posttable		= 'post';
my $commenttable	= 'comment';
my $project 		= 'intervention';

$help = 1 unless GetOptions(
				'dbname=s'	=> \$dbname,
				'project=s'	=> \$project,
				'mode=s'	=> \$mode,
				'course=s'	=> \$course,
				'tf'		=> \$tf,
				'tftab=s'	=> \$tftab,
				'idftab=s'	=> \$idftable,
				'ptab=s'	=> \$posttable,
				'ctab=s'	=> \$commenttable,
				'uni'		=> \$countUnigrams,
				'bi'		=> \$countBigrams,
				'idf'		=> \$idf,
				'stem'		=> \$stem,
				'stop'		=> \$stopword,
				'thread=s'	=> \$threadtype, # only posts upto and not including instructor's reply are considered
				'count'		=> \$threadcount, # counts thread and post data. Term weights in tables aren't updated
				'h' 		=> \$help,
				'q' 		=> \$quite
			);
			
if ( $help || (!$countUnigrams && !$countBigrams && !$threadcount) ){
	Help();
	exit(0);
}

if( (!$threadcount && !defined $mode) || (!$threadcount && !$tf && !$idf) ){
	Help();
	exit(0);	
}

if (!$quite){
	License();
}

if( !defined $threadtype || ($threadtype ne 'inst' && $threadtype ne 'noinst' && $threadtype ne 'nota') ){
	print "Exception: Invalid threadtype \n";
	Help();
	exit(0);
}

if ($threadtype eq 'inst'){
	$posttable		= 'post2';
	$commenttable	= 'comment2';
	$tftab			= 'termFreqC14inst';
}
if ($threadtype eq 'noinst'){
	$posttable		= 'post';
	$commenttable	= 'comment';
	$tftab			= 'termFreqC14noinst';
}

if(!defined $dbname){
	print "\n Exception: dname not defined"; exit(0);
}

my $datahome = "$path/../data";
my $dbh = Model::getDBHandle($datahome,undef,undef,$dbname);

open (my $log ,">$path/../logs/$progname"."_$dbname.log")
			or die "cannot open file $path/../logs/$progname"."_$dbname.log for writing";
if(!defined $dbh){
	print $log "\n Exception dbhandle not defined"; 
	print "\n Exception dbhandle not defined"; exit(0);
}

print $log "\n Using database file at $datahome/$dbname";
# counts only posts where an instructor/TA/Staff has replied to a post
print  $log "\n threadtype.. $threadtype";
print  $log "\t tftab: $tftab \t posttab: $posttable \t commenttab: $commenttable ";

my $postsquery = "select id, post_text
				  from $posttable
				  where thread_id= ? 
				  and courseid=? and forumid=?";
				  
my $poststh = $dbh->prepare($postsquery)
						or die "Couldn't prepare user insert statement: " . $DBI::errstr;
				
my $commentsquery = "select id, comment_text 
					 from $commenttable
					 where post_id=? and thread_id=? 
					 and courseid=? and forumid=?";
					 
my $commentssth = $dbh->prepare($commentsquery) 
						or die "Couldn't prepare user insert statement: " . $DBI::errstr;
	
my $nonstem_df	= ();
my $termcounter = 0;
my %allterms	= ();
# collects doc (thread) frequencies 
my $alldf;
my $df;
my @courses;

if(defined $course){
	push (@courses, $course);
}

#initialization
if(defined $mode){
	$mode =~ s/\'//g;
	$mode =~ s/\"//g;
	print "\t Mode: $mode \n";
	if ( ($stem || $mode eq 'inc') && !$threadcount){
		print "\n Initializing terms...";
		print $log "\n Initializing terms...";
		($termcounter, $alldf) = initializeDFTerms($mode,\@courses);
		print $log "\n Current term counter $termcounter";
	}
}
								
my $forumidsquery = "select id,courseid,forumname from forum ";

#forumtypes
if ($project eq 'intervention'){
	print $log "\n Using data from Errata, Lecture, Homework, Exam forumtypes";
	print "\n Using data from Errata, Lecture, Homework, Exam forumtypes";
	$forumidsquery.= "	where forumname in('Errata','Lecture','Homework','Exam')";
}
else{
	print $log "\n Using data from all forumtypes";
	print "\n Using data from all forumtypes";
	$forumidsquery.= "	where forumname in('Errata','Lecture','Homework','Exam',
									'General','Project','Discussion','PeerA')";
}

if(defined $course){
	$forumidsquery = Model::appendListtoQuery($forumidsquery,\@courses,' courseid ',' and ');
}

my $forumrows		= $dbh->selectall_arrayref($forumidsquery) 
								or die "Courses query failed! $DBI::errstr \n $forumidsquery";

if(@$forumrows eq 0){
	print "\n No courses selected. Exiting...";
	print $log "\n No courses selected. Exiting..."; exit(0);
}
my $countofthreads	= "select count(1) from thread 
						where forumid=? and courseid=? 
						and inst_replied = ?";
my $countofthreadsth = $dbh->prepare($countofthreads)
								or die "prepare failed. $DBI::errstr \n $countofthreads";
							
my $numpostsquery 	= "select count(id) from $posttable where courseid = ? and forumid = ?";
my $numcmntsquery 	= "select count(id) from $commenttable where courseid = ? and forumid = ?";
my $numpoststh		= $dbh->prepare($numpostsquery) or die "prepare failed";
my $numcmntssth		= $dbh->prepare($numcmntsquery) or die "prepare failed";

my $total_number_of_threads = 0;
my $total_numposts = 0;

if( $threadcount ){
	print $log "\nDoing thread count...";
	if ($threadtype eq 'all') {
		print $log "\nPlease sepcify what type of threads to count with -thread option.\n";
		Help();
		exit(0);
	}
	my %count_by_forumtype = ();
	my $inst_replied = ($threadtype eq 'inst') ?1 :0 ;
	foreach my $forumrow ( @$forumrows ){
		my $forumid 	= @$forumrow[0];
		my $coursecode	= @$forumrow[1];
		$forumtype		= @$forumrow[2];
		$countofthreadsth->execute($forumid,$coursecode,$inst_replied) 
											or die "Execute failed \n $!";
		my $num_threads = @{$countofthreadsth->fetchrow_arrayref()}[0];
		
		$total_number_of_threads += $num_threads;
		$count_by_forumtype{$forumtype}{'threads'} += $num_threads;
		
		my $numposts += @{$dbh->selectcol_arrayref($numpoststh,undef,$coursecode,$forumid)}[0];
		$numposts += @{$dbh->selectcol_arrayref($numcmntssth,undef,$coursecode,$forumid)}[0];
		$total_numposts += $numposts;
		$count_by_forumtype{$forumtype}{'posts'} += $numposts;
	}
	
	foreach my $forumtype (keys %count_by_forumtype){
		print $log "\n$forumtype \t #threads \t $count_by_forumtype{$forumtype}{'threads'}";
		print $log "\t #posts \t$count_by_forumtype{$forumtype}{'posts'}";
	}
	print $log "\nTotal# thread: $total_number_of_threads \n";
	print $log "\nTotal# posts: $total_numposts \n";
	exit(0);
}

my $insertunigramsidfqry = "insert into $idftable (termid,term,df,idf,stem,courseid,forumid) 
																		values(?,?,?,?,?,?,?)";
my $insertunigramsidfsth = $dbh->prepare($insertunigramsidfqry)
						or die("prepare insertunigramsidfqry failed");

my $updateunigramsidfqry = "Update $idftable set df= ? 
								where termid =? and	term =?
								and courseid =? and forumid =?";

my $updateunigramsidfsth = $dbh->prepare($updateunigramsidfqry)
						or die("prepare updateunigramsidfqry failed");
						
my $insertunigramsqry = "insert into $tftab (termid,threadid,courseid,term,tf,type,stem,
																		stopword,commentid,postid,ispost) 
																	values(?,?,?,?,?,?,?,?,?,?,?)";
my $insertunigramssth	= $dbh->prepare($insertunigramsqry)
						or die("prepare insertunigramsqry failed");
my $insertbigramsqry = "insert into $tftab (termid,threadid,courseid,term,tf,type,stem,
																		stopword,commentid, postid,ispost) 
																	values(?,?,?,?,?,?,?,?,?,?,?)";
my $insertbigramssth	= $dbh->prepare($insertbigramsqry)
						or die("prepare insertbigramsqry failed");
						
if($idf && $mode eq 'inc'){
	print $log "\n Making a copy of initialised df hash..";
	
	# creating a copy
	foreach my $courseid ( keys %$alldf ){
		foreach my $forumid ( keys %{$alldf->{$courseid}} ){	
			foreach my $term ( keys %{$alldf->{$courseid}{$forumid}} ){	
				$df->{$courseid}{$forumid}{$term} = $alldf->{$courseid}{$forumid}{$term};
			}
		}
	}	
	
	#sanity check
	# foreach my $courseid ( keys %$df ){
		# foreach my $forumid ( keys %{$df->{$courseid}} ){	
			# print $log "\n #terms in the copy: $courseid ". keys (%$df->{$courseid}{$forumid});
			# if ( keys( %{$df->{$courseid}{$forumid}} ) != keys(%$alldf->{$courseid}{$forumid}) ){
				# print $log "Exception: inc mode error. dfs not initialized properly";
				# print "Exception: inc mode error. dfs not initialized properly";
				# exit(0);
			# }
		# }
	# }
}

foreach my $forumrow ( @$forumrows ){
	my $forumid		= @$forumrow[0];
	my $coursecode	= @$forumrow[1];
	$forumtype		= @$forumrow[2];
	
	my $inst_replied = (($threadtype eq 'inst') || ($threadtype eq 'nota') )?1 :0 ;
	my $number_of_threads;
	$number_of_threads = @{$dbh->selectcol_arrayref($countofthreads,undef,$forumid,$coursecode,$inst_replied)}[0];
	
	if( $number_of_threads == 0){ 
		print $log "\n No threads found for $forumid with inst_reply = $inst_replied";
		next; 
	}
	
	my @threads = undef;
	
	if ($threadtype eq 'inst'){
		print $log "\n Picking threads where an instructor or ta has replied\n";
		@threads = @{Model::Getthreadids($dbh, $coursecode, $forumid, "inst_replied=1")};
	}
	elsif ($threadtype eq 'nota'){
		print $log "\n Picking threads where an instructor has replied.\n Comm TA replies don't count.\n";
		@threads = @{Model::getIntructorTAOnlyThreads($dbh, $coursecode, $forumid)};
	}
	elsif ($threadtype eq 'noinst'){
		print $log "\n Picking threads where an instructor has **not** replied\n";
		@threads = @{Model::Getthreadids($dbh, $coursecode, $forumid, "inst_replied<>1")};
	}
	else{
		print $log "\n Picking all threads";
		@threads = @{Model::Getthreadids($dbh, $coursecode, $forumid, undef)};
	}
	
	print $log "\n Starting to loop over all the threads for $coursecode \t $forumid \n";
	foreach my $thread (@threads){
		# collects term frequencies for this thread
		my $terms	 = ();
		my $threadid = $thread->[0];
		
		$poststh->execute($threadid,$coursecode,$forumid) or die $DBI::errstr;
		my $posts = $poststh->fetchrow_hashref();
		
		if ($poststh->rows == 0) {
			print $log "\n No records for $threadid $coursecode $forumid";
		}

		while ( defined $posts ){
			my $postId		= $posts->{'id'};
			my $postText	= $posts->{'post_text'};

			if ($countUnigrams){
				my $unigrams = FeatureExtraction::extractNgrams($postText, 1, $stem,$stopword);
				my $termfreqhash;
				($terms, $termfreqhash)= termFreq($terms,$unigrams);
				if($tf){	updateTermFreq($termfreqhash, \%allterms, 'uni', $threadid, $coursecode, undef, $postId,1);	}
			}
			if ($countBigrams){
				my $bigrams = FeatureExtraction::extractNgrams($postText, 2, $stem,$stopword);
				my $termfreqhash;
				($terms, $termfreqhash)= termFreq($terms,$bigrams);
				if($tf){	updateTermFreq($termfreqhash, \%allterms, 'bi', $threadid, $coursecode, undef, $postId,1);	}
			}
			
			$commentssth->execute($postId, $threadid, $coursecode, $forumid) or die $DBI::errstr;
			my $comments = $commentssth->fetchrow_hashref();
			while ( defined $comments ){
				my $commentId = $comments->{'id'};
				my $commentText = $comments->{'comment_text'};

				if ($countUnigrams){
					my $unigrams = FeatureExtraction::extractNgrams($commentText, 1, $stem, $stopword);
					my $termfreqhash;
					($terms, $termfreqhash) = termFreq($terms,$unigrams);
					if($tf){	updateTermFreq($termfreqhash, \%allterms, 'uni', $threadid, $coursecode, $commentId, $postId,0);	}
				}
				if ($countBigrams){
					my $bigrams = FeatureExtraction::extractNgrams($commentText, 2, $stem, $stopword);
					my $termfreqhash;
					($terms, $termfreqhash) = termFreq($terms,$bigrams);
					if($tf){	updateTermFreq($termfreqhash, \%allterms, 'bi', $threadid, $coursecode,  $commentId, $postId,0);	}
				}
				## fetch the next comment
				$comments = $commentssth->fetchrow_hashref();
			}##comment loop ends
			## fetch the next post
			$posts = $poststh->fetchrow_hashref();
		}##post loop ends
		
		## Update document frequncies
		$df = docFreq($terms, $coursecode, $forumid, $df, $alldf);
	}##thread loop ends
}

if($idf){
		insertIDF($df,$alldf);
}

Utility::exit_script($progname,\@ARGV);
## Main Ends ##

sub initializeDFTerms{
	my ($mode,$courses) = @_;
	print $log "\n initializing DFTerms ...\n\n";
	my $termcounter = 0;
	my %alldf = ();
	
	if ($mode eq 'inc'){
		my $idfmaxquery = "select max(termid) from $idftable ";
		$termcounter = @{$dbh->selectcol_arrayref($idfmaxquery)}[0];

		if(!defined $termcounter){
			print "\n Exception: initializeDFTerms: initialization of termcounter failed.";
			print "\n args: IDFtab: $idftable \n Query: $idfmaxquery ";
			print $log "\n Exception: initializeDFTerms: initialization of termcounter failed.";
			print $log "\n args: IDFtab: $idftable \n Query: $idfmaxquery ";
			exit(0);
		}
		
		my $idfquery = "select distinct term,termid from $idftable ";
		print $log "\n $idfquery";
		
		my @idfterms = @{$dbh->selectall_arrayref($idfquery)};
		my $num_df_terms = scalar @idfterms;
		
		print $log "\n $num_df_terms terms found in the DF table";
		
		if ( $num_df_terms == 0){
			print $log "\n Exception: can't run inc mode. termDF table is empty \n";
			print  "\n Exception: can't run inc mode. termDF table is empty \n";
			exit(0);
		}
		
		# $allterms is a hash of terms to termids
		foreach my $idf_tab_row (@idfterms){
			$allterms{$idf_tab_row->[0]} = $idf_tab_row->[1];
		}
		
		# all terms with their document frequency
		my $idfrowquery = "select distinct term, forumid, courseid, df from $idftable ";
		if(defined $courses && scalar @courses ne 0){
			$idfrowquery = Model::appendListtoQuery($idfrowquery,$courses, ' courseid ',' where ');
		}
		print $log "\n$idfrowquery";
		my @idfrows = @{$dbh->selectall_arrayref($idfrowquery)};
		
		foreach my $idf_tab_row (@idfrows){
			$alldf{$idf_tab_row->[2]}{$idf_tab_row->[1]}{$idf_tab_row->[0]} = $idf_tab_row->[3];
		}
		
		#sanity check
		if(keys %allterms != $num_df_terms){
			print "Exception: initializeDFTerms: Expected $num_df_terms. Found " . (keys %allterms) ." terms\n";
			print $log "Exception: initializeDFTerms: Expected $num_df_terms. Found " . (keys %allterms) ." terms\n";
			exit(0);
		}
		print $log "\n Initialization complete.\n\n";
	}
	return ($termcounter,\%alldf);
}

sub termFreq{
	my($tf,$grams) = @_;
	my %termfreqhash = ();	
	# document frequency
	foreach ( keys %$grams ) {
		my $termid;
		
		if ( !exists $allterms{$_} ){
			$termcounter++; 
			$termid			= $termcounter;
			$allterms{$_}	= $termid;
		}
		else{
			$termid = $allterms{$_};
		}
		
		#update tf for the thread
		if ( !exists $tf->{$_} ){
			$tf->{$_} = $grams->{$_};
		}
		else{
			$tf->{$_} += $grams->{$_};
		}
		$termfreqhash{$termid} = $grams->{$_};
	}
	return ($tf,\%termfreqhash);
}

sub docFreq{
	my($tf, $coursecode, $forumid, $df, $alldf) = @_;
	foreach my $term (keys %$tf)
	{
		if ( !exists $df->{$coursecode}{$forumid}{$term} 
				&& !exists $alldf->{$coursecode}{$forumid}{$term} ){
			$df->{$coursecode}{$forumid}{$term}= 1;
		}
		else{
			$df->{$coursecode}{$forumid}{$term}++;
		}
	}
	return $df;
}

sub updateTermFreq{
	my ($termfreqhash, $allterms, $termtype, $threadid, $coursecode, $commentid, $postid, $ispost) = @_;
	my %termidtoterm = reverse %$allterms;
	#sanity check
	if ( keys %termidtoterm != keys %$allterms ){
		die "Insane! updateTermFreq: allterms hash keys don\'t map 1-to-1 with values";
	}
	print $log "\n $threadid : $coursecode :$postid : $ispost ";
	#Update term Frequency
	#defer commit by tuning off autocommit
	$dbh->{AutoCommit} = 1;
	$dbh->begin_work;
	
	foreach my $termid ( keys %$termfreqhash )
	{
		if ( $ispost eq '1'){
			if ( $termtype eq 'uni'){
				$insertunigramssth->execute($termid, $threadid,  $coursecode, 
														$termidtoterm{$termid}, $termfreqhash->{$termid}, 
														$forumtype, $stem, $stopword, undef, $postid, $ispost)
											or die "updateTermFreq: Update failed $termid:$threadid:$coursecode";
			}
			elsif ( $termtype eq 'bi'){
				$insertbigramssth->execute($termid, $threadid,  $coursecode, 
														$termidtoterm{$termid}, $termfreqhash->{$termid}, 
														$forumtype, $stem, $stopword, undef, $postid, $ispost)
											or die "updateTermFreq: Update failed $termid:$threadid:$coursecode";
			}
			else{
				print "Exception: updateTermFreq: undefined type $termtype $!"; exit(0);
			}
		}
		else{
			if ( $termtype eq 'uni'){
				$insertunigramssth->execute($termid, $threadid,  $coursecode, 
														$termidtoterm{$termid}, $termfreqhash->{$termid}, 
														$forumtype, $stem, $stopword, $commentid, $postid, $ispost)
											or die "Update failed $termid:$threadid:$coursecode";
			}
			elsif ( $termtype eq 'bi'){
				$insertbigramssth->execute($termid, $threadid,  $coursecode, 
														$termidtoterm{$termid}, $termfreqhash->{$termid}, 
														$forumtype, $stem, $stopword, $commentid, $postid, $ispost)
											or die "Update failed $termid:$threadid:$coursecode";
			}
			else{
				print "Exception: updateTermFreq: undefined type $termtype $!"; exit(0);
			}
		}
	}
	#commit
	$dbh->commit;
}

sub insertIDF{
	my ($df,$alldf) = @_;
	print "\n Inserting DFs...";
	#defer commit by tuning off autocommit
	$dbh->{AutoCommit} = 1;
	$dbh->begin_work;
	
	foreach my $coursecode ( keys %$df ){
		foreach my $forumid ( keys %{$df->{$coursecode}} ){
			#sanity check
			# if ( keys%{$df->{$coursecode}{$forumid}} == keys%{$alldf->{$coursecode}{$forumid}} ){
				# print "\n Debug info";
				# print "\n $coursecode \t $forumid ";
				# print "\t". (keys %{$df->{$coursecode}{$forumid}});
				# print "\t" .(keys %{$alldf->{$coursecode}{$forumid}});
				# die "Exception: inc mode error. originl dfs 'alldf' illegally updated!";
			# }
			foreach my $term ( keys %{$df->{$coursecode}{$forumid}} ){
				my $termid = $allterms{$term};
				my $freq = $df->{$coursecode}{$forumid}{$term};
				if(!exists $alldf->{$coursecode}{$forumid}{$term}){
					print $log "insert into IDF for $term \n";
					$insertunigramsidfsth->execute($termid, $term, $freq, 
													undef, 0,
													$coursecode, $forumid)
					or warn "insert to $idftable failed for \n  \t $termid \t $coursecode \t $forumid\n";
				}
				else{
					print $log "update IDF for $term \n";
					
					#sanity check
					my $oldFreq = $alldf->{$coursecode}{$forumid}{$term};
					#debug
					#print "\n old:$oldFreq new:$freq"; exit(0);
					if($freq < $oldFreq){
						print "\n$termid \t $term \t old:$oldFreq \t new:$freq";
						exit(0);
					}
					
					$updateunigramsidfsth->execute($freq, $termid, $term,
													$coursecode, $forumid)
					or warn "update to $idftable failed for \n  \t $termid \t $coursecode \t $forumid\n";					
				}
			}
			print $log "\n # terms: ".(keys %{$df->{$coursecode}{$forumid}});
			print $log "\n ## $coursecode -- $forumid Done.## \n";
			print "\n ## $coursecode -- $forumid Done.## \n";
		}
	}
	#commit
	$dbh->commit;
}

sub isnewstemterm{
	my $term = shift;
	if (!exists $nonstem_df->{$_}){	return 1;}
}