package FeatureExtraction;

# Configuration
use strict;
use warnings;

##
#
# Author : Muthu Kumar C
# Created in May, 2014
#
##

# Dependencies
# External libraries
use FindBin;
use Lingua::EN::Ngram;
use Lingua::EN::Tokenizer::Offsets qw( token_offsets tokenize get_tokens );
no autovivification;

#use lib "/opt/perl5/perls/perl-5.20.1/lib/site_perl/5.20.1"

my $path;	# Path to binary directory

BEGIN{
	if ($FindBin::Bin =~ /(.*)/) 
	{
		$path  = $1;
	}
}

use lib "$path/../lib";
# Local libraries
use Preprocess;
use Model;

sub generateTrainingFile{
	my (	$FH, $dbh, $mysqldbname, $threadcats,
			$unigrams, $freqcutoff, $stem, $term_length, $tftype, $idftype,
			$tprop, $numw, $numsentences,
			$courseref, $nonterm_courseref, $affir, $agree,
			$tlength, $forumtype,
			$path, $feature_file,
			$course_samples, $corpus, $corpus_type, $FEXTRACT, $log,
			$debug, $pdtb_exp, $pdtb_imp, $viewed, 
			$pdtbfilepath, $removed_files, $print_format
		) = @_;

	my @courses = keys %{$course_samples};
	my $num_threads_coursewise; 
	my $num_interventions_coursewise;
	
	if (keys %{$course_samples} == 0){
		print $log "\n Exception generateTrainingFile: course samples empty!!";
		print "\n Exception generateTrainingFile: course samples empty!!";
		exit(0);
	}
	
	#sanity checks
	if(defined $corpus){
		print "\n CORPUS: @$corpus ";
	}
	else{
		die "\n Exception: Corpus undef";
	}
	
	my $total_num_threads	= 0;
	if($unigrams){
		$total_num_threads		= Model::getNumValidThreads($dbh,$corpus);
		if ($total_num_threads	== 0){
			die "Exception in generateTrainingFile: # threads is zero in the corpus\n";
		}
		print "\n Number of valid threads \t $total_num_threads";
	}
	
	my $lexical = 0;
	my $lengthf = 0;
	my $time	= 0;
	
	if ( $courseref || $nonterm_courseref || $agree || $affir || $pdtb_exp || $pdtb_imp ){
		 $lexical = 1;
	}
	
	if ($tlength || $numw || $numsentences || $tprop){
		$lengthf = 1;
	}
	
	#thread length features
	my %thread_length		= ();
	my %numsentences		= ();
	my %avgnumsentences		= ();
	my %numsentences_first	= ();
	my %numposts			= ();
	my %threadPostlength	= ();
	my %threadCommentlength	= ();
	
	#course material reference features
	my %coursematerialterms				= ();
	my %coursematerialterms_nkd			= ();
	my %coursematerialterms_pfx			= ();
	my %coursematerialterms_sfx			= ();
	my %coursematerialtermdensity		= ();
	my %coursematerialtermdensity_nkd	= ();
	my %coursematerialtermdensity_pfx 	= ();
	my %coursematerialtermdensity_sfx 	= ();
	
	# discourse features: affirmations
	my %affirmations			= ();
	my %affirtermdensity		= ();
	my %agreedisagree			= ();
	my %agreedisagreedensity	= ();
	
	# pdtb discourse connectives and relations
	my %pdtbconnectives	= ();
	my %pdtbrelation	= ();
	my %pdtbrelation_imp= ();
	my %postwise_agreements = ();
	
	#lexical features
	my %num_urlref	= ();
	my %num_timeref = ();
	my %hasEquation = ();
	my %num_urlrefinfirstpost	= ();
	my %num_timereffirstpost	= ();
	my %hasEquationfirstpost	= ();
	
	#initialise maximum and minimum values
	my $maxnum_urlref	= 0.0;			my $minnum_urlref	= 999999999.0;
	my $maxnum_timeref	= 0.0;			my $minnum_timeref	= 999999999.0;
	my $maxnum_urlreffirstpost	= 0.0;	my $minnum_urlreffirstpost	= 999999999.0;
	my $maxnum_timereffirstpost = 0.0;	my $minnum_timereffirstpost = 999999999.0;
	
	my $maxthreadlength	= 0.0;			my $minthreadlength = 999999999.0;
	
	my $maxnumsentences = 0.0;			my $minnumsentences = 999999999.0;
	my $maxavgnumsentences 	  = 0.0;	my $minavgnumsentences	= 9999.0;
	my $maxnumsentences_first = 0.0; 	my $minnumsentences_first = 999999999.0;
	
	my $maxaffirmatives  = 0.0;			my $minaffirmatives  = 9999;
	my $maxagreedisagree = 0.0;			my $minagreedisagree = 9999;
	my $maxagreedisagreedensity	= 0.0;	my $minagreedisagreedensity = 999999999.0;
	
	my $maxcoursematerail	  = 0.0;	my $mincoursematerail	  = 9999;
	my $maxcoursematerail_nkd = 0.0;	my $mincoursematerail_nkd = 9999;
	my $maxcoursematerail_sfx = 0.0;	my $mincoursematerail_sfx = 9999;
	my $maxcoursematerail_pfx = 0.0;	my $mincoursematerail_pfx = 9999;

	my $maxcoursemateraildensity	 = 0.0;		my $mincoursemateraildensity	 = 9999;
	my $maxcoursemateraildensity_nkd = 0.0;		my $mincoursemateraildensity_nkd = 9999;
	my $maxcoursemateraildensity_sfx = 0.0;		my $mincoursemateraildensity_sfx = 9999;
	my $maxcoursemateraildensity_pfx = 0.0;		my $mincoursemateraildensity_pfx = 9999;
	
	my $maxaffir		= 0.0;				my $minaffir 		= 9999;
	my $maxaffirdensity = 0.0;				my $minaffirdensity = 9999;
	
	my $maxpdtbexpansion	= 0;			my $minpdtbexpansion	= 999999;
	my $maxpdtbcontingency	= 0;			my $minpdtbcontingency	= 999999;
	my $maxpdtbtemporal		= 0;			my $minpdtbtemporal		= 999999;
	my $maxpdtbcontrast		= 0;			my $minpdtbcontrast		= 999999;
	my $maxpdtball 			= 0;			my $minpdtball 			= 999999;

	my $maxpdtbimpexpansion		= 0;			my $minpdtbimpexpansion		= 999999;
	my $maxpdtbimpcontingency	= 0;			my $minpdtbimpcontingency	= 999999;
	my $maxpdtbimptemporal		= 0;			my $minpdtbimptemporal		= 999999;
	my $maxpdtbimpcontrast		= 0;			my $minpdtbimpcontrast		= 999999;
	my $maxpdtbimpall 			= 0;			my $minpdtbimpall 			= 999999;
	
	## Coursewise max-mins
	my %maxnum_urlref			= ();	my %minnum_urlref	= ();
	my %maxnum_timeref			= ();	my %minnum_timeref	= ();
	my %maxnum_urlreffirstpost	= ();	my %minnum_urlreffirstpost	= ();
	my %maxnum_timereffirstpost = ();	my %minnum_timereffirstpost	= ();
	
	my %maxthreadlength			= ();	my %minthreadlength		= ();
	
	my %maxnumsentences 		= ();	my %minnumsentences 	= ();
	my %maxavgnumsentences		= ();	my %minavgnumsentences	= ();
	my %maxnumsentences_first 	= (); 	my %minnumsentences_first = ();
	
	my %maxaffirmatives		= ();			my %minaffirmatives  = ();
	
	my %maxcoursematerail	= ();			my %mincoursematerail = ();
	my %maxcoursematerail_nkd = ();		my %mincoursematerail_nkd = ();
	my %maxcoursematerail_sfx = ();		my %mincoursematerail_sfx = ();
	my %maxcoursematerail_pfx = ();		my %mincoursematerail_pfx = ();

	my %maxcoursemateraildensity 	 = ();		my %mincoursemateraildensity 	 = ();
	my %maxcoursemateraildensity_nkd = ();		my %mincoursemateraildensity_nkd = ();
	my %maxcoursemateraildensity_sfx = ();		my %mincoursemateraildensity_sfx = ();
	my %maxcoursemateraildensity_pfx = ();		my %mincoursemateraildensity_pfx = ();
	
	my %maxaffir				= ();		my %minaffir				= ();
	my %maxaffirdensity			= ();		my %minaffirdensity 		= ();
	my %maxagreedisagree 		= ();		my %minagreedisagree 		= ();
	my %maxagreedisagreedensity	= ();		my %minagreedisagreedensity	= ();
	
	open (LOG,">$path/bad_threads.log")
				or die "cannot open a log file at $path";

	# load termIDFs to memory
	my $terms;
	my $term_course;
	my $termsstem;

	if($unigrams){
		$terms	=	Model::getalltermIDF($dbh,$freqcutoff,0,$corpus);
		#sanity check
		if (keys %{$terms} == 0 ){
			print "Exception: termIDFs are empty for $corpus_type. Check the tables and the query!\n @$corpus";
			exit(0);
		}
	}
	
	# load TFs to memory
	my %termFrequencies = ();
	if($unigrams){
		foreach my $category_id (keys %$threadcats){
			my $tftab	=	$threadcats->{$category_id}{'tftab'};
			
			my %termFrequencies_part = %{Model::getalltfs($dbh,$tftab,$course_samples,$terms,$stem,$term_length)};

			#sanity check
			if (keys %termFrequencies_part == 0){
				print "\n-Exception: TFs are empty";
				exit(0);
			}
			
			foreach my $courseid (keys %termFrequencies_part ){
				foreach my $threadid (keys %{$termFrequencies_part{$courseid}} ){
					foreach my $termid (keys %{$termFrequencies_part{$courseid}{$threadid}} ){
						if (!exists $termFrequencies{$courseid}{$threadid}{$termid}){
							$termFrequencies{$courseid}{$threadid}{$termid} = 
								$termFrequencies_part{$courseid}{$threadid}{$termid};
						}
						else{
							$termFrequencies{$courseid}{$threadid}{$termid} += 
								$termFrequencies_part{$courseid}{$threadid}{$termid};
						}
					}
					if (keys %{$termFrequencies{$courseid}{$threadid}} == 0){
						warn "Warning: TFs are empty for $courseid \t $threadid\n";
					}						
				}
				#sanity check
				if (keys %{$termFrequencies{$courseid}} == 0){
					print "---Exception: TFs are empty for $courseid\n";
					exit(0);
				}
			}
		}
		
		#sanity check
		if (keys %termFrequencies == 0){
			print "Exception: TFs are empty\n";
			exit(0);
		}
	}
	
	my %inst_viewed_threads;
	if($viewed){
		#my $access_group_id_query 	= "select id from access_groups 
		#									where name in ('Instructor', 'Teaching Staff', 'Staff', 'Community TA')";
		#my $accessids_ref			= $dbh->selectall_arrayref($access_group_id_query,'user_id')
		#									or die "query failed: $access_group_id_query \n $DBI::errstr";
		#my $accessids				= @$accessids_ref;
		my $dbhmysql		 			= Model::getDBHandle(undef,1,'mysql',$mysqldbname);
		my $inst_viewed_threads_local 	= getInstViewedThreads($dbhmysql);
		
		foreach my $id (keys %$inst_viewed_threads_local){         #item_id, user_id, timestamp
			$inst_viewed_threads{ $inst_viewed_threads_local->{$id}{'item_id'} } =  $inst_viewed_threads_local->{$id}{'timestamp'};
			#print "\n $id \t $inst_viewed_threads->{$id}{'item_id'} \t $inst_viewed_threads->{$id}{'user_id'}\t $inst_viewed_threads->{$id}{'timestamp'}";
		}
	}
	
	# First pass (applicable for features that require normalisation)
	foreach my $category_id (keys %$threadcats){
		
		my $pdtbout = undef;
		
		my $posttable		=	$threadcats->{$category_id}{'post'};
		my $commenttable	=	$threadcats->{$category_id}{'comment'};
		my $threads			=	$threadcats->{$category_id}{'threads'};
		
		if (!$lexical && !$lengthf){ 
			print $log "\n Skipping $category_id due to no lexical lengthf or time feature calculations"; 
			next; 
		}
		
		my $postqry = "select id,post_text,original from $posttable 
								where thread_id = ? and courseid = ? 
								order by id";
		my $cmntqry = "select id,comment_text from $commenttable 
								where thread_id = ? and courseid = ? and post_id = ? 
								order by id";
		
		my $poststh = $dbh->prepare($postqry) 
								or die " prepare for $postqry failed \n";
		my $cmntsth = $dbh->prepare($cmntqry)
								or die " prepare for $cmntqry failed \n";
		
		my $posttimesth = $dbh->prepare("select p.id,p.post_time from $posttable p, user u
											where p.thread_id = ? and p.courseid = ?
											and (u.user_title not in (\"Instructor\",\"Staff\",\"Coursera Staff\", \"Community TA\",\"Coursera Tech Support\")
												or u.user_title is null)
											and u.threadid = p.thread_id
											and u.courseid = p.courseid
											and u.forumid = p.forumid
											and u.postid = p.id
											order by 1") or die $dbh->errstr;
		my $commenttimesth= $dbh->prepare("select p.id,p.post_time from $commenttable p, user u
											where p.thread_id = ? and p.courseid = ?
											and (u.user_title not in (\"Instructor\",\"Staff\",\"Coursera Staff\", \"Community TA\",\"Coursera Tech Support\")
												or u.user_title is null)
											and u.threadid = p.thread_id
											and u.courseid = p.courseid
											and u.forumid = p.forumid
											and u.postid = p.id
											order by 1") or die $dbh->errstr;
		
		foreach (@$threads){
			my $threadid			= $_->[0];
			my $docid 				= $_->[1];
			my $courseid			= $_->[2];
			my $label				= $_->[3];
			my $forumid_number		= $_->[5];
	
			#my $num_threads = $num_threads_coursewise->{$courseid};
			my $num_interventions = $num_interventions_coursewise->{$courseid};

			#traverse post by post to aggregate post level features 
			$poststh->execute($threadid, $courseid) 
							or die "failed to execute $postqry";
			my $posts = $poststh->fetchall_hashref('id');
			
			#skip thread that do not have any posts
			if ( (keys %$posts) == 0 ) { 
				print LOG "\n cat-$category_id tab-$posttable cmttab-$commenttable";
				print LOG "Empty thread: $courseid $threadid $docid \n"; 
				next;	
			}
			
			$threadPostlength{$docid} = 
				@{$dbh->selectcol_arrayref("select count(id) from $posttable where thread_id = $threadid and courseid =\'$courseid\'")}[0];
			$threadCommentlength{$docid} = 
				@{$dbh->selectcol_arrayref("select count(id) from $commenttable where thread_id = $threadid and courseid =\'$courseid\'")}[0];
			$numposts{$docid} = $threadPostlength{$docid} + $threadCommentlength{$docid};
			
			if($numposts{$docid} == 0 || $threadPostlength{$docid} == 0){
				print "\n Sanity check failed! $numposts{$docid} \t " . (keys %$posts);
				print $log "\n Sanity check failed! $numposts{$docid} \t " . (keys %$posts); exit(0);
			}
			
			$thread_length{$docid} = @{$dbh->selectcol_arrayref("select sum(length(post_text)) from $posttable where 
																thread_id = $threadid and courseid = \'$courseid\'")}[0];
			my $num_comment_words = @{$dbh->selectcol_arrayref("select sum(length(comment_text)) from $commenttable where 
															thread_id = $threadid and courseid = \'$courseid\'")}[0];
			if(defined $num_comment_words){	
				$thread_length{$docid} += $num_comment_words;
			}
			
			# log thread id and skip this thread/document
			if ( $thread_length{$docid} == 0 ){ 
				print LOG "Empty thread: $docid $courseid $threadid \n"; 
				next;
			}
		
			if($pdtb_exp){
				#initialization
				my @relations = ('expansion','contingency','temporal','comparison');
				
				#skip removed files. These are files that failed to be parsed by the PDTB parser
				if(exists $removed_files->{$courseid}{$threadid}){
					next;
				}
				
				open (my $SENSE_FILE, "<$pdtbfilepath/$courseid"."_pdtbinput/"."$forumid_number/output/$threadid".".txt.exp2.out")
					or die "\n Cannot open file spans at $pdtbfilepath/$courseid"."_pdtbinput/"."$forumid_number/output/$threadid".".txt.exp2.out \n $!";
				
				$pdtbrelation{$docid}{'expansion'}		= 0;
				$pdtbrelation{$docid}{'contingency'}	= 0;
				$pdtbrelation{$docid}{'temporal'}		= 0;
				$pdtbrelation{$docid}{'contrast'} 		= 0;
				$pdtbrelation{$docid}{'all'} 			= 0;
				$pdtbrelation{$docid}{'biall'}			= 0;
				
				#initialization of bi relations to 0
				foreach my $relation1 (@relations){
					foreach my $relation2 (@relations){
						$pdtbrelation{$docid}{$relation1.$relation2."den"}	= 0;
					}
				}
				
				my $prev_sense = undef;
				while (my $line = <$SENSE_FILE>){
					my @fields = split /\s+/,$line;
					my $relation_post = $fields[0];
					$fields[5] = lc($fields[5]);
					
					#skips relations from posts not in truncated intervened threads
					if(!exists $posts->{$relation_post}){ next; } 
					
					if ($fields[5] eq 'expansion'){
						$pdtbrelation{$docid}{'expansion'} ++;
					}
					elsif ($fields[5] eq 'contingency'){
						$pdtbrelation{$docid}{'contingency'} ++;
					}
					elsif ($fields[5] eq 'temporal'){
						$pdtbrelation{$docid}{'temporal'} ++;
					}
					elsif ($fields[5] eq 'comparison'){
						$pdtbrelation{$docid}{'contrast'} ++;
					}
					else{
						print "\n Unknown pdtb relation  $fields[5] in post $fields[0] $threadid of forum $forumid_number";
						exit(0);
					}
					$pdtbrelation{$docid}{'all'}++;
					
					if(defined $prev_sense){
						$pdtbrelation{$docid}{ $prev_sense.$fields[5]."den"}++;
						$pdtbrelation{$docid}{'biall'}++;
					}
					$prev_sense = $fields[5];
				}
				close $SENSE_FILE;
				
				#normalisation by sum of number of posts and comments
				foreach my $relation (sort keys %{$pdtbrelation{$docid}}){
					$pdtbrelation{$docid}{$relation} = 
						$pdtbrelation{$docid}{$relation}  / $numposts{$docid};
				}
				
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
					$pdtbrelation{$docid}{'expden'}			= 0;
					$pdtbrelation{$docid}{'contden'}		= 0;
					$pdtbrelation{$docid}{'tempden'}		= 0;
					$pdtbrelation{$docid}{'compden'}		= 0;
				}
			}
			
			if($pdtb_imp){
				#initialization
				my @relations = ('expansion','contingency','temporal','comparison','norel','entrel');
				
				#skip removed files. These are files that failed to be parsed by the PDTB parser
				if(exists $removed_files->{$courseid}{$threadid}){
					next;
				}
				
				open (my $SENSE_FILE, "<$pdtbfilepath/$courseid"."_pdtbinput/"."$forumid_number/output/$threadid".".txt.nonexp2.out")
					or die "\n Cannot open file spans at $pdtbfilepath/$courseid"."_pdtbinput/"."$forumid_number/output/$threadid".".txt.nonexp2.out \n $!";
				
				$pdtbrelation_imp{$docid}{'expansion'}		= 0;
				$pdtbrelation_imp{$docid}{'contingency'}	= 0;
				$pdtbrelation_imp{$docid}{'temporal'}		= 0;
				$pdtbrelation_imp{$docid}{'contrast'} 		= 0;
				
				$pdtbrelation_imp{$docid}{'all'} 			= 0;
				$pdtbrelation_imp{$docid}{'biall'}			= 0;
				
				#initialization of bi relations to 0
				foreach my $relation1 (@relations){
					foreach my $relation2 (@relations){
						$pdtbrelation_imp{$docid}{$relation1.$relation2."den"}	= 0;
					}
				}
				
				my $prev_sense = undef;
				while (my $line = <$SENSE_FILE>){
					my @fields = split /\s+/,$line;
					my $relation_post = $fields[0];
					$fields[7] = lc($fields[7]);
					
					#skips relations from posts not in truncated intervened threads
					if(!exists $posts->{$relation_post}){ next; } 
					
					#skip entity relations as they are not discourse connectives
					if ($fields[7] eq 'entrel' || $fields[7] eq 'norel'){ next; }
					
					if ($fields[7] eq 'expansion'){
						$pdtbrelation_imp{$docid}{'expansion'} ++;
					}
					elsif ($fields[7] eq 'contingency'){
						$pdtbrelation_imp{$docid}{'contingency'} ++;
					}
					elsif ($fields[7] eq 'temporal'){
						$pdtbrelation_imp{$docid}{'temporal'} ++;
					}
					elsif ($fields[7] eq 'comparison'){
						$pdtbrelation_imp{$docid}{'contrast'} ++;
					}
					else{
						print "\n Unknown pdtb relation  $fields[7] in post $fields[0] $threadid of forum $forumid_number";
						exit(0);
					}
					$pdtbrelation_imp{$docid}{'all'}++;
					
					if(defined $prev_sense){
						$pdtbrelation_imp{$docid}{$prev_sense.$fields[7]."den"}++;
						$pdtbrelation_imp{$docid}{'biall'}++;
					}
					$prev_sense = $fields[7];
				}
				close $SENSE_FILE;
				
				#normalisation by sum of number of posts and comments
				foreach my $relation (sort keys %{$pdtbrelation_imp{$docid}}){
					$pdtbrelation_imp{$docid}{$relation} = 
						$pdtbrelation_imp{$docid}{$relation}  / $numposts{$docid};
				}
				
				# make pdtb densities: birelations
				if($pdtbrelation_imp{$docid}{'biall'} > 0){
					foreach my $relation (sort keys %{$pdtbrelation_imp{$docid}}){
						if(	$relation =~ /den$/){
							$pdtbrelation_imp{$docid}{ $relation} = 
							$pdtbrelation_imp{$docid}{ $relation }/$pdtbrelation_imp{$docid}{'biall'};
						}
					}
				}
				
				# make pdtb densities: uni relations
				if($pdtbrelation_imp{$docid}{'all'} > 0){
					$pdtbrelation_imp{$docid}{'expden'}		= $pdtbrelation_imp{$docid}{'expansion'}	/$pdtbrelation_imp{$docid}{'all'};
					$pdtbrelation_imp{$docid}{'contden'}	= $pdtbrelation_imp{$docid}{'contingency'} /$pdtbrelation_imp{$docid}{'all'};
					$pdtbrelation_imp{$docid}{'tempden'}	= $pdtbrelation_imp{$docid}{'temporal'}	/$pdtbrelation_imp{$docid}{'all'};
					$pdtbrelation_imp{$docid}{'compden'}	= $pdtbrelation_imp{$docid}{'contrast'}	/$pdtbrelation_imp{$docid}{'all'};	
				}
				else{
					$pdtbrelation_imp{$docid}{'expden'}			= 0;
					$pdtbrelation_imp{$docid}{'contden'}		= 0;
					$pdtbrelation_imp{$docid}{'tempden'}		= 0;
					$pdtbrelation_imp{$docid}{'compden'}		= 0;
				}
			}
			
			foreach my $post ( sort {$a <=> $b} keys %$posts){
				my $postText = $posts->{$post}{'post_text'};
				$postText = Preprocess::replaceURL($postText);
				$postText = Preprocess::replaceMath($postText);
				$postText = Preprocess::replaceTimeReferences($postText);

				if($numsentences){
					my $postSentences = Preprocess::getSentences($postText);
					if( defined $postSentences ){ 
						$numsentences{$docid} += @$postSentences;
					}
					
					#num sentences in the original / first post of the thread
					if ( defined $postSentences && $posts->{$post}{'original'} ){
						$numsentences_first{$docid} = @$postSentences;
					}
				}		
				if($nonterm_courseref){
					my $urlref = getnumURLref($postText);
					my $timeref = getnumTimeref($postText);
					my $equation = hasEquation($postText);
					
					if ( $posts->{$post}{'original'} ){
						$num_urlrefinfirstpost{$docid} = $urlref;
						$num_timereffirstpost{$docid} = $timeref;
						$hasEquationfirstpost{$docid} = $equation;
					}
					
					$num_urlref{$docid} += $urlref;
					$num_timeref{$docid} += $timeref;
					$hasEquation{$docid} += $equation;
				}
				if($courseref){
					my ($prefixcount,$suffixcount,$nakedcount,$allcount) 
										= getCourseMaterialMentions($postText);
					$coursematerialterms_pfx{$docid} += $prefixcount;
					$coursematerialterms_sfx{$docid} += $suffixcount;
					$coursematerialterms_nkd{$docid} += $nakedcount;
					$coursematerialterms{$docid} += $allcount;
				}
				if($affir){
					$affirmations{$docid}  += getAffirmations($postText);
				}
				if($agree){
					$agreedisagree{$docid} += getAgreeDisagree($postText);
				}
				
				$cmntsth->execute($threadid, $courseid, $post) or die "failed to execute $cmntqry";
				my $comments = $cmntsth->fetchall_hashref('id');
				foreach my $comment ( sort {$a <=> $b} keys %$comments){
					my $commentText = $comments->{$comment}{'comment_text'};
					$commentText = Preprocess::replaceURL($commentText);
					$commentText = Preprocess::replaceMath($commentText);
					$commentText = Preprocess::replaceTimeReferences($commentText);
					
					if($numsentences){
						my $commentSentences = Preprocess::getSentences($commentText);
						if(defined $commentSentences ){ 
							$numsentences{$docid} += @$commentSentences;
						}
					}
					if($nonterm_courseref){
						my $urlref = getnumURLref($commentText);
						my $timeref = getnumTimeref($commentText);
						my $equation = hasEquation($commentText);

						$num_urlref{$docid} += $urlref;
						$num_timeref{$docid} += $timeref;
						$hasEquation{$docid} += $equation;
					}
					if($courseref){
						my ($prefixcount,$suffixcount,$nakedcount,$allcount) 
									= getCourseMaterialMentions($commentText);
						$coursematerialterms_pfx{$docid} += $prefixcount;
						$coursematerialterms_sfx{$docid} += $suffixcount;
						$coursematerialterms_nkd{$docid} += $nakedcount;
						$coursematerialterms{$docid} += $allcount;
					}
					if($affir){
						$affirmations{$docid}  += getAffirmations($commentText);
					}
					if($agree){
						$agreedisagree{$docid} += getAgreeDisagree($commentText);
					}
				}
			}
			
			my $thread_length_nomalizer = $thread_length{$docid};

			if($pdtb_exp){
				($maxpdtbexpansion, $minpdtbexpansion) 
					= 	getMaxMin(	$pdtbrelation{$docid}{'expansion'},
									$maxpdtbexpansion,
									$minpdtbexpansion,
									"pdtb_expansion"
								 );
				($maxpdtbcontingency, $minpdtbcontingency) 
					= 	getMaxMin(	$pdtbrelation{$docid}{'contingency'},
									$maxpdtbcontingency,
									$minpdtbcontingency,
									"pdtb_contingency"
								 );
				($maxpdtbtemporal, $minpdtbtemporal) 
					= 	getMaxMin(	$pdtbrelation{$docid}{'temporal'} ,
									$maxpdtbtemporal,
									$minpdtbtemporal,
									"pdtb_temporal"
								 );
				($maxpdtbcontrast, $minpdtbcontrast) 
					= 	getMaxMin(	$pdtbrelation{$docid}{'contrast'} ,
									$maxpdtbcontrast,
									$minpdtbcontrast,
									"pdtb_contrast"
								 );
								 
				($maxpdtball, $minpdtball) 
					= 	getMaxMin(	$pdtbrelation{$docid}{'all'} ,
									$maxpdtball,
									$minpdtball,
									"pdtb_all"
								 );					
			}
			
			if($pdtb_imp){
				($maxpdtbimpexpansion, $minpdtbimpexpansion) 
					= 	getMaxMin(	$pdtbrelation_imp{$docid}{'expansion'},
									$maxpdtbimpexpansion,
									$minpdtbimpexpansion,
									"pdtb_expansion"
								 );
				($maxpdtbimpcontingency, $minpdtbimpcontingency) 
					= 	getMaxMin(	$pdtbrelation_imp{$docid}{'contingency'},
									$maxpdtbimpcontingency,
									$minpdtbimpcontingency,
									"pdtb_contingency"
								 );
				($maxpdtbimptemporal, $minpdtbimptemporal) 
					= 	getMaxMin(	$pdtbrelation_imp{$docid}{'temporal'} ,
									$maxpdtbimptemporal,
									$minpdtbimptemporal,
									"pdtb_temporal"
								 );
				($maxpdtbimpcontrast, $minpdtbimpcontrast) 
					= 	getMaxMin(	$pdtbrelation_imp{$docid}{'contrast'} ,
									$maxpdtbimpcontrast,
									$minpdtbimpcontrast,
									"pdtb_contrast"
								 );
								 
				($maxpdtbimpall, $minpdtbimpall) 
					= 	getMaxMin(	$pdtbrelation_imp{$docid}{'all'} ,
									$maxpdtbimpall,
									$minpdtbimpall,
									"pdtb_imp_all"
								 );					
			}
			
			if($numw){
				#log transformation to prevent loss of precision
				if ( $thread_length{$docid} != 0){
					$thread_length{$docid} = log ($thread_length{$docid}) / log(10);
				}
				$maxthreadlength = ($thread_length{$docid} > $maxthreadlength)? $thread_length{$docid} : $maxthreadlength;
				$minthreadlength = ($thread_length{$docid} < $minthreadlength)? $thread_length{$docid} : $minthreadlength;
			}
			
			if($numsentences && defined $numsentences{$docid}){
					($maxnumsentences, $minnumsentences) =	getMaxMin( $numsentences{$docid},
																	   $maxnumsentences,
																	   $minnumsentences,
																	   "num_sentences"
																	 );
					
					$avgnumsentences{$docid} = $numsentences{$docid} / $numposts{$docid};
					($maxavgnumsentences, $minavgnumsentences) =	getMaxMin( $avgnumsentences{$docid},
																				$maxavgnumsentences,
																				$minavgnumsentences,
																				"num_sentences"
																			  );
																	 
					if (defined $numsentences_first{$docid}){
						($maxnumsentences_first, $minnumsentences_first) =	getMaxMin( $numsentences_first{$docid},
																						$maxnumsentences_first,
																						$minnumsentences_first,
																						"num_sentences_first"
																					 );
					}
			}
		
			if($courseref ){
				if ($thread_length_nomalizer eq 0){
					print $log "Warning:  $docid \t $courseid: $coursematerialterms{$docid} \n";
				}
				
				if ( defined $coursematerialterms{$docid} ){
						$coursematerialtermdensity{$docid} 	=	$coursematerialterms{$docid} / 
																			$thread_length_nomalizer;
						$coursematerialtermdensity_nkd{$docid} = $coursematerialterms_nkd{$docid} / 
																			$thread_length_nomalizer;
						$coursematerialtermdensity_pfx{$docid} = $coursematerialterms_pfx{$docid} / 
																			$thread_length_nomalizer;
						$coursematerialtermdensity_sfx{$docid} = $coursematerialterms_sfx{$docid} / 
																			$thread_length_nomalizer;
				
						($maxcoursemateraildensity, $mincoursemateraildensity) = 
																getMaxMin(  $coursematerialtermdensity{$docid},
																			$maxcoursemateraildensity,
																			$mincoursemateraildensity,
																			"courserefdensity"
																		 );
																	 
						($maxcoursemateraildensity_nkd, $mincoursemateraildensity_nkd) = 
																getMaxMin(  $coursematerialtermdensity_nkd{$docid},
																			$maxcoursemateraildensity_nkd,
																			$mincoursemateraildensity_nkd,
																			"courserefden_nkd"
																		 );
																		 
						($maxcoursemateraildensity_pfx, $mincoursemateraildensity_pfx) = 
																getMaxMin(  $coursematerialtermdensity_pfx{$docid},
																			$maxcoursemateraildensity_pfx,
																			$mincoursemateraildensity_pfx,
																			"courserefden_pfx"
																		 );
																 
						($maxcoursemateraildensity_sfx, $mincoursemateraildensity_sfx) = 
																getMaxMin(  $coursematerialtermdensity_sfx{$docid},
																			$maxcoursemateraildensity_sfx,
																			$mincoursemateraildensity_sfx,
																			"courserefden_sfx"
																		 );
																		 
						($maxcoursematerail, $mincoursematerail) =
																getMaxMin(  $coursematerialterms{$docid},
																			$maxcoursematerail,
																			$mincoursematerail,
																			"courseref"
																		 );
																 
						($maxcoursematerail_nkd, $mincoursematerail_nkd) =
																getMaxMin(  $coursematerialterms_nkd{$docid},
																			$maxcoursematerail_nkd,
																			$mincoursematerail_nkd,
																			"courseref_nkd"
																		 );
																	 
						($maxcoursematerail_pfx, $mincoursematerail_pfx) =
																getMaxMin(  $coursematerialterms_pfx{$docid},
																			$maxcoursematerail_pfx,
																			$mincoursematerail_pfx,
																			"courseref_pfx"
																		 );
																 
						($maxcoursematerail_sfx, $mincoursematerail_sfx) =
																getMaxMin(  $coursematerialterms_sfx{$docid},
																			$maxcoursematerail_sfx,
																			$mincoursematerail_sfx,
																			"courseref_sfx"
																		 );
				}
			}
			
			if(defined $nonterm_courseref){
				if(defined $num_urlref{$docid} ){
					($maxnum_urlref, $minnum_urlref) =	getMaxMin(  $num_urlref{$docid},
																	$maxnum_urlref,
																	$minnum_urlref,
																	"courseref_url"
																 );
				}
			
				if(defined $num_urlrefinfirstpost{$docid} ){
					($maxnum_urlreffirstpost, $minnum_urlreffirstpost) = 
														getMaxMin(	$num_urlrefinfirstpost{$docid},
																	$maxnum_urlreffirstpost,
																	$minnum_urlreffirstpost,
																	"courseref_url_first"
																 );
				}
			
				if(defined $num_timeref{$docid} ){

					($maxnum_timeref, $minnum_timeref)  =	getMaxMin(  $num_timeref{$docid},
																		$maxnum_timeref,
																		$minnum_timeref,
																		"courseref_time"
																	 );
				}
			
				if(defined $num_timereffirstpost{$docid} ){

					($maxnum_timereffirstpost, $minnum_timereffirstpost) 
														=	getMaxMin(  $num_timereffirstpost{$docid},
																		$maxnum_timereffirstpost,
																		$minnum_timereffirstpost,
																		"courseref_time_first"
																	 );
				}
			}
			
			if($affir && defined $affirmations{$docid} ){
				$affirtermdensity{$docid} = $affirmations{$docid}/$thread_length_nomalizer;

					$maxaffir = ($affirmations{$docid} > $maxaffir)?$affirmations{$docid}: $maxaffir;
					$minaffir = ($affirmations{$docid} < $minaffir)?$affirmations{$docid}: $minaffir;
				
					$maxaffirdensity = ($affirtermdensity{$docid} > $maxaffirdensity)? $affirtermdensity{$docid}: $maxaffirdensity;
					$minaffirdensity = ($affirtermdensity{$docid} < $minaffirdensity)? $affirtermdensity{$docid}: $minaffirdensity;
			}
			
			if($agree && defined $agreedisagree{$docid} ){
				$agreedisagreedensity{$docid} = $agreedisagree{$docid}/$thread_length_nomalizer;
								
				$maxagreedisagree = ($agreedisagree{$docid} > $maxagreedisagree)?$agreedisagree{$docid}: $maxagreedisagree;
				$minagreedisagree = ($agreedisagree{$docid} < $minagreedisagree)?$agreedisagree{$docid}: $minagreedisagree;
				
				$maxagreedisagreedensity = ($agreedisagreedensity{$docid} > $maxagreedisagreedensity)? $agreedisagreedensity{$docid}: $maxagreedisagreedensity;
				$minagreedisagreedensity = ($agreedisagreedensity{$docid} < $minagreedisagreedensity)? $agreedisagreedensity{$docid}: $minagreedisagreedensity;
			}
		}
		
		#close $pdtbout;
	}
	# first pass ends

	if($numw){
		print "MAX & MIN thread lengths: $maxthreadlength \t $minthreadlength\n";
		checkmaxminexception($maxthreadlength , $minthreadlength, 'number of words');
	}
	
	if($numsentences){
		print "MAX & MIN numsentences: $maxnumsentences \t $minnumsentences\n";
		print "MAX & MIN avg numsentences: $maxavgnumsentences \t $minavgnumsentences\n";
		checkmaxminexception($maxnumsentences , $minnumsentences, 'number of sentences');
		checkmaxminexception($maxavgnumsentences , $minavgnumsentences, 'avg number of sentences');
		checkmaxminexception($maxnumsentences_first , $minnumsentences_first, 'number of sentences first');
	}
	
	if($courseref){
		print "MAX & MIN course material mentions: $maxcoursematerail \t $mincoursematerail\n";
		print "MAX & MIN avg course material density: $maxcoursemateraildensity \t $mincoursemateraildensity\n";
		checkmaxminexception($maxcoursematerail, $mincoursematerail, 'course references');
		checkmaxminexception($maxcoursematerail_nkd, $mincoursematerail_nkd, 'course references nkd');
		checkmaxminexception($maxcoursematerail_sfx, $mincoursematerail_sfx, 'course references sfx');
		checkmaxminexception($maxcoursematerail_pfx, $mincoursematerail_pfx, 'course references pfx');
		checkmaxminexception($maxcoursemateraildensity, $mincoursemateraildensity, 'avg course references');
		checkmaxminexception($maxcoursemateraildensity_nkd, $mincoursemateraildensity_nkd, 'avg course references nkd');
		checkmaxminexception($maxcoursemateraildensity_sfx, $mincoursemateraildensity_sfx, 'avg course references sfx');
		checkmaxminexception($maxcoursemateraildensity_pfx, $mincoursemateraildensity_pfx, 'avg course references pfx');
	}
	
	if($nonterm_courseref){	
		checkmaxminexception($maxnum_urlref, $minnum_urlref, 'urlref');
		checkmaxminexception($maxnum_timeref, $minnum_timeref, 'timeref');
		checkmaxminexception($maxnum_timereffirstpost, $minnum_timereffirstpost, 'urlreffirst');
		checkmaxminexception($maxnum_urlreffirstpost, $minnum_urlreffirstpost, 'timereffirst');
	}
	
	if($affir){
		print "MAX & MIN affir mentions: $maxaffir \t $minaffir\n";
		print "MAX & MIN affir density : $maxaffirdensity \t $minaffirdensity\n";
		checkmaxminexception($maxaffir, $minaffir, 'affirmations');
		checkmaxminexception($maxaffirdensity, $minaffirdensity, 'affirmation dentsity');
	}

	if($pdtb_exp){
		print "MAX & MIN pdtb e: $maxpdtbexpansion \t $minpdtbexpansion\n";
		print "MAX & MIN pdtb c: $maxpdtbcontingency \t $minpdtbcontingency\n";
		print "MAX & MIN pdtb com: $maxpdtbcontrast \t $minpdtbcontrast\n";
		print "MAX & MIN pdtb tem: $maxpdtbtemporal \t $minpdtbtemporal\n";
	}

	if($pdtb_imp){
		print "MAX & MIN pdtb e: $maxpdtbimpexpansion \t $minpdtbimpexpansion\n";
		print "MAX & MIN pdtb c: $maxpdtbimpcontingency \t $minpdtbimpcontingency\n";
		print "MAX & MIN pdtb com: $maxpdtbimpcontrast \t $minpdtbimpcontrast\n";
		print "MAX & MIN pdtb tem: $maxpdtbimptemporal \t $minpdtbimptemporal\n";
	}
	
	my %nontermfeatures = ();
	my $maxtermfeaturecount = 0;

	if($unigrams){	
		# find maxnumber of unigram features.
		foreach my $category_id (keys %$threadcats){
			my $tftab			 =	$threadcats->{$category_id}{'tftab'};
			my $max_termid 		+= @{$dbh->selectcol_arrayref("select max(termid) from $tftab")}[0];
			$maxtermfeaturecount = ($max_termid > $maxtermfeaturecount) ? $max_termid : $maxtermfeaturecount;
		}
	}
	print "\n Maxtermfeaturecount: $maxtermfeaturecount";
	
	# compute tf-IDFs
	my $termWeights;
	if($unigrams){
			$termWeights = computeTFIDFs(	\%termFrequencies,
											$terms, 	## sends per course IDF weights 
											$term_course,
											$total_num_threads,
											$corpus_type,
											$dbh,
											$tftype	#uses normalised tf without idf
										);	
		if (keys %{$termWeights} ==0 ){
			print "\n Exception... termweights matrix is empty ";
			print $log "\n Exception... termweights matrix is empty "; exit(0);
		}
	}
	
	# will store term vectors from both +ve adn -ve thread categories
	my %termvectors_collated = ();
	
	# Second pass
	foreach my $category_id (keys %$threadcats){
		my $posttable		=	$threadcats->{$category_id}{'post'};
		my $commenttable	=	$threadcats->{$category_id}{'comment'};
		my $threads			=	$threadcats->{$category_id}{'threads'};

		my $postqry = "select id,post_text,original from $posttable 
								where thread_id = ? and courseid = ? 
								order by id";
		my $cmntqry = "select id,comment_text from $commenttable 
								where thread_id = ? and courseid = ? and post_id = ? 
								order by id";
			
		my $poststh = $dbh->prepare($postqry) 
							or die " prepare for $postqry failed \n";
		my $cmntsth = $dbh->prepare($cmntqry) 
							or die " prepare for $cmntqry failed \n";		
		
		if (!defined $threads || scalar @$threads == 0){
			print $log "\n Exception: Second pass! No threads found $category_id";
			print "\n Exception: Second pass! No threads found $category_id";
			exit(0);
		}
		
		print $FEXTRACT "\n Writing feature file for potentially ". scalar(@$threads).
												" threads\n";
		foreach (@$threads){
			my $threadid			= $_->[0];
			my $docid				= $_->[1];
			my $courseid			= $_->[2];
			my $label				= $_->[3];
			my $forumname			= $_->[4];
			my $forumid_number		= $_->[5];
			my $serialid			= $_->[6];
			
			my $num_threads 		= $num_threads_coursewise->{$courseid};
			my $num_interventions	= $num_interventions_coursewise->{$courseid};
			
			if(!defined $threadid){
				print "\n $category_id $posttable $commenttable $threads";
				exit(0);
			}
			
			$poststh->execute($threadid, $courseid) 
							or die "failed to execute $postqry";
			my $posts = $poststh->fetchall_hashref('id');
			
			#skip thread that do not have any posts
			if ( (keys %$posts) == 0 ) {
				print LOG "\n cat-$category_id tab-$posttable cmttab-$commenttable";
				print LOG "\n Empty thread: $courseid $threadid $docid $forumname \n"; 
				next;	
			}
			
			print $FEXTRACT "\n Writing feature file for thread: $courseid $forumname $threadid $docid $label";
			my $isfirstpostquestion = 0;
			
			my $sum_of_squares		= 0;
			
			my $term_vector;
			
			if(!defined $termFrequencies{$courseid}{$threadid}){
				print LOG "\n Warning unigrams are undef for $threadid \t $courseid \t $docid \t $posttable \t #posts ". (keys %$posts);
			}
			elsif(keys %{$termFrequencies{$courseid}{$threadid}} == 0){
				print LOG "\n Warning unigrams are empty for $threadid \t $courseid\t $docid  \t $posttable \t #posts ". (keys %$posts);
			}
			
			if($unigrams){
				$term_vector = fetchTermVector($termWeights->{$courseid}{$threadid}, $debug);

				if ( keys %$term_vector == 0 ){
					print LOG "term_vector is empty! for $threadid in $courseid\n";
					next;
				}
			}
			
			my $nontermfeaturecount = $maxtermfeaturecount;
			
			if($tprop){
				print $log "adding thread length feature..\n";
				if ($threadPostlength{$docid} == 0 && $numposts{$docid} > 0){
					print "\n Sanity check failed! Exiting...";
					exit(0);
				}
				#posts in the thread
				$nontermfeaturecount++;
				$term_vector->{$nontermfeaturecount} = $threadPostlength{$docid};
				$sum_of_squares += ($threadPostlength{$docid} * $threadPostlength{$docid});
				$nontermfeatures{$nontermfeaturecount} = 'tlen:#posts in thread';
				
				#comments in the thread
				$nontermfeaturecount++;
				$term_vector->{$nontermfeaturecount} = $threadCommentlength{$docid};
				$sum_of_squares += ($threadCommentlength{$docid} * $threadCommentlength{$docid});
				$nontermfeatures{$nontermfeaturecount} = 'tlen:#comments in thread';
				
				#posts + #comments in the thread
				$nontermfeaturecount++;
				$term_vector->{$nontermfeaturecount} = $numposts{$docid};
				$sum_of_squares += ($numposts{$docid} * $numposts{$docid});
				$nontermfeatures{$nontermfeaturecount} = 'tlen:#posts+#comments';
				
				#comments per post
				$nontermfeaturecount++;
				my $commentsperpost = $threadCommentlength{$docid}/$threadPostlength{$docid};
				$term_vector->{$nontermfeaturecount} = $commentsperpost;
				$sum_of_squares += ($commentsperpost * $commentsperpost);
				$nontermfeatures{$nontermfeaturecount} = 'tlen:#comments to post ratio';
			}
			
			if($unigrams){
				$sum_of_squares = sumOfSquares( $term_vector );
			
				if ( $sum_of_squares == 0 || !defined $sum_of_squares ){
					print $FEXTRACT "Exception: sum_of_square is undef or zero $threadid \t $courseid \t $label \t $docid";
					print "Exception: sum_of_square is undef or zero $threadid \t $courseid \t $label \t $docid";
					exit(0);
				}
				
				#lnorm of term vector. Scales to 0 to 1 range. 
				#Turns term vector into a unit vector
				$sum_of_squares = sqrt($sum_of_squares);		
				foreach my $tid (keys %$term_vector){
					$term_vector->{$tid} = $term_vector->{$tid} / $sum_of_squares;
				}
			}

			if($viewed){
			
			}
			
		    if($pdtb_exp){
				print $log "adding pdtb relation feature..\n";
				##afterAAAI changes begin
				#if(keys %{$pdtbrelation{$docid}} eq 0){
				#	$nontermfeaturecount+=25;
				#}
				#elsif(keys %{$pdtbrelation{$docid}} < 25){
				#	die "\n Exeption: pdtb feature vector is partially empty";
				#}afterAAAI changes end
				
				foreach my $relation ( sort keys %{$pdtbrelation{$docid}} ){
					$nontermfeaturecount++;
					#print "\n $docid \t $nontermfeaturecount \t $relation";
					if($relation eq 'expansion'){
						my $max_minus_min = ($maxpdtbexpansion - $minpdtbexpansion);
						if(defined $pdtbrelation{$docid}{'expansion'} && $maxpdtbexpansion ne 0){
							my $normalised = ($pdtbrelation{$docid}{'expansion'} - $minpdtbexpansion)/$max_minus_min;
							$term_vector->{$nontermfeaturecount} = $normalised;
						}
						$nontermfeatures{$nontermfeaturecount} = 'pdtb:#Expansioninthread';
					}
					elsif($relation eq 'contingency'){
						my $max_minus_min = ($maxpdtbcontingency - $minpdtbcontingency);
						if(defined $pdtbrelation{$docid}{'contingency'} && $maxpdtbcontingency ne 0){
							my $normalised = ($pdtbrelation{$docid}{'contingency'} - $minpdtbcontingency)/$max_minus_min;
							$term_vector->{$nontermfeaturecount} = $normalised;
						}
						$nontermfeatures{$nontermfeaturecount} = 'pdtb:#Contingencyinthread';			
					}
					elsif($relation eq 'temporal'){
						my $max_minus_min = ($maxpdtbtemporal - $minpdtbtemporal);
						if(defined $pdtbrelation{$docid}{'temporal'} && $maxpdtbtemporal ne 0){
							my $normalised = ($pdtbrelation{$docid}{'temporal'} - $minpdtbtemporal)/$max_minus_min;
							$term_vector->{$nontermfeaturecount} = $normalised;
						}
						$nontermfeatures{$nontermfeaturecount} = 'pdtb:#Temporalinthread';
					}
					elsif($relation eq 'contrast'){
						my $max_minus_min = ($maxpdtbcontrast - $minpdtbcontrast);
						if(defined $pdtbrelation{$docid}{'contrast'} && $maxpdtbcontrast ne 0){
							my $normalised = ($pdtbrelation{$docid}{'contrast'} - $minpdtbcontrast)/$max_minus_min;
							$term_vector->{$nontermfeaturecount} = $normalised;
						}
						$nontermfeatures{$nontermfeaturecount} = 'pdtb:#Contrastinthread';
					}
					elsif($relation eq 'all'){
						my $max_minus_min = ($maxpdtball - $minpdtball);
						if(defined $pdtbrelation{$docid}{'all'} && $maxpdtball ne 0){
							my $normalised = ($pdtbrelation{$docid}{'all'} - $minpdtball)/$max_minus_min;
							$term_vector->{$nontermfeaturecount} = $normalised;
						}
						$nontermfeatures{$nontermfeaturecount} = 'pdtb:#all';
					}
					elsif($relation =~ /den$/){
						$term_vector->{$nontermfeaturecount}	= $pdtbrelation{$docid}{$relation};
						$nontermfeatures{$nontermfeaturecount}	= $relation;
					}
					elsif($relation eq 'biall'){
						$nontermfeaturecount--;
						next;
					}
					else{
						print $log "\n bad exp relation: $relation in $docid \t $nontermfeaturecount \t $relation";
						print "\n bad exp relation: $relation in $docid \t $nontermfeaturecount \t $relation";
						exit(0);
					}
				}
			}
	
		    if($viewed){
				$nontermfeaturecount++;
				if ( !exists $inst_viewed_threads{$threadid} ){
					$term_vector->{$nontermfeaturecount} = 0;
				}
				else{
					$term_vector->{$nontermfeaturecount} = 1;
				}
			}
			
			if($pdtb_imp){
				print $log "adding pdtb relation feature..\n";
				
				foreach my $relation ( sort keys %{$pdtbrelation_imp{$docid}} ){
					$nontermfeaturecount++;
					#print "\n $docid \t $nontermfeaturecount \t $relation";
					if($relation eq 'expansion'){
						my $max_minus_min = ($maxpdtbimpexpansion - $minpdtbimpexpansion);
						if(defined $pdtbrelation_imp{$docid}{'expansion'} && $maxpdtbimpexpansion ne 0){
							my $normalised = ($pdtbrelation_imp{$docid}{'expansion'} - $minpdtbimpexpansion)/$max_minus_min;
							$term_vector->{$nontermfeaturecount} = $normalised;
						}
						$nontermfeatures{$nontermfeaturecount} = 'pdtbimp:#Expansioninthread';
					}
					elsif($relation eq 'contingency'){
						my $max_minus_min = ($maxpdtbimpcontingency - $minpdtbimpcontingency);
						if(defined $pdtbrelation_imp{$docid}{'contingency'} && $maxpdtbimpcontingency ne 0){
							my $normalised = ($pdtbrelation_imp{$docid}{'contingency'} - $minpdtbimpcontingency)/$max_minus_min;
							$term_vector->{$nontermfeaturecount} = $normalised;
						}
						$nontermfeatures{$nontermfeaturecount} = 'pdtbimp:#Contingencyinthread';			
					}
					elsif($relation eq 'temporal'){
						my $max_minus_min = ($maxpdtbimptemporal - $minpdtbimptemporal);
						if(defined $pdtbrelation_imp{$docid}{'temporal'} && $maxpdtbimptemporal ne 0){
							my $normalised = ($pdtbrelation_imp{$docid}{'temporal'} - $minpdtbimptemporal)/$max_minus_min;
							$term_vector->{$nontermfeaturecount} = $normalised;
						}
						$nontermfeatures{$nontermfeaturecount} = 'pdtbimp:#Temporalinthread';
					}
					elsif($relation eq 'contrast'){
						my $max_minus_min = ($maxpdtbimpcontrast - $minpdtbimpcontrast);
						if(defined $pdtbrelation_imp{$docid}{'contrast'} && $maxpdtbimpcontrast ne 0){
							my $normalised = ($pdtbrelation_imp{$docid}{'contrast'} - $minpdtbimpcontrast)/$max_minus_min;
							$term_vector->{$nontermfeaturecount} = $normalised;
						}
						$nontermfeatures{$nontermfeaturecount} = 'pdtbimp:#Contrastinthread';
					}
					elsif($relation eq 'all'){
						my $max_minus_min = ($maxpdtbimpall - $minpdtbimpall);
						if(defined $pdtbrelation_imp{$docid}{'all'} && $maxpdtbimpall ne 0){
							my $normalised = ($pdtbrelation_imp{$docid}{'all'} - $minpdtbimpall)/$max_minus_min;
							$term_vector->{$nontermfeaturecount} = $normalised;
						}
						$nontermfeatures{$nontermfeaturecount} = 'pdtb:#all';
					}
					elsif($relation =~ /den$/){
						$term_vector->{$nontermfeaturecount}	= $pdtbrelation_imp{$docid}{$relation};
						$nontermfeatures{$nontermfeaturecount}	= $relation;
					}
					elsif($relation eq 'biall'){
						$nontermfeaturecount--;
						next;
					}
					else{
						print $log "\n bad imp relation: $relation in  $docid \t $nontermfeaturecount \t $relation";
						print "\n bad imp relation: $relation in $docid \t $nontermfeaturecount \t $relation";
						exit(0);
					}
				}
			}
			
			if($tlength){
				$nontermfeaturecount++;
				$term_vector->{$nontermfeaturecount}	= $numposts{$docid};
				$nontermfeatures{$nontermfeaturecount}	= 'tlen:#posts+#comments';
			}

			if($forumtype){
				print $log "\n adding forumtype feature.";
				my @forumtype_vector = @{encodeforumname($forumname)};
				foreach my $code (@forumtype_vector){
					$nontermfeaturecount++;
					if ( $code ne -1){
						$term_vector->{$nontermfeaturecount} = ($code == 1)? 1: 0;
					}
					else{
						Utility::logg($FEXTRACT, "Missing forum code $code: $forumname|$courseid | $threadid | $docid\n");
					}
					$nontermfeatures{$nontermfeaturecount} = 'forumcode';
				}
			}

			if($numw){
				print $log "\nadding num_words feature.";
				$nontermfeaturecount++;
				$nontermfeatures{$nontermfeaturecount} = 'numw:# words';
				if(defined $thread_length{$docid}){
					my $normalised = ($thread_length{$docid} - $minthreadlength)/ ($maxthreadlength - $minthreadlength);
					$term_vector->{$nontermfeaturecount} = $normalised;
				}
				else{
					$term_vector->{$nontermfeaturecount} = undef;
				}
				$nontermfeatures{$nontermfeaturecount}	= 'wc_all';
			}
			
			if($numsentences){
				print $log "\n Adding num_sentences feature";
				$nontermfeaturecount++;
				$nontermfeatures{$nontermfeaturecount} = 'numw:# sentences';
				if(defined $numsentences{$docid}){				
					my $normalised = $maxnumsentences ;
					if(($maxnumsentences - $minnumsentences) != 0){
						$normalised = ($numsentences{$docid} - $minnumsentences)/($maxnumsentences - $minnumsentences);
					}
					$term_vector->{$nontermfeaturecount} = $normalised;
				}
				
				$nontermfeaturecount++;
				$nontermfeatures{$nontermfeaturecount} = 'numw:avg. # sentences per post';
				if(defined $avgnumsentences{$docid}){
					my $normalised = $maxavgnumsentences;
					if (($maxavgnumsentences - $minavgnumsentences) != 0){
						$normalised = ($avgnumsentences{$docid} - $minavgnumsentences)/ ($maxavgnumsentences - $minavgnumsentences);
					}
					$term_vector->{$nontermfeaturecount} = $normalised;
				}
				$nontermfeaturecount++;
				$nontermfeatures{$nontermfeaturecount} = 'numw:# sentences in 1st post';
				if(defined $numsentences_first{$docid}){
					my $normalised = $maxnumsentences_first;
					if (($maxnumsentences_first - $minnumsentences_first) != 0){
						$normalised = ($numsentences_first{$docid} - $minnumsentences_first)/ ($maxnumsentences_first - $minnumsentences_first);
					}
					$term_vector->{$nontermfeaturecount} = $normalised;
				}
			}
	
			if($courseref){
				print $log "\n adding courseref mention feature";
				$nontermfeaturecount++;
				$nontermfeatures{$nontermfeaturecount} = 'courseref_all';
				if(defined $coursematerialterms{$docid}){
					my $normalised = ($coursematerialterms{$docid} - $mincoursematerail)/ ($maxcoursematerail - $mincoursematerail);
					$term_vector->{$nontermfeaturecount} = $normalised;
					$nontermfeaturecount++;
					$normalised = ($coursematerialtermdensity{$docid} - $mincoursemateraildensity)/ ($maxcoursemateraildensity - $mincoursemateraildensity);
					$term_vector->{$nontermfeaturecount} = $normalised;
					$nontermfeatures{$nontermfeaturecount} = 'courserefdensity';
				}
				else{
					$nontermfeaturecount++;
					$nontermfeatures{$nontermfeaturecount} = 'courserefdensity';
				}
				
				$nontermfeaturecount++;
				$nontermfeatures{$nontermfeaturecount} = 'courseref_nkd';
				if(defined $coursematerialterms_nkd{$docid}){
					my $normalised = ($coursematerialterms_nkd{$docid} - $mincoursematerail_nkd)/ 
													($maxcoursematerail_nkd - $mincoursematerail_nkd);
					$term_vector->{$nontermfeaturecount} = $normalised;
					$nontermfeaturecount++;
					$normalised = ($coursematerialtermdensity_nkd{$docid} - $mincoursemateraildensity_nkd)/($maxcoursemateraildensity_nkd - $mincoursemateraildensity_nkd);
					$term_vector->{$nontermfeaturecount} = $normalised;
					$nontermfeatures{$nontermfeaturecount} = 'courserefdensity_nkd';
				}
				else{
					$nontermfeaturecount++;
					$nontermfeatures{$nontermfeaturecount} = 'courserefdensity_nkd';
				}
				
				$nontermfeaturecount++;
				$nontermfeatures{$nontermfeaturecount} = 'courseref_pfx';
				if(defined $coursematerialterms_pfx{$docid}){
					my $normalised = ($coursematerialterms_pfx{$docid} - $mincoursematerail_pfx)/ ($maxcoursematerail_pfx - $mincoursematerail_pfx);
					$term_vector->{$nontermfeaturecount} = $normalised;
					$nontermfeaturecount++;
					$normalised = ($coursematerialtermdensity_pfx{$docid} - $mincoursemateraildensity_pfx)/ ($maxcoursemateraildensity_pfx - $mincoursemateraildensity_pfx);
					$term_vector->{$nontermfeaturecount} = $normalised;
					$nontermfeatures{$nontermfeaturecount} = 'courserefdensity_pfx';
				}
				else{
					$nontermfeaturecount++;
					$nontermfeatures{$nontermfeaturecount} = 'courserefdensity_pfx';
				}
				
				$nontermfeaturecount++;
				$nontermfeatures{$nontermfeaturecount} = 'courseref_sfx';
				if(defined $coursematerialterms{$docid}){
					my $normalised = ($coursematerialterms_sfx{$docid} - $mincoursematerail_sfx)/ ($maxcoursematerail_sfx - $mincoursematerail_sfx);
					$term_vector->{$nontermfeaturecount} = $normalised;
					$nontermfeaturecount++;
					$normalised = ($coursematerialtermdensity_sfx{$docid} - $mincoursemateraildensity_sfx)/ ($maxcoursemateraildensity_sfx - $mincoursemateraildensity_sfx);
					$term_vector->{$nontermfeaturecount} = $normalised;
					$nontermfeatures{$nontermfeaturecount} = 'courserefdensity_sfx';
				}
				else{
					$nontermfeaturecount++;
					$nontermfeatures{$nontermfeaturecount} = 'courserefdensity_sfx';
				}
			}
			
			if($nonterm_courseref){
				print $log "\n adding nonterm_courseref mention feature";
				$nontermfeaturecount++;
				$nontermfeatures{$nontermfeaturecount} = 'urlref';
				if(defined $num_urlref{$docid}){
					my $normalised = ($num_urlref{$docid} - $minnum_urlref) / ($maxnum_urlref - $minnum_urlref);
					$term_vector->{$nontermfeaturecount} = $normalised;
				}				
				
				$nontermfeaturecount++;
				$nontermfeatures{$nontermfeaturecount} = 'timeref';				
				if(defined $num_timeref{$docid}){
					my $denom = $maxnum_timeref - $minnum_timeref;
					my $normalised;
					if ($denom > 0){
						$normalised = ($num_timeref{$docid} - $minnum_timeref) / $denom;
					}
					else{
						$normalised = $num_timeref{$docid};
					}
					$term_vector->{$nontermfeaturecount} = $normalised;
				}
				
				$nontermfeaturecount++;
				$nontermfeatures{$nontermfeaturecount} = 'urlreffirstpost';
				if(defined $num_urlrefinfirstpost{$docid}){
					my $denom = $maxnum_urlreffirstpost - $minnum_urlreffirstpost;
					my $normalised;
					if ($denom > 0){
						$normalised = ($num_urlrefinfirstpost{$docid} - $minnum_urlreffirstpost) / $denom;
					}
					else{
						$normalised = $num_urlrefinfirstpost{$docid};
					}
					$term_vector->{$nontermfeaturecount} = $normalised;
				}

				$nontermfeaturecount++;
				$nontermfeatures{$nontermfeaturecount} = 'timereffirstpost';
				if(defined $num_timereffirstpost{$docid}){
					my $normalised;
					my $denom = ($maxnum_timereffirstpost - $minnum_timereffirstpost);
					if ($denom > 0 ){
						$normalised = ($num_timereffirstpost{$docid} - $minnum_timereffirstpost) / $denom;
					}
					else{
						$normalised = $num_timereffirstpost{$docid};
					}
					$term_vector->{$nontermfeaturecount} = $normalised;
				}

				$nontermfeaturecount++;
				$nontermfeatures{$nontermfeaturecount} = 'equation';				
				if(defined $hasEquation{$docid}){					
					$term_vector->{$nontermfeaturecount} = $hasEquation{$docid};
				}
			}
						
			if($affir){
				print $log "\n adding affir mention feature";
				$nontermfeaturecount++;
				$nontermfeatures{$nontermfeaturecount} = '#affirmations';
				if(defined $affirmations{$docid}){
					my $normalised = $maxaffir;
					if(($maxaffir - $minaffir) != 0){
						$normalised = ($affirmations{$docid} - $minaffir)/ ($maxaffir - $minaffir);
					}
					$term_vector->{$nontermfeaturecount} = $normalised;
					
					$nontermfeaturecount++;
					$nontermfeatures{$nontermfeaturecount} = 'affirmations density';
					$normalised = $maxaffirdensity;
					if(($maxaffirdensity - $minaffirdensity) != 0){
						$normalised =($affirtermdensity{$docid} - $minaffirdensity)/ ($maxaffirdensity - $minaffirdensity);
					}
					$term_vector->{$nontermfeaturecount} = $normalised;
				}
				else{
					$nontermfeaturecount++;
					$nontermfeatures{$nontermfeaturecount} = 'affirmations density';
				}
			}
			
			if($agree){
				print $log "\n adding agree feature";
				$nontermfeaturecount++;
				$nontermfeatures{$nontermfeaturecount} = '#agreedisagree';
				if(defined $agreedisagree{$docid}){			
					my $normalised = $maxagreedisagree;
					if(($maxagreedisagree - $minagreedisagree) != 0){
						$normalised = ($agreedisagree{$docid} - $minagreedisagree)/ ($maxagreedisagree - $minagreedisagree);
					}
					$term_vector->{$nontermfeaturecount} = $normalised;
					
					$nontermfeaturecount++;					
					$nontermfeatures{$nontermfeaturecount} = 'agreedisagree density';
					$normalised = $maxagreedisagreedensity;
					if(($maxagreedisagreedensity - $minagreedisagreedensity) != 0){
						$normalised =($agreedisagreedensity{$docid} - $minagreedisagreedensity)/ ($maxagreedisagreedensity - $minagreedisagreedensity);
					}
					$term_vector->{$nontermfeaturecount} = $normalised;
				}
				else{
					$nontermfeaturecount++;
					$nontermfeatures{$nontermfeaturecount} = '#agreedisagree density';
				}
			}

			#record the term vector for this thread
			$termvectors_collated{$docid} = [$serialid, $label, $forumname, $term_vector];
		}
		
	}
	
	# print all features for this thread to file
	my $inter_thread_couter	= 	0;
	
	foreach my $docid ( sort {$a<=>$b} keys %termvectors_collated){
		my @thread 		= @{$termvectors_collated{$docid}};
		my $serialid	= $thread[0];
		my $label 		= $thread[1];
		my $forumname	= $thread[2];
		my $term_vector = $thread[3];

		print $FH "$docid\t $label\t";
		foreach my $tid (sort { $a <=> $b } (keys %$term_vector)){
			if(!defined $term_vector->{$tid}){	next;	}
			printf $FH  "$tid:%.3f\t",$term_vector->{$tid};
		}
			
		if( $label eq '+1' ){	$inter_thread_couter++;	}
		print $FH "\n";
	}
	
	open (my $feature_list, ">$path/$feature_file") 
					or warn "Cannot open $path/$feature_file for writing";
	foreach (sort {$a <=> $b} (keys %nontermfeatures)){
		print $feature_list "$_\t$nontermfeatures{$_}\n";
	}
	close $feature_list;
	
	close LOG;
}

sub getInstViewedThreads{
	my ($dbh) = @_;
	# my $instructors_query		= "select user_id from users u, hash_mapping h where access_group_id in (2,3,7,10) and u.session_user_id = h.session_user_id";
	# 4,5,6,9 - students
	my $instructors_query		= "select user_id from users u, hash_mapping h where access_group_id in (2,3,7) and u.session_user_id = h.session_user_id";
	my $instructors_ref			= $dbh->selectall_hashref($instructors_query,'user_id')
										or die "query failed: $instructors_query \n $DBI::errstr";
	my @instructors				= keys %{$instructors_ref};
	
	my $threads_viewed_query	= "select id, item_id, user_id, timestamp from activity_log where action = 'view.thread' and user_id in (";

	my $userstring 				= join(",",@instructors);
	$threads_viewed_query		.= "$userstring);";
		
	print "\n Executing... $threads_viewed_query \n ";
		
	my $threads_viewed			= $dbh->selectall_hashref($threads_viewed_query,'id')
										or die "query failed: $threads_viewed_query \n $DBI::errstr";
	return $threads_viewed;
}

sub isThreadApproved{
	my ($dbh,$docid)	= @_;
	my $approved = 0;
	$approved = @{$dbh->selectcol_arrayref("select approved from thread where docid = $docid")}[0];
	return $approved;
}

sub isThreadResolved{
	my ($dbh,$docid) = @_;
	my $resolved = 0;
	$resolved = @{$dbh->selectcol_arrayref("select has_resolved from thread where docid = $docid")}[0];
	return $resolved;
}

sub isThreadDeleted{
	my ($dbh,$docid) = @_;
	my $deleted = 0;
	$deleted = @{$dbh->selectcol_arrayref("select deleted from thread where docid = $docid")}[0];
	return $deleted;
}

sub getTitleFeatures{
	my ($titleunigrams) = @_;
	my $lecture = 0; my $otherwords = 0;
	
	foreach my $term (keys %$titleunigrams){
		$term = lc($term);
		if ($term eq 'lecture' || $term eq 'lectures'){
			$lecture = 1;
		}
		
		if ($term eq 'assignment' || $term eq 'assignments' ||
			$term eq 'quiz'			|| $term eq 'quizes'	|| $term eq 'quizzes' ||
			$term eq 'grade'		|| $term eq 'grades'	||
			$term eq 'project'	|| $term eq 'projects'	||
			$term eq 'exam'		|| $term eq 'exams' 
		){
			$otherwords = 1;
		}
	}
	return ($lecture,$otherwords);
}

sub getAssessmentWordCount{
	my ($unigrams,$terms) = @_;
	my $count = 0;
	
	foreach my $termid (keys %$unigrams){
		my $term = $terms->{$termid}{'term'};
		$term = lc($term);
		if ($term eq 'assignment' || $term eq 'assignments' ||
			$term eq 'quiz'		|| $term eq 'quizes'	||
			$term eq 'grade'	|| $term eq 'grades'	||
			$term eq 'project'	|| $term eq 'projects'	||
			$term eq 'exam'		|| $term eq 'exams' 	||
			$term eq 'reading'	|| $term eq 'readings' 
		){
			$count += $unigrams->{$termid};
		}
	}
	
	return $count;
}

sub getProblemWordCount{
	my ($unigrams,$terms) = @_;
	my $count = 0;
	foreach my $termid (keys %$unigrams){
		my $term = $terms->{$termid}{'term'};
		$term = lc($term);
		if ($term eq 'problem' || $term eq 'problems' || 
			$term eq 'error' || $term eq 'errors'
			){
			$count += $unigrams->{$termid};
		}
	}
	return $count;
}

sub getRequestWordCount{
	my ($unigrams,$terms) = @_;
	my $count = 0;
	foreach my $termid (keys %$unigrams){
		my $term = $terms->{$termid}{'term'};
		$term = lc($term);
		if ($term eq 'request' || $term eq 'submit'	||
				$term eq 'suggest'
			){
			$count += $unigrams->{$termid};
		}
	}
	return $count;
}

sub getConclusionWordCount{
	my ($text) = @_;
	
	$text = lc($text);
	my $count = 0;
	my @matches = $text =~ /thank/g;
	
	$count = @matches;
	
	return $count;
}

sub isThreadOPAnon{
	my ($dbh,$docid) = @_;
	my $isanaon = 0;
	$isanaon = @{$dbh->selectcol_arrayref("select starter from thread where docid = $docid")}[0];
	$isanaon = ($isanaon == -1)? 1: 0;
	return $isanaon;
}
			
sub averageTermIDF{
	my ($termIDFs, $total_num_courses) = @_;
	my %normalisedIDF = ();
	
	foreach my $courseid ( keys %$termIDFs ){
		foreach my $termid ( keys %{$termIDFs->{$courseid}} ){
			if (!exists $normalisedIDF{$termid}){
				$normalisedIDF{$termid} =
						$termIDFs->{$courseid}{$termid};
			}
			else{
				$normalisedIDF{$termid} +=
						$termIDFs->{$courseid}{$termid};					
			}
		}
	}
	
	foreach my $termid ( keys %normalisedIDF ){
			$normalisedIDF{$termid} = 
						$normalisedIDF{$termid} / $total_num_courses;
	}
	return \%normalisedIDF;
}

sub computeTFIDFs{
	my ($termFrequencies, $termIDFs, 
		$term_course, $num_threads, 
		$corpus_type, $dbh, $tftype) = @_;
	
	if(!defined $termIDFs || keys %$termIDFs eq 0){
		print "\n computeTFIDFs: IDFs not defined. Check IDF table and retrieval steps ";
		exit(0);
	}
	
	if(!defined $termFrequencies){
		print "\n computeTFIDFs: TFs not defined. Check TF tables and retrieval steps ";
		exit(0);
	}
	
	my $max_tfs; #hashref
	my %termWeights = ();
	my $maxtf_thread; # hashref
	
	my $puretf	= 0;
	my $logtf	= 0;
	my $booltf	= 0;
	
	my $alpha	= 1;
	
	if ($tftype eq "bool"){
		$booltf	= 1;
	}elsif($tftype eq "pure"){
		$puretf = 1;
	}elsif($tftype eq "log"){
		$logtf	= 1;
	}
	
	my $puretf_param;
	
	if( $puretf ){
		# http://nlp.stanford.edu/IR-book/html/htmledition/maximum-tf-normalization-1.html
		$puretf_param = 0.4; # Smoothing parameter 0.4 or 0.5 are good values. See above url.
		$maxtf_thread = getMaxThreadTF($termFrequencies);
	}
	
	#calculate total number of threads across all courses
	my $tot_num_threads = 0;
	my $tot_num_courses = 0;
	$tot_num_courses = keys %$termFrequencies;
	foreach my $courseid ( keys %$termFrequencies ){
		$tot_num_threads += keys %{$termFrequencies->{$courseid}};
	}
	
	foreach my $courseid ( keys %$termFrequencies ){
		foreach my $threadid ( keys %{$termFrequencies->{$courseid}} ){
			my $maxtf_this_thread = $maxtf_thread->{$courseid}{$threadid};
			if(keys %{$termFrequencies->{$courseid}{$threadid}} == 0){
				print "\n No terms found for $courseid \t $threadid";
			}
			foreach my $termid ( keys %{$termFrequencies->{$courseid}{$threadid}} ){
				###tf
				my $tf = $termFrequencies->{$courseid}{$threadid}{$termid};
				
				if ( $booltf ){
					$tf = 1;
				}elsif ( $puretf ){
					$tf = $puretf_param +  ( (1-$puretf_param) * ($tf / $maxtf_this_thread) )
				}elsif( $logtf){
					$tf = 1 +  log($tf)/log(10);
				}
				
				###idf
				my $df		= $termIDFs->{$termid}{'sumdf'};
				my $term	= $termIDFs->{$termid}{'term'};
				my $num_courses_term = keys %{$term_course->{$termid}};
				my $idf = 0;
				
				if (!defined $term){
					print "\n computeTFIDFs: Exception: $termid \t $term not found in TF table ";
					exit(0);
				}
				if (!defined $df){
					print "\n computeTFIDFs: Exception: idf not read. $termid \t $term \t $courseid ";
					exit(0);
				}

				if (!defined $num_threads){
					die "\n computeTFIDFs: Exception: num_threads not read. $courseid";
				}
				
				if ($df == 0){	$idf = 0;	}
				$idf = $tot_num_threads / $df;

				###round off idf score to 3 decimal places
				$idf = sprintf("%.3f", $idf);
				
				###tf.idf				
				if ( $puretf || $logtf || $booltf){
					$termWeights{$courseid}{$threadid}{$termid}	= $tf;
				}
				else{
					my $termweight = $tf * $idf;
					$termWeights{$courseid}{$threadid}{$termid}	= $termweight;
				}
			}
		}
	}
	return \%termWeights;
}

sub getMaxThreadTF{
	my ($termFrequencies) = @_;
	my %maxtf = ();
		foreach my $courseid ( keys %$termFrequencies ){
			foreach my $threadid ( keys %{$termFrequencies->{$courseid}} ){
				my $maxtf = 0.0;
				foreach my $termid ( keys %{$termFrequencies->{$courseid}{$threadid}} ){
					my $thistf	= $termFrequencies->{$courseid}{$threadid}{$termid};
					$maxtf		= ($thistf > $maxtf)?$thistf:$maxtf;
				}
				$maxtf{$courseid}{$threadid}  = $maxtf;
			}
		}
	return \%maxtf;
}

sub sumOfSquares{	
	my ($term_vector) = @_;
	my $sum_of_squares = 0;
	foreach my $termid (keys %$term_vector ){
		my $termweight = $term_vector->{$termid};		
		
		#print "\n $termid \t $termweight";
		$sum_of_squares += ($termweight * $termweight);
	}
	return $sum_of_squares;
}

sub normalize{
	my ($value, $denominator) = @_;
	
	if (!defined $value){
		return undef;
	}
	if ($value == 0){
		return 0;
	}
	
	#my $intervention_density = $denominator/$num_inter;
	#$normalized = $value * $intervention_density;
	my $normalized = $value / $denominator;
	return $normalized;
}

sub maxminNorm{
	my ($value, $max, $min) = @_;
	my $range = ($max - $min);
	
	#sanity test
	if ($range == 0 && $value!= 0){
		die "\n maxminNorm: Exception: $value. Check your max min assignments!";
	}
	
	if ($range == 0) { return 0; }
	my $normalised = ( ($value - $min) / $range);
	return $normalised;
}

sub fetchTermVector{
	my ($term_weights, $debug) = @_;

	my %term_vector = ();
	#print "\n #terms " .(keys %$term_weights );
	foreach my $termid (keys %$term_weights ){
		if ( !exists $term_vector{$termid} ){
			$term_vector{$termid} = $term_weights->{$termid};
		}
		else{
			$term_vector{$termid} += $term_weights->{$termid};
		}
		#print "\n $termid \t $term_weights->{$termid}\n";
	}
	#print "\n";
	return \%term_vector;
}

sub getConcordance{
	my($term,$text) = @_;
	$text = lc($text);
	$term = lc($term);
	if ($text =~ /([A-Za-z0-9\']+[\s+\.\?\!\']+)?([A-Za-z0-9\']+[\s+\.\?\!\']+)?([A-Za-z0-9\']+[\s+\.\?\!\']+)?$term([\s+\.\?\!\']+[A-Za-z0-9\']+)?([\s+\.\?\!\']+[A-Za-z0-9\']+)?([A-Za-z0-9\']+[\s+\.\?\!\']+)?/ ){
		my $con;
		if (defined $1){ $con .= $1; }
		if (defined $2){ $con .= $2; }
		if (defined $3){ $con .= $3; }
		$con .= $term;
		if (defined $4){ $con .= $4; }
		if (defined $5){ $con .= $5; }
		if (defined $6){ $con .= $6; }		
		return $con;
	}
	else{
		print "$text\n getConcordance: exiting abnormally.."; exit(0);
	}
}

sub encodeforumname{
	my $forumname = shift;
	my @code;
	if($forumname eq 'Errata'){
		@code = (1,0,0,0);
	}
	elsif($forumname eq 'Lecture'){
		@code = (0,1,0,0);
	}
	elsif($forumname eq 'Homework'){
		@code = (0,0,1,0);
	}
	elsif($forumname eq 'Exam'){
		@code = (0,0,0,1);
	}
=pod
	elsif($forumname eq 'Project'){
		@code = (0,0,0,0,1);
	}

	elsif($forumname eq 'PeerA'){
		@code = (0,0,0,0,0,1);
	}
	elsif($forumname eq 'General'){
		@code = (0,0,0,0,0,0,1);
	}
	elsif (!defined $forumname){
		@code = (-1,-1,-1,-1,-1,-1,-1);
	}
=cut
	else{
		@code = (-1,-1,-1,-1);
	}

	return \@code;
}

sub getExternalSourceReference{
	my $text = shift;
	$text = lc($text);
	my @matches = $text =~ s/(from|according\s+to)? wiki|wikipedia|(text)?book|thread|forum|<URL>(states?|says?|suggests?)that//g;
	my $matchcount = @matches;
	return $matchcount;
}

sub getCourseMaterialMentions{
	my $text = shift;
	my $nakedcount = my $prefixcount = my $suffixcount = 0;
	$text = lc($text);
	$text =~ s/(first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth|eleventh|twelfth|thirteenth|fourteenth|fifteenth|sixteenth|seventeenth|eighteenth|nineteenth|twentieth|final)/<ordinal>/g;
	
	my @prefixmatches = $text =~ /((?:(?:[0-9][0-9]?(?:[\.:-][0-9][0-9]?)?)|(?:\<ordinal\>))\s?(?:nd|th|st)?[\s\-:\.]?(?:(?:video\s*)?lectures?\s*(?:notes?|videos?|slides)|slides?|questions?|assignments?|problem\s(sets?)?|(practice|pop)?quiz(es)?|quiz(es)?|homeworks?|qn|weeks?|videos?))/g;
	$prefixcount = @prefixmatches;
	$text =~ s/((([0-9][0-9]?([\.:-][0-9][0-9]?)?)|(\<ordinal\>))\s?(nd|th|st)?[\s\-:\.]?(lectures?|slides?|questions?|assignments?|problem\s(sets?)?|(practice|pop)?quiz(es)?|homeworks|qn|weeks|videos))//g;
	
	my @suffixmatches = $text =~ /((?:(?:video\s*)?lectures?\s*(?:notes?|videos?|slides?)|slides?|questions?|assignments?|problem\s(sets?)?|(practice|pop)?quiz(es)?|homeworks?|qn|weeks?|videos?)[\s\-:\.]?(?:[0-9][0-9]?(?:[\.:\-][0-9][0-9]?)?))/g;
	$suffixcount = @suffixmatches;
	$text =~ s/((lectures?|slides?|pages?|questions?|assignments?|problem\s(sets?)?|(practice|pop)?quiz(es)?|homeworks?|qn|weeks?|videos?)[\s\-:\.]?([0-9][0-9]?([\.:\-][0-9][0-9]?)?))//g;
	
	#my @nakedmatches = $text =~ /(lecture|lecture\s+notes?|lecture\s+video|video\s+lecture|lecture\s+slide|slide|question|assignment|quiz|homework|qn|week)/g;
	my @nakedmatches = $text =~ /((?:video\s*)?lectures?\s*(?:notes?|videos?|slides)?|slides?|questions?|assignments?|(practice|pop)?quiz(es)?|homeworks?|qn|weeks?)/g;
	$nakedcount = @nakedmatches;
	my $allcount = $nakedcount + $prefixcount + $suffixcount;
	
	return ($prefixcount,$suffixcount,$nakedcount,$allcount);	
	#return $allcount;	
}


## basline system's affirmation EDM2015
sub getAffirmations{
	my $text = shift;
	my $original_text = $text;
	
	$text = lc($text);
	$text =~ s/\s+that\s*('s|is)/ <DEM> /g;
	$text =~ s/you\s*('r|ar)e?/ <SPP> /g;
	$text =~ s/((yes|no)\,?\s+)?(<SPP>|<DEM>)?\s+((very|quite|absolute(ly)?)\s+)?((in)?correct|right)\s*/ <ACK> /g;
	my @matches = $text =~ /<ACK>/g;
	
	# reset the text variable
	# todo: it does look like an error. 
	# I agree
	$text = lc($original_text);
	$text =~ s/\s+(absolute|exact)(ly)?\s*/ <SIM> /;
	$text =~ s/(<SIM>)?\s*(same|similar)\s*/ <SIM> /;
	$text =~ s/(\s+issue|problem|bug|error|mistake)s?\s*/ <BUG> /g;
	my @agreement_matches = $text =~ /(<SIM>\s*<BUG>|me\s+too)/g;
	
	my $count = @matches + @agreement_matches;
	return $count;
}

sub getAgreeDisagree{
	my $text = shift;
	if(!defined $text){
		print "\n getAffirmations: text not defined";
		exit(0);
	}
	
	my $original_text 	= $text;
	$original_text 		= lc($original_text);
	
	my $sentences = Preprocess::getSentences($original_text);
	my $firstsentence;
	$firstsentence = $sentences->[0];
	$text = $firstsentence;
	if (!defined $text){
		return 0;
	}
	
	$text =~ s/\s+that\s*('s|is)/ <DEM> /g;
	$text =~ s/you\s*('r|ar)e?/ <SPP> /g;
	$text =~ s/(<SPP>|<DEM>)?((in)?correct|right|wrong|mistaken)\s*/ <ACK> /g;
	my @matches = $text =~ /<ACK>/g;
	
	## reset the text variable
	## todo: "it does look like an error."
	## I agree
	$text	= $firstsentence;
	$text =~ s/(yes|no|agree|disagree|agreed|disagreed)\s*/ <YESNO> /g;
	my @agreement_matches = $text =~ /(<YESNO>)/g;
	
	my $count = @matches + @agreement_matches;
	return $count;
}

sub getHedging{
	my $text = shift;
	
	$text =~ s/I/<PRONOUN>/g;
	$text = lc($text);
	$text =~ s/(it|this|that)/<PRONOUN>/g;
	$text =~ s/(can|could|may\s?(be)?|might|should|would)/<MODAL>/g;
	$text =~ s/(generally|usually|often|occasionally|mostly)/<ADVERB>/g;
	$text =~ s/(think|thought|appear(s|ed)?|believ(s|ed)?|seem(s|ed)?|look(s|ed)?|supposed?|tend(s|ed)?|suggest(s|ed))/<VERB>/g;
	$text =~ s/\s*//g;
	my @matches = $text =~ /((?:<ADVERB>)?<VERB>)/g;
	my @auxverb_matches = $text =~ /((?:<PRONOUN>)(?:<MODAL>)(?:<ADVERB>)?(?:be))/g;
	
	my $count = @matches + @auxverb_matches;
	return $count;
}

sub hasProfMention{
	my $text = shift;
	$text = lc($text);
	
	my @profmatches = $text =~ /(prof\.?(ess?or)?s?|instructors?|lecturers?|dr)[\?\.\!\:\;\s]/;
	my @staffmatches = $text =~ /(prof\.?(ess?or)?s?|instructors?|lecturers?|staffs?|tas?)[\?\.\!\:\;\s]/;
	
	my $staff = scalar (@staffmatches);
	my $prof = scalar (@profmatches);	
	
	return ($prof,$staff);
}

sub hasQuestionMarks{
	my $text = shift;
	if ($text =~ /.*\?+.*/){		
		return 1;
	}
	else{
		return 0;
	}
}

sub hasEquation{
	my $text = shift;
	
	if ( $text =~ /EQUATION/ ){
		return 1;
	}
	else{
		return 0;
	}
}

sub getnumURLref{
	my $text = shift;
	if(!defined $text){ return 0;}

	my @matches = $text =~ /(URLREF)/g;
	my $count = @matches;
	
	if($count > 0 ){
		return $count;
	}
	else{
		return 0;
	}
}

sub getnumTimeref{
	my $text = shift;
	if(!defined $text){ return 0;}

	my @matches = $text =~ /(TIMEREF)/g;
	my $count = @matches;
	
	if($count > 0 ){
		return $count;
	}
	else{
		return 0;
	}
}

sub getnumQuestions{
	my $text = shift;
	if(!defined $text){ return 0;}
	
	my @matches = $text =~ /(\?)/g;
	my $count = @matches;	
	if($count > 0 ){
		return $count;
	}
	else{
		return 0;
	}
}

sub getSentiPunct{
	my $text = shift;
	#Successive appearance of a question mark 2 or more times
	my @qmatches = $text =~ /([\?][\?]+)/g;
	#Successive appearance of the same letter 3 or more times
	my @wmatches = $text =~ /([a-zA-Z])\1\1+/g;
	
	my $count = @qmatches;
	$count += @wmatches;	

	return $count;
}

#Returns number of quotes appearing in a text
sub getnumQuotes{
	my $text = shift;
	my @matches = $text =~ /(\")/g;
	my $count = @matches;
	if(!defined $text || $count < 1){ return 0;}
	# adjusting error due to unbalanced quote marks
	$count =  ($count%2 == 0)? $count : $count - 1;	
	return ($count/2);
}

sub getMeanPosttimeDifferences{
	my ($dbh,$threadid,$courseid,$posttimesth,$commenttimesth) = @_;
										
	$posttimesth->execute($threadid,$courseid) or die $dbh->errstr;
	$commenttimesth->execute($threadid,$courseid) or die $dbh->errstr;
	
	my @posts = @{$posttimesth->fetchall_arrayref()};
	my @comments = @{$commenttimesth->fetchall_arrayref()};	
		
	if ( scalar @posts == 0){ 	
		return -1;	
	}	
		
	my %times;
	foreach my $post (@posts){			
		$times{$post->[0]} = $post->[1]
	}	
		
	foreach my $post (@comments){	
		$times{$post->[0]} = $post->[1]
	}

	#print "\n";
	
	my $time_diffs_sum = 0;
	my $index = 0;
	my $prev_time = 0;
	foreach my $id (sort {$times{$a}<=>$times{$b}} keys %times){
		my $time = $times{$id};
		my $diff = 0;
		if ($index != 0) { 			
			$diff = $time - $prev_time;
			$time_diffs_sum += $diff;
		}
		#print "\n $id \t $time \t $diff";
		$prev_time = $time;
		$index++;
	}
	
	#if there is just one post then this feature
	#simply doesn't exist
	if(keys %times == 1){
		return -1; 
	}
	
	my $mean_time_diff = $time_diffs_sum / keys %times;
	
	if( $mean_time_diff == 0 && (keys %times) > 1 ){
		print " " .(keys %times) ."\n";
		print join (": ", (keys %times));
		print "\nException: $time_diffs_sum. Mean time diff cannot be zero.\n"; exit(0);
	}
	
	#print "\n $threadid \t $mean_time_diff ". (keys %times);
	
	return $mean_time_diff;
}

sub checkmaxminexception{
	my($max,$min,$feature) = @_;
	if ( $max == $min ){
		warn "Warning: MAX & MIN for $feature are same.".
			"$max \t $min";
	}
	elsif( $max == 0 && $min == 0){
		die "Exception: MAX & MIN for $feature are zero.";
	}
}

sub getMaxMin{
	my ($var, $max, $min, $var_name) = @_;
	if(!defined $var){
		die "getMaxMin: $var_name is not defined \n";
	}
	if(!defined $max){
		die "getMaxMin: Max not defined for $var_name: $var\n";
	}
	if(!defined $min){
		die "getMaxMin: Min not defined for $var_name: $var\n";
	}
	$max = ($var > $max)? $var: $max;
	$min = ($var < $min)? $var: $min;
	return ($max, $min);
}

sub printTermVector{
	my ($term_vector) = @_;
	foreach my $term ( sort {$a<=>$b} (keys %$term_vector) ){
		printf "\n $term \t %.3f\t", $term_vector->{$term};		
	}
}

sub extractNgrams{
	my ($text, $n, $stem, $stopword) = @_;
	
	$text = Preprocess::replaceURL($text);	
	$text = Preprocess::replaceTimeReferences($text);
	$text = Preprocess::replaceMath($text);
	my $sentences = Preprocess::getSentences($text);
	$sentences = Preprocess::removeMarkers($sentences);
	$sentences = Preprocess::removePunctuations($sentences);

	my $tokens = Preprocess::getTokens($sentences,0);
	
	#lowercase tokens. This affects stopword removal.
	foreach (@$tokens){
		$_  = lc($_);
	}
	
	if(!$stopword){
		# curated list of 100+ stopwords. Minimal reduction
		$tokens = Preprocess::removeStopWords($tokens,4);
	}
	else{
		# a standard strict filtering of 600 odd terms
		$tokens = Preprocess::removeStopWords($tokens,3);
	}
	
	if( $stem ){
		$tokens = Preprocess::stem($tokens);
	}
	
	$tokens = Preprocess::removeOrphanCharacters($tokens);	
	#chunk tokens by sentences and remember order of appearance of words by chunk
	my $gramtext = join ' ', @$tokens;
	my $ngrams = Lingua::EN::Ngram->new;
	$ngrams->text($gramtext);
	
	# term frequency
	my $grams = $ngrams->ngram( $n );
	
	# the ngrams package screws up some tokens
	# reducing some terms to orphan characters 
	# and to stopwords such as 'a' and 'th'
	my @ngramtokens;
	foreach my $ngram ( keys %$grams ) {
		push ( @ngramtokens, (split (/\s/, $ngram)) );
	}
	
	my $gramtokens = \@ngramtokens;
	if(!$stopword){
		# curated list of stopwords. Minimal reduction
		$gramtokens = Preprocess::removeStopWords($gramtokens,4);
	}
	else{
		# a standard strict filtering of 600 odd terms
		$gramtokens = Preprocess::removeStopWords($gramtokens,3);
	}
	
	$gramtokens = Preprocess::removeOrphanCharacters($gramtokens);
	
	my %filteredtokens = (); 
	foreach (@$gramtokens){
		$filteredtokens{ $_ } = 1;
	}
	
	foreach my $ngram ( keys %$grams ) {
		my @ngramtokens = split (/\s/, $ngram);
		foreach my $gram ( @ngramtokens ) {
			if ( !exists $filteredtokens{$gram} || scalar (@ngramtokens) < $n ){
				delete $grams->{$ngram};
				last;
			}
		}
	}

	return $grams;
}

sub getMaxTfs{
	my ($dbh,$courses) = @_;
	my $qry = "select termid, maxtf, courseid 
					from termFreqMax where courseid in (";
	foreach my $course (@$courses){
		$qry .= " \'$course\',";
	}
	$qry =~ s/\,$//;
	$qry .= " ) ";
	
	my $sth = $dbh->prepare($qry) or die "cannot prepare maxtfs qry \n $!";
	$sth->execute() or die "cannot execute maxtfs qry \n $!";
	my @maxtfs_arr = @{$sth->fetchall_arrayref()};
	my %maxtfs = ();
	foreach my $row (@maxtfs_arr) {
		$maxtfs{$row->[2]}{$row->[0]} = $row->[1];		
	}
	return \%maxtfs;
}

sub normalizeTermWeights{
	my($term_vector, $terms, $num_inter, $max_tf) = @_;
	
	foreach my $termid (keys %$term_vector ){
		my $tf = $term_vector->{$termid};
		# Normalize term weights for #threads and #interventions
		# in this course
		$tf = normalize( $tf, $max_tf->{$termid}, $num_inter);
		$term_vector->{$termid} = $tf;
	}
	
	return $term_vector;
}

1;
