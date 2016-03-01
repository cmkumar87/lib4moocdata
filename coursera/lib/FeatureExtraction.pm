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

sub extractCueWordFeatures{
	my ($text, $cueword_dict, $cueword_dict2) = @_;
	
	my %cueword_occurrences = ();
	$text =~ s/<PAR>/ /g;
	$text = Preprocess::normalizePeriods($text);
	$text = Preprocess::normalizeSpace($text);
	my $sentences = Preprocess::getSentences($text);
	$sentences = Preprocess::fixApostrophe($sentences);
	$sentences = Preprocess::removePunctuations( $sentences);
	
	my $sid = 1;
	foreach my $sentence (@$sentences){
		print "\n$sentence\n";
		$sentence = lc($sentence);
		
		my $tokens = get_tokens($sentence);
		$tokens = Preprocess::removeOrphanCharacters($tokens);
		
		my $pos = -1;
		
		if ($sentence =~ /(^so|^then)/ ){
			$cueword_occurrences{$sid}{$1} = 1;
			shift @$tokens; $pos++;
		}
		
		foreach my $token (@$tokens){
			$pos++;
			if( !exists $cueword_dict->{$token} ){
				next; 
			}
			if ( !exists $cueword_occurrences{$sid}{$token} ){
				$cueword_occurrences{$sid}{$token} = $pos;			
			}
		}
		
		my $bigram_positions = getBigramsPos($tokens);		
		
		$cueword_occurrences{$sid}
			 = Utility::merge_hashes($cueword_occurrences{$sid}, extractDomainWordFeatures($bigram_positions, $cueword_dict2));		
	
		
		foreach my $t(keys %{$cueword_occurrences{$sid}}){
			print "cue: $t\t";
		}
		$sid++;
	}
	return \%cueword_occurrences;
}

sub extractDomainWordFeatures{
	my($bigrams, $domainword_dict) = @_;
	
	my %domainword_count = ();

	foreach my $bigram ( sort{$bigrams->{$a}<=>$bigrams->{$b}} (keys %{$bigrams}) ){
		
		my $word1 =  (split /\s/,$bigram)[0];
		my $word2 =  (split /\s/,$bigram)[1];
		
		my $pos = $bigrams->{$bigram};
		$bigram =~ s/\s*//g;
		
		my $word = $bigram;
		
		if( !exists $domainword_dict->{$bigram} ){
			
			if( !exists $domainword_dict->{$word1} ){
			
				if( !exists $domainword_dict->{$word2} ){
					next;
				}
				else{
					$word = $word2;
				}
			}
			else{
				$word = $word1;
			}
		}
		
		if ( !exists $domainword_count{$word} ){
			$domainword_count{$word} = $pos;
		}
	}
	return \%domainword_count;
}

sub extractFeatures{
	my($text, $pos, $domainword_dict, $debug) = @_;
	
	if( !defined $text || $text eq '' ) { return; }
	
	# features to extract
	my $hascite = 0;
	my $hasurl = 0;
	my $textlength = 0;
	my $hastimeref = 0;
	my $posbigrams;
	
	#length of post in words excluding punctuations
	
	#timeref boolean
	if ( $text =~ /\<TIME\_REF\>/ ){
		$hastimeref = 1;
	}

	$text = Preprocess::replaceURL($text);
	
	my $sentences = Preprocess::getSentences($text);
	# TODO find if sentence is question. 
	# this is an interesting feature
	
	# quotation/citation features
	my $quotes = Preprocess::getQuotes($sentences);
	
	my $extracted_sentences = Preprocess::splitParentheses($sentences);
	foreach (@$extracted_sentences){
		push \@$sentences, $_;
	}
	
	## PARA MARKERS DISAPPEAR AFTER THE NEXT STEP ##
	$sentences = Preprocess::removeMarkers($sentences);
	
	#POS tagging
	my $tagged_sentences = undef;
	
	if ($pos){
		$tagged_sentences = getPOStagged($sentences,$debug);
		#Create POS bigrams
		$posbigrams = createPOSBigrams($tagged_sentences);
	}
	
	#Extracting ngram features respecting sentence boundaries
	$sentences = Preprocess::fixApostrophe($sentences);
	$sentences = Preprocess::removePunctuations($sentences);
	
	# Lowercase and tokenize to words
	my $tokens;
	my %word_count;
	my %bigram_count;
	my %reasoning_cuewords_count =();
	my %domainword_counts = ();
	
	my %sentlengths;
	my %numcontentwords;
	my $avgsentlength;
	
	my $sid = 0;
	foreach (@$sentences){
		$sid++;
		$_ = lc($_);
		
		$tokens = get_tokens($_);
		$tokens = Preprocess::removeOrphanCharacters($tokens);
		
		$sentlengths{$sid} = scalar @$tokens;
		
		#Remove stopwords
		my $gram_input = Preprocess::removeStopWords($tokens,1);
		$gram_input = Preprocess::removeModals($gram_input);
		
		$numcontentwords{$sid} = scalar @$gram_input;
		
		my $gram_text = join " ", @$gram_input;
		my $bigram = Lingua::EN::Bigram->new;
		$bigram->text( $gram_text );
		
		my $bigram_positions = getBigramsPos($gram_input);
		
		#domain words detection
		$domainword_counts{$sid} = extractDomainWordFeatures
									($bigram_positions, $domainword_dict);
=pod		
		# this is useless
		my @bigram_count = $bigram->bigram_count;
		foreach (keys %{$bigram->bigram_count}){
			if( !exists $bigram_count{$_} ){
				$bigram_count{$_} =  $bigram->bigram_count->{$_} ;
			}
			else{
				$bigram_count{$_} += $bigram->bigram_count->{$_};
			}
		}
=cut
	}
	
	#Find average sentence length for this post
	foreach ( keys %sentlengths ){
		$avgsentlength += $sentlengths{$_};
	}
	$avgsentlength = $avgsentlength/(keys %sentlengths);
	
	return(\%sentlengths, \%numcontentwords, $avgsentlength, 
			$hastimeref, $hasurl, \%domainword_counts, $tagged_sentences, $posbigrams );
}

sub createPOSBigrams{
	my ($tagged_sentences) = @_;
	my %posbigrams = ();
	foreach my $sid ( sort{$a <=> $b} (keys %{$tagged_sentences}) ){
		my $sentence = $tagged_sentences->{$sid};
		$sentence =~ s/>(.*?)</ /g;
		$sentence =~ s/[\"\,\'\?\!\_\&\=\:\\\/\<\>\(\)\[\]\{\}\%\@\#\!\*\+\-\^\.]/ /g;
		$sentence =~ s/\s+((pp)|(ppc)|(ppd)|(ppl)|(ppr)|(pps)|(lrb)|(rrb)|(sym))\s+/ /g;
		$sentence = Preprocess::normalizeSpace($sentence);
		
		my $bigraminputtokens = get_tokens($sentence);
		
		my $tokenid = 0;
		foreach my $t (@$bigraminputtokens){
			$t = $t.$tokenid;
			$tokenid++;			
		}
		
		my $temp = getBigramsPos($bigraminputtokens);
		
		my %temp2 = ();
		foreach my $bi ( sort{$temp->{$a} <=> $temp->{$b}} (keys %{$temp}) ){
			my $value = $temp->{$bi};
			$bi =~ s/[0-9]//g;
			$temp2{$bi} = $value;
		}
		
		$posbigrams{$sid} = \%temp2;
	}
	return \%posbigrams;
}

sub getBigramsPos{
	my ($gram_input) = @_;
	my %bigram_positions = ();
	
	my $pos = 0;
	foreach my $gram (@$gram_input){
		# Failsafe for end of array
		if ($pos+1 >= scalar (@$gram_input)){ last; }
		
		my $bigram = $gram . " " . (@$gram_input)[$pos+1];
		$bigram_positions{$bigram} = $pos;
		$pos++;
	}
	
	return \%bigram_positions;
}

sub getPOStagged{
	my($sentences,$debug) = @_;
	my %tagged_sentences = ();
	my $sid = 1;
	foreach (@$sentences){
			# Create a parser object
			my $posTagger = new Lingua::EN::Tagger;

			# Add part of speech tags to a text
			my $tagged_text = $posTagger->add_tags($_);
			if ($debug){		print $tagged_text."\n";	}
			$tagged_sentences{$sid} = $tagged_text;
			$sid ++;
	}
	return \%tagged_sentences;
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

sub generateTrainingFile{
	my (	$FH, $dbh, $threadcats,
			$unigrams, $freqcutoff, $stem, $term_length, $tftype, $idftype,
			$tprop, $posttime, $diffw, $numw, $numsentences,
			$prof, $courseref, $nonterm_courseref, $hedging, $affir, $agree,
			$question, $numq, $numquotes, $senti, $tlength,
			$forumtype, $forumid, $threadtime, $userfeatures, $solvedness,
			$titlewords, $assessmentwc, $problemwc, $conclusionwc, $requestwc,
			$bi, $path, $feature_file,
			$normalize, $course_samples, $corpus, $corpus_type, $FEXTRACT,
			$debug, $date_formatter, $votes, $pdtb, $pdtbfilepath, $print_format
		) = @_;	

	my @courses = keys %{$course_samples};
	my $num_threads_coursewise; my $num_interventions_coursewise;
	
	if (keys %{$course_samples} == 0){
		die "\n Exception generateTrainingFile: course samples empty!!";
	}
	
	#sanity checks
	if(defined $corpus){
		print "\nCORPUS: @$corpus ";
	}
	else{
		die "\nException: Corpus undef \n";
	}
	
	my $total_num_threads = 0;
	$total_num_threads = Model::getNumValidThreads($dbh,$corpus);
	if ($total_num_threads == 0){
		die "Exception in generateTrainingFile: # threads is zero in $corpus_type course corpus\n";
	}
	print "\n Number of valid threads \t $total_num_threads";
	
	my $lexical = 0;
	my $lengthf = 0;
	my $time	= 0;
	
	if ( $prof || $courseref || $nonterm_courseref || $hedging || 
		 $agree || $affir || $question || $numq || $numquotes || $senti|| $conclusionwc ||
		 $assessmentwc || $problemwc || $requestwc || $pdtb){
		 $lexical = 1;
	}
	
	if ($tlength || $numw || $numsentences || $tprop){
		$lengthf = 1;
	}
	
	if ($posttime || $threadtime || $forumid){
		$time =	1;
	}
	
	my $coursewiseIDF = 0; 
	if($idftype eq  'cwise'){
		$coursewiseIDF = 1;
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
	my %postwise_agreements = ();
	
	#lexical features
	my %num_urlref	= ();
	my %num_timeref = ();
	my %hasEquation = ();
	my %num_urlrefinfirstpost	= ();
	my %num_timereffirstpost	= ();
	my %hasEquationfirstpost	= ();
	
	#discourse features: hedging
	my %hedgeterm			= ();
	my %hedgetermdensity	= ();
	
	#lexical features
	my %profmention			= ();
	my %profmentiondensity	= ();	
	my %staffmention		= ();
	my %staffmentiondensity = ();
	my %conclusionwords		= ();
	my %assessmentwords		= ();
	my %requestwords		= ();
	my %problemwords		= ();	
	
	#time features
	my %meanposttimediff	= ();
	my %numquestions		= ();
	my %multiquestions		= ();
	my %numquotes			= ();
	my %threadStartTime		= ();
	my %threadEndTime		= ();
	
	my %forumid_vector		= ();;
	
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
	
	my $maxprofmentions		= 0.0;			my $minprofmentions		= 9999;
	my $maxavgprofmentions	= 0.0;			my $minavgprofmentions	= 9999;
	my $maxstaffmentions 	= 0.0;			my $minstaffmentions	= 9999;
	my $maxavgstaffmentions = 0.0;			my $minavgstaffmentions = 9999;
	
	my $maxquestionmarks	= 0.0;			my $minquestionmarks	= 99999;
	my $maxquotationmarks	= 0.0; 			my $minquotationmarks	= 9999;
	
	my $maxmultiquestionmarks	= 0.0; 		my $minmultiquestionmarks 	= 9999;
	my $maxhedging				= 0.0; 		my $minhedging				= 9999;
	my $maxhedgingdensity		= 0.0; 		my $minhedgingdensity		= 9999;
	my $maxtimediff				= 0.0;		my $mintimediff				= 999999999.0;
	my $maxthreadStartTime		= 0;		my $minthreadStartTime		= 99999999999;
	my $maxthreadEndTime		= 0;		my $minthreadEndTime		= 99999999999;
	
	my $maxassessmentwords	= 0;			my $minassessmentwords	= 999999;
	my $maxconclusionwords	= 0;			my $minconclusionwords	= 999999;
	my $maxrequestwords		= 0;			my $minrequestwords		= 999999;
	my $maxproblemwords		= 0;			my $minproblemwords		= 999999;
	
	my $maxpdtbexpansion	= 0;			my $minpdtbexpansion	= 999999;
	my $maxpdtbcontingency	= 0;			my $minpdtbcontingency	= 999999;
	my $maxpdtbtemporal		= 0;			my $minpdtbtemporal		= 999999;
	my $maxpdtbcontrast		= 0;			my $minpdtbcontrast		= 999999;
	my $maxpdtball 			= 0;			my $minpdtball 			= 999999;
	
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
	
	my %maxprofmentions		= ();			my %minprofmentions		= ();
	my %maxstaffmentions	= ();			my %minstaffmentions	= ();
	my %maxavgprofmentions	= ();			my %minavgprofmentions	= ();
	my %maxavgstaffmentions = ();			my %minavgstaffmentions = ();
	my %maxquestionmarks	= ();			my %minquestionmarks	= ();
	
	open (LOG,">$path/bad_threads.log")
				or die "cannot open a log file at $path";
				
				
	if($forumid){
		foreach my $courseid (@courses){
			my @forumids = @{$dbh->selectall_arrayref("select distinct forumid from thread 
														where courseid = \"$courseid\"			
															and docid is not null")};
			print "\t #courses\t @courses";
			foreach my $row (@forumids){
				if(!exists $forumid_vector{$row->[0]} ){
					$forumid_vector{$row->[0]} = 1;
					print "\n $courseid \t  $row->[0]";
				}
			}
		}
	}
	
	# Queries
	my $titlewordqry = "select title from thread where docid = ?";
	my $titlewordsth = $dbh->prepare($titlewordqry) or die "prepare failed titlewordqry";		

	# load termIDFs to memory
	my $terms;
	my $term_course;
	my $termsstem;

	if($unigrams || $assessmentwc || $problemwc || $requestwc){
		if($coursewiseIDF){
			($terms,$term_course)	= Model::getCoursewisetermIDF($dbh,$freqcutoff,0,$corpus,$normalize);
		}
		else{
			$terms		 = Model::getalltermIDF($dbh,$freqcutoff,0,$corpus,$normalize);
			$term_course = Model::gettermCourse($dbh);
		}
		#$termsstem	= Model::getalltermIDF($dbh,$freqcutoff,1,$corpus,$normalize);
		#if (keys %{$terms} == 0 || keys %{$termsstem} == 0){
		#sanity check
		if (keys %{$terms} == 0 ){
			die "Exception: termIDFs are empty for $corpus_type. Check the tables and the query!\n @$corpus";
		}
	}
	
	# load TFs to memory
	my %termFrequencies = ();
	if($unigrams || $assessmentwc || $problemwc || $requestwc){
		foreach my $category_id (keys %$threadcats){
			my $tftab	=	$threadcats->{$category_id}{'tftab'};
			
			my %termFrequencies_part = %{Model::getalltfs($dbh,$tftab,$course_samples,$terms,$stem,$term_length)};

			#sanity check
			if (keys %termFrequencies_part == 0){
				print "\n-Exception: TFs are empty";
				exit(0);
			}
			
			foreach my $courseid (keys %termFrequencies_part ){
				#print "\ninside loop"; exit(0);
				foreach my $threadid (keys $termFrequencies_part{$courseid} ){
					foreach my $termid (keys $termFrequencies_part{$courseid}{$threadid} ){
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
	
	
	# First pass
	foreach my $category_id (keys %$threadcats){
		
		my $pdtbout = undef;
		#analysis
		# if($pdtb){
			# open($pdtbout, ">$pdtbfilepath/pdtbrelcount_$category_id.txt")
					# or warn "\n Cannot open file spans at $pdtbfilepath/\n $!";
		# }
		#analysis
		
		my $posttable		=	$threadcats->{$category_id}{'post'};
		my $commenttable	=	$threadcats->{$category_id}{'comment'};
		my $threads			=	$threadcats->{$category_id}{'threads'};
		
		if (!$lexical && !$lengthf && !$time){ 
			print "\n Skipping $category_id due to no lexical lengthf or time feature calculations"; next; 
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
				print LOG "Empty thread: $courseid $threadid $docid \n"; 
				next;	
			}
			
			if($forumid){
				$forumid_vector{$forumid_number}	= 1
			}

			if($threadtime){
				$threadStartTime{$docid}	= getThreadStime($dbh,$threadid,$courseid,$label);
				$threadEndTime{$docid}		= getThreadEtime($dbh,$threadid,$courseid,$label);
			}
			
			if($posttime){
				my $timediff = getMeanPosttimeDifferences($dbh, $threadid, $courseid, $posttimesth, $commenttimesth);
				if ($timediff == -1 ){ 
					$meanposttimediff{$docid} = undef;
					next;
				}
				else{
					#log transformation to prevent loss of precision
					$meanposttimediff{$docid} = log($timediff)/log(10);
				}
				$maxtimediff = ($meanposttimediff{$docid} > $maxtimediff)? $meanposttimediff{$docid} : $maxtimediff;
				$mintimediff = ($meanposttimediff{$docid} < $mintimediff)? $meanposttimediff{$docid} : $mintimediff;
			}
			
			#if (!$lexical && !$lengthf) { next; }
			
			$threadPostlength{$docid} = 
				@{$dbh->selectcol_arrayref("select count(id) from $posttable where thread_id = $threadid and courseid =\'$courseid\'")}[0];
			$threadCommentlength{$docid} = 
				@{$dbh->selectcol_arrayref("select count(id) from $commenttable where thread_id = $threadid and courseid =\'$courseid\'")}[0];
			$numposts{$docid} = $threadPostlength{$docid} + $threadCommentlength{$docid};
			
			if($numposts{$docid} == 0 || $threadPostlength{$docid} == 0){
				print "\n Sanity check failed! $numposts{$docid} \t " . (keys %$posts); exit(0);
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
				print LOG "Empty thread: $docid $courseid $threadid \n"; next;
			}
		
			if($pdtb){
				#initialization
				my @relations = ('expansion','contingency','temporal','comparison');
				open (my $SENSE_FILE, "<$pdtbfilepath/$forumid_number/output-muthu/$threadid".".txt.exp2.out")
								or warn "\n Cannot open file spans at $pdtbfilepath/$forumid_number/output-muthu/$threadid.txt.exp2.out \n $!";
				
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
						die "\n Unknown pdtb relation  $fields[5] in post $fields[0] $threadid of forum $forumid_number";
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
				foreach my $relation (sort keys $pdtbrelation{$docid}){
					$pdtbrelation{$docid}{$relation} = 
						$pdtbrelation{$docid}{$relation}  / $numposts{$docid};
				}
				
				# make pdtb densities: birelations
				if($pdtbrelation{$docid}{'biall'} > 0){
					foreach my $relation (sort keys $pdtbrelation{$docid}){
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
				
				# Analysis
				# print "\n $pdtbrelation{$docid}{'expansion'} \t $pdtbrelation{$docid}{'contingency'} \t $pdtbrelation{$docid}{'temporal'}";
				# print $pdtbout "\n $forumid_number \t $docid \t $threadid\t" . $pdtbrelation{$docid}{'all'}. "\t"
				# . $pdtbrelation{$docid}{'expansion'} . "\t" .$pdtbrelation{$docid}{'contingency'} . "\t" . $pdtbrelation{$docid}{'temporal'} . "\t" . $pdtbrelation{$docid}{'contrast'}
				# . "\t" . $thread_length{$docid} . "\t" . $numposts{$docid};
			}
		
			foreach my $post ( sort {$a <=> $b} keys %$posts){
				#$thread_length{$docid} += my $numwords = @{$dbh->selectcol_arrayref("select length(post_text) from $posttable where 
				#								id = $post and thread_id = $threadid and courseid = \'$courseid\'")}[0];
				#if ( $numwords == 0 ){ next; }
				
				my $postText = $posts->{$post}{'post_text'};
				if($assessmentwc){
					$assessmentwords{$docid} += getAssessmentWordCount($termFrequencies{$courseid}{$threadid},$terms);
				}
				if($problemwc){
					$problemwords{$docid} += getProblemWordCount($termFrequencies{$courseid}{$threadid},$terms);
				}
				if($requestwc){
					$requestwords{$docid} += getRequestWordCount($termFrequencies{$courseid}{$threadid},$terms);
				}
				if($conclusionwc){
					$conclusionwords{$docid} += getConclusionWordCount($postText);
				}
				
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
				if($numq){
					$numquestions{$docid} +=  getnumQuestions($postText);
				}
				if($numquotes){
					$numquotes{$docid} += getnumQuotes($postText);
				}
				if($senti){
					$multiquestions{$docid} += getSentiPunct($postText);
				}
				if($prof){
					my($prof_mention, $staff_mention) = hasProfMention($postText);
					$profmention{$docid} += $prof_mention;
					$staffmention{$docid} += $staff_mention;
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
				if($hedging){
					$hedgeterm{$docid} += getHedging($postText);				
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
					#$thread_length{$docid} += my $numwords = @{$dbh->selectcol_arrayref("select length(comment_text) from $commenttable where 
					#						id = $comment and thread_id = $threadid and courseid = \'$courseid\' and post_id=$post")}[0];
					#if ( $numwords == 0 ){ next; }

					my $commentText = $comments->{$comment}{'comment_text'};
					if($assessmentwc){
						$assessmentwords{$docid} += getAssessmentWordCount($termFrequencies{$courseid}{$threadid},$terms);
					}
					if($problemwc){
						$problemwords{$docid} += getProblemWordCount($termFrequencies{$courseid}{$threadid},$terms);
					}
					if($requestwc){
						$requestwords{$docid} += getRequestWordCount($termFrequencies{$courseid}{$threadid},$terms);
					}					
					if($conclusionwc){
						$conclusionwords{$docid} += getConclusionWordCount($commentText);
					}
					
					$commentText = Preprocess::replaceURL($commentText);
					$commentText = Preprocess::replaceMath($commentText);
					$commentText = Preprocess::replaceTimeReferences($commentText);
					
					if($numsentences){
						my $commentSentences = Preprocess::getSentences($commentText);
						if(defined $commentSentences ){ 
							$numsentences{$docid} += @$commentSentences;
						}
					}
					if($numq){
						$numquestions{$docid} += getnumQuestions($commentText);
					}
					if($numquotes){
						$numquotes{$docid} += getnumQuotes($commentText);
					}
					if($senti){
						$multiquestions{$docid} += 	getSentiPunct($commentText);
					}
					if($prof){
						my($prof_mention, $staff_mention) = hasProfMention($postText);
						$profmention{$docid} += $prof_mention;
						$staffmention{$docid} += $staff_mention;
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
					if($hedging){
						$hedgeterm{$docid} += getHedging($commentText);
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

			if($pdtb){
				# if (!defined $pdtbrelation{$docid}{'expansion'}) { 
					# print "\n $docid \t $threadid \t $forumid_number";
				# }
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
			
			if($threadtime){
				($maxthreadStartTime, $minthreadStartTime) 
					= 	getMaxMin(	$threadStartTime{$docid} ,
									$maxthreadStartTime,
									$minthreadStartTime,
									"thread_start_time"
								 );
				($maxthreadEndTime, $minthreadEndTime) 
					= 	getMaxMin(	$threadEndTime{$docid} ,
									$maxthreadEndTime,
									$minthreadEndTime,
									"thread_end_time"
								 );
			}
			
			if($assessmentwc){
				($maxassessmentwords, $minassessmentwords) 
					=	getMaxMin(	$assessmentwords{$docid},
									$maxassessmentwords,
									$minassessmentwords,
									"asses_word_count"
								 );
			}
			
			if($problemwc){
				($maxproblemwords, $minproblemwords) 
					=	getMaxMin(	$problemwords{$docid},
									$maxproblemwords,
									$minproblemwords,
									"prob_word_count"
								 );
			}
			
			if($requestwc){
				($maxrequestwords, $minrequestwords) 
					=	getMaxMin(	$requestwords{$docid},
									$maxrequestwords,
									$minrequestwords,
									"request_word_count"
								 );
			}
			
			if($conclusionwc){
				($maxconclusionwords, $minconclusionwords) 
					=	getMaxMin(	$conclusionwords{$docid} ,
									$maxconclusionwords,
									$minconclusionwords,
									"conclusion_word_count"
								 );
			}
			
			if($prof){
				$profmentiondensity{$docid} = $profmention{$docid}/$thread_length_nomalizer;
				$staffmentiondensity{$docid} = $staffmention{$docid}/$thread_length_nomalizer;
				
				if ($normalize) {
					if(!exists $maxprofmentions{$courseid}){
						$maxprofmentions{$courseid} = 0;
					}
					
					if(!exists $minprofmentions{$courseid}){
						$minprofmentions{$courseid} = 999999;
					}
					
					if(!exists $maxstaffmentions{$courseid}){
						$maxstaffmentions{$courseid} = 0;
					}
					
					if(!exists $minstaffmentions{$courseid}){
						$minstaffmentions{$courseid} = 999999;
					}
					
					($maxprofmentions{$courseid}, $minprofmentions{$courseid}) 
									=	getMaxMin(	$profmention{$docid},
													$maxprofmentions{$courseid},
													$minprofmentions{$courseid},
													"prof_mentions"
												);
					($maxavgprofmentions{$courseid}, $minavgprofmentions{$courseid}) 
									=	getMaxMin(	$profmentiondensity{$docid},
													$maxavgprofmentions{$courseid},
													$minavgprofmentions{$courseid},
													"prof_mention_density"
												);
				}
				else{
					($maxprofmentions, $minprofmentions) 
									=	getMaxMin(	$profmention{$docid},
													$maxprofmentions,
													$minprofmentions,
													"prof_mentions"
												 );

					($maxavgprofmentions, $minavgprofmentions) 
									=	getMaxMin(	$profmentiondensity{$docid},
													$maxavgprofmentions,
													$minavgprofmentions,
													"prof_mention_density"
												 );
					($maxstaffmentions, $minstaffmentions) 
									=	getMaxMin(	$staffmention{$docid},
													$maxstaffmentions,
													$minstaffmentions,
													"staff_mentions"
												 );

					($maxavgstaffmentions, $minavgstaffmentions) 
									=	getMaxMin(	$staffmentiondensity{$docid},
													$maxavgstaffmentions,
													$minavgstaffmentions,
													"staff_mention_density"
												 );
				}
			}
			
			if($numw){
				if ($normalize) {
				}
				#log transformation to prevent loss of precision
				if ( $thread_length{$docid} != 0){
					$thread_length{$docid} = log ($thread_length{$docid}) / log(10);
				}
				$maxthreadlength = ($thread_length{$docid} > $maxthreadlength)? $thread_length{$docid} : $maxthreadlength;
				$minthreadlength = ($thread_length{$docid} < $minthreadlength)? $thread_length{$docid} : $minthreadlength;
			}
			
			if($numsentences && defined $numsentences{$docid}){
				if ($normalize) {
					if(!exists $maxnumsentences{$courseid}){
						$maxnumsentences{$courseid} = 0;
					}
					
					if(!exists $minnumsentences{$courseid}){
						$minnumsentences{$courseid} = 999999;
					}	
				
					($maxnumsentences{$courseid}, $minnumsentences{$courseid}) 
										=	getMaxMin( $numsentences{$docid},
													   $maxnumsentences{$courseid},
													   $minnumsentences{$courseid},
													   "num_sentences"
													 );
					
					if(!exists $maxavgnumsentences{$courseid}){
						$maxavgnumsentences{$courseid} = 0.0;
					}
					
					if(!exists $minavgnumsentences{$courseid}){
						$minavgnumsentences{$courseid} = 999999.999;
					}
					
					$avgnumsentences{$docid} = $numsentences{$docid} / $numposts{$docid};
					($maxavgnumsentences{$courseid}, $minavgnumsentences{$courseid})
										=	getMaxMin( $avgnumsentences{$docid},
														$maxavgnumsentences{$courseid},
														$minavgnumsentences{$courseid},
														"avg_num_sentences"
													 );
													 
					if (defined $numsentences_first{$docid}){
						if(!exists $maxnumsentences_first{$courseid}){
							$maxnumsentences_first{$courseid} = 0;
						}
						
						if(!exists $minnumsentences_first{$courseid}){
							$minnumsentences_first{$courseid} = 999999;
						}
						
						($maxnumsentences_first{$courseid}, $minnumsentences_first{$courseid}) 
										=	getMaxMin( $numsentences_first{$docid},
														$maxnumsentences_first{$courseid},
														$minnumsentences_first{$courseid},
														"num_sentences_first"
													 );
					}
				}
				else{
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
			}
			
			if($numq){
				if ($normalize) {
					($maxquestionmarks{$courseid}, $minquestionmarks{$courseid}) = 
										getMaxMin($numquestions{$docid},
												   $maxquestionmarks{$courseid},
												   $minquestionmarks{$courseid},
												   "num_questions"
												  );
				}
				else{
					($maxquestionmarks, $minquestionmarks) = getMaxMin($numquestions{$docid},
																	   $maxquestionmarks,
																	   $minquestionmarks,
																	   "num_questions"
																	  );
				}
			}
			
			if($senti){
				($maxmultiquestionmarks, $minmultiquestionmarks) 
														= getMaxMin($multiquestions{$docid},
																    $maxmultiquestionmarks,
																    $minmultiquestionmarks,
																	"sentiment"
																   );
			}
			
			if($numquotes){

				$numquotes{$docid} = $numquotes{$docid}/$numposts{$docid};
				($maxquotationmarks, $minquotationmarks) = getMaxMin($numquotes{$docid},
																	 $maxquotationmarks,
																	 $minquotationmarks,
																	 "num_quotes"
																    );
			}
			
			if($courseref ){
				if ($thread_length_nomalizer eq 0){
					die "Exception:  $docid \t $courseid: $coursematerialterms{$docid} \n";
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
					if ($normalize) {
						if(!exists $maxcoursemateraildensity{$courseid}){
							$maxcoursemateraildensity{$courseid} = 0.0;
						}
						
						if(!exists $mincoursemateraildensity{$courseid}){
							$mincoursemateraildensity{$courseid} = 999999.999;
						}

						($maxcoursemateraildensity{$courseid}, $mincoursemateraildensity{$courseid}) = 
														getMaxMin(  $coursematerialtermdensity{$docid},
																	$maxcoursemateraildensity{$courseid},
																	$mincoursemateraildensity{$courseid},
																	"courserefdensity"
																 );
						#######
						if(!exists $maxcoursemateraildensity_nkd{$courseid}){
							$maxcoursemateraildensity_nkd{$courseid} = 0.0;
						}
						
						if(!exists $mincoursemateraildensity_nkd{$courseid}){
							$mincoursemateraildensity_nkd{$courseid} = 999999.999;
						}
						
						($maxcoursemateraildensity_nkd{$courseid}, $mincoursemateraildensity_nkd{$courseid}) = 
														getMaxMin(  $coursematerialtermdensity_nkd{$docid},
																	$maxcoursemateraildensity_nkd{$courseid},
																	$mincoursemateraildensity_nkd{$courseid},
																	"courserefden_nkd"
																 );
						
						
						#######
						if(!exists $maxcoursemateraildensity_pfx{$courseid}){
							$maxcoursemateraildensity_pfx{$courseid} = 0.0;
						}
						
						if(!exists $mincoursemateraildensity_pfx{$courseid}){
							$mincoursemateraildensity_pfx{$courseid} = 999999.999;
						}
						($maxcoursemateraildensity_pfx{$courseid}, $mincoursemateraildensity_pfx{$courseid}) = 
														getMaxMin(  $coursematerialtermdensity_pfx{$docid},
																	$maxcoursemateraildensity_pfx{$courseid},
																	$mincoursemateraildensity_pfx{$courseid},
																	"courserefden_pfx"
																 );
						
						#######
						if(!exists $maxcoursemateraildensity_sfx{$courseid}){
							$maxcoursemateraildensity_sfx{$courseid} = 0.0;
						}
						
						if(!exists $mincoursemateraildensity_sfx{$courseid}){
							$mincoursemateraildensity_sfx{$courseid} = 999999.999;
						}						
						($maxcoursemateraildensity_sfx{$courseid}, $mincoursemateraildensity_sfx{$courseid}) = 
														getMaxMin(  $coursematerialtermdensity_sfx{$docid},
																	$maxcoursemateraildensity_sfx{$courseid},
																	$mincoursemateraildensity_sfx{$courseid},
																	"courserefden_sfx"
																 );
						
						#######
						if(!exists $maxcoursematerail{$courseid}){
							$maxcoursematerail{$courseid} = 0;
						}
						
						if(!exists $mincoursematerail{$courseid}){
							$mincoursematerail{$courseid} = 999999;
						}
						($maxcoursematerail{$courseid}, $mincoursematerail{$courseid}) =
														getMaxMin(  $coursematerialterms{$docid},
																	$maxcoursematerail{$courseid},
																	$mincoursematerail{$courseid},
																	"courseref_all"
																 );
								
						#######
						if(!exists $maxcoursematerail_nkd{$courseid}){
							$maxcoursematerail_nkd{$courseid} = 0;
						}
						
						if(!exists $mincoursematerail_nkd{$courseid}){
							$mincoursematerail_nkd{$courseid} = 999999;
						}						
						($maxcoursematerail_nkd{$courseid}, $mincoursematerail_nkd{$courseid}) =
														getMaxMin(  $coursematerialterms_nkd{$docid},
																	$maxcoursematerail_nkd{$courseid},
																	$mincoursematerail_nkd{$courseid},
																	"courseref_nkd"
																 );
						
						#######
						if(!exists $maxcoursematerail_pfx{$courseid}){
							$maxcoursematerail_pfx{$courseid} = 0;
						}
						
						if(!exists $mincoursematerail_pfx{$courseid}){
							$mincoursematerail_pfx{$courseid} = 999999;
						}		
						($maxcoursematerail_pfx{$courseid}, $mincoursematerail_pfx{$courseid}) =
														getMaxMin(  $coursematerialterms_pfx{$docid},
																	$maxcoursematerail_pfx{$courseid},
																	$mincoursematerail_pfx{$courseid},
																	"courseref_pfx"
																 );

						#######
						if(!exists $maxcoursematerail_sfx{$courseid}){
							$maxcoursematerail_sfx{$courseid} = 0;
						}
						
						if(!exists $mincoursematerail_sfx{$courseid}){
							$mincoursematerail_sfx{$courseid} = 999999;
						}	
						($maxcoursematerail_sfx{$courseid}, $mincoursematerail_sfx{$courseid}) =
														getMaxMin(  $coursematerialterms_sfx{$docid},
																	$maxcoursematerail_sfx{$courseid},
																	$mincoursematerail_sfx{$courseid},
																	"courseref_sfx"
																 );
					}
					else {
				
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
			}
			
			if(defined $nonterm_courseref){
				
				if($normalize){
					if(defined $num_urlref{$docid} ){
						if(!exists $maxnum_urlref{$courseid}){
							$maxnum_urlref{$courseid} = 0;
						}
						
						if(!exists $minnum_urlref{$courseid}){
							$minnum_urlref{$courseid} = 999999;
						}
						($maxnum_urlref{$courseid}, $minnum_urlref{$courseid}) 
											=	getMaxMin(  $num_urlref{$docid},
															$maxnum_urlref{$courseid},
															$minnum_urlref{$courseid},
															"courseref_url"
														 );
					}
					
					if(defined $num_urlrefinfirstpost{$docid} ){
						if(!exists $maxnum_urlreffirstpost{$courseid}){
							$maxnum_urlreffirstpost{$courseid} = 0;
						}
						
						if(!exists $minnum_urlreffirstpost{$courseid}){
							$minnum_urlreffirstpost{$courseid} = 999999;
						}
						($maxnum_urlreffirstpost, $minnum_urlreffirstpost) 
											= getMaxMin(	$num_urlrefinfirstpost{$docid},
															$maxnum_urlreffirstpost{$courseid},
															$minnum_urlreffirstpost{$courseid},
															"courseref_url_first"
														);
					}
				
					if(defined $num_timeref{$docid} ){
						if(!exists $maxnum_timeref{$courseid}){
							$maxnum_timeref{$courseid} = 0;
						}
						
						if(!exists $minnum_timeref{$courseid}){
							$minnum_timeref{$courseid} = 999999;
						}
						($maxnum_timeref{$courseid}, $minnum_timeref{$courseid})  
											=	getMaxMin(  $num_timeref{$docid},
															$maxnum_timeref{$courseid},
															$minnum_timeref{$courseid},
															"courseref_time"
														 );
					}
				
					if(defined $num_timereffirstpost{$docid} ){
						if(!exists $maxnum_timereffirstpost{$courseid}){
							$maxnum_timereffirstpost{$courseid} = 0;
						}
						
						if(!exists $minnum_timereffirstpost{$courseid}){
							$minnum_timereffirstpost{$courseid} = 999999;
						}
						($maxnum_timereffirstpost{$courseid}, $minnum_timereffirstpost{$courseid}) 
											=	getMaxMin(  $num_timereffirstpost{$docid},
															$maxnum_timereffirstpost{$courseid},
															$minnum_timereffirstpost{$courseid},
															"courseref_time_first"
											  			 );
					}
				}
				else{
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
			}
			
			if($affir && defined $affirmations{$docid} ){
				$affirtermdensity{$docid} = $affirmations{$docid}/$thread_length_nomalizer;
				
				if ($normalize) {
					if(!exists $maxaffir{$courseid}){
						$maxaffir{$courseid} = 0;
					}
					
					if(!exists $minaffir{$courseid}){
						$minaffir{$courseid} = 999999;
					}
					
					$maxaffir{$courseid} = ($affirmations{$docid} > $maxaffir{$courseid})?$affirmations{$docid}: $maxaffir{$courseid};
					$minaffir{$courseid} = ($affirmations{$docid} < $minaffir{$courseid})?$affirmations{$docid}: $minaffir{$courseid};

					if(!exists $maxaffirdensity{$courseid}){
						$maxaffirdensity{$courseid} = 0;
					}
					
					if(!exists $minaffirdensity{$courseid}){
						$minaffirdensity{$courseid} = 999999;
					}
					
					$maxaffirdensity{$courseid} = ($affirtermdensity{$docid} > $maxaffirdensity{$courseid})? $affirtermdensity{$docid}: $maxaffirdensity{$courseid};
					$minaffirdensity{$courseid} = ($affirtermdensity{$docid} < $minaffirdensity{$courseid})? $affirtermdensity{$docid}: $minaffirdensity{$courseid};
				}
				else{
					$maxaffir = ($affirmations{$docid} > $maxaffir)?$affirmations{$docid}: $maxaffir;
					$minaffir = ($affirmations{$docid} < $minaffir)?$affirmations{$docid}: $minaffir;
				
					$maxaffirdensity = ($affirtermdensity{$docid} > $maxaffirdensity)? $affirtermdensity{$docid}: $maxaffirdensity;
					$minaffirdensity = ($affirtermdensity{$docid} < $minaffirdensity)? $affirtermdensity{$docid}: $minaffirdensity;
				}
			}
			if($agree && defined $agreedisagree{$docid} ){
				$agreedisagreedensity{$docid} = $agreedisagree{$docid}/$thread_length_nomalizer;
								
				$maxagreedisagree = ($agreedisagree{$docid} > $maxagreedisagree)?$agreedisagree{$docid}: $maxagreedisagree;
				$minagreedisagree = ($agreedisagree{$docid} < $minagreedisagree)?$agreedisagree{$docid}: $minagreedisagree;
				
				$maxagreedisagreedensity = ($agreedisagreedensity{$docid} > $maxagreedisagreedensity)? $agreedisagreedensity{$docid}: $maxagreedisagreedensity;
				$minagreedisagreedensity = ($agreedisagreedensity{$docid} < $minagreedisagreedensity)? $agreedisagreedensity{$docid}: $minagreedisagreedensity;
			}			
			if($hedging && defined $hedgeterm{$docid} ){
				$hedgetermdensity{$docid} = $hedgeterm{$docid}/$thread_length_nomalizer;
				$maxhedging = ($hedgeterm{$docid} > $maxhedging)?$hedgeterm{$docid}: $maxhedging;
				$minhedging = ($hedgeterm{$docid} < $minhedging)?$hedgeterm{$docid}: $minhedging;
				
				$maxhedgingdensity = ($hedgetermdensity{$docid} > $maxhedgingdensity)? $hedgetermdensity{$docid}: $maxhedgingdensity;
				$minhedgingdensity = ($hedgetermdensity{$docid} < $minhedgingdensity)? $hedgetermdensity{$docid}: $minhedgingdensity;
			}
			
		}
		
		#close $pdtbout;
	}
	# first pass ends
	
	if($posttime){
		print "MAX & MIN time diffs: $maxtimediff \t $mintimediff\n";
		checkmaxminexception($maxtimediff,$mintimediff, 'posttime');
	}
	
	if($threadtime){
		print "\n MAX & MIN thread Start time : $maxthreadStartTime \t $minthreadStartTime";
		checkmaxminexception($maxthreadStartTime,$minthreadStartTime,'threadStime');
		
		print "\n MAX & MIN thread End time  : $maxthreadEndTime \t $minthreadEndTime";
		checkmaxminexception($maxthreadEndTime,$minthreadEndTime,'threadEtime');
		
		# find max and minimum times across both thread start and end times
		$maxthreadStartTime = ($maxthreadStartTime > $maxthreadEndTime) ? $maxthreadStartTime: $maxthreadEndTime;
		$maxthreadEndTime = $maxthreadStartTime;
		
		$minthreadStartTime = ($minthreadStartTime < $minthreadEndTime) ? $minthreadStartTime: $minthreadEndTime;
		$minthreadEndTime = $minthreadStartTime;
		
		print "\n Fixed: MAX & MIN thread Start time : $maxthreadStartTime \t $minthreadStartTime";
		print "\n Fixed: MAX & MIN thread End time  : $maxthreadEndTime \t $minthreadEndTime";
	}
	
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
	
	if($numq){
		print "MAX & MIN num questions: $maxquestionmarks \t $minquestionmarks\n";
		checkmaxminexception($maxquestionmarks , $minquestionmarks, 'number of questions');
	}
	
	if($senti && (keys %multiquestions ne 0) ){
		print "MAX & MIN num questions: $maxmultiquestionmarks \t $minmultiquestionmarks\n";
		checkmaxminexception($maxmultiquestionmarks , $minmultiquestionmarks, 'sentiments');
	}
	
	if($numquotes){
		print "MAX & MIN num quotations: $maxquotationmarks \t $minquotationmarks\n";
		checkmaxminexception($maxquotationmarks, $minquotationmarks, 'number of quotes');
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
	
	if($hedging){
		print "MAX & MIN hedging mentions: $maxhedging \t $minhedging\n";
		print "MAX & MIN hedging density: $maxhedgingdensity \t $minhedgingdensity\n";
		checkmaxminexception($maxhedging, $minhedging, 'hedging');
		checkmaxminexception($maxhedgingdensity, $minhedgingdensity, 'avg hedging');
	}
	
	if($affir){
		print "MAX & MIN affir mentions: $maxaffir \t $minaffir\n";
		print "MAX & MIN affir density : $maxaffirdensity \t $minaffirdensity\n";
		checkmaxminexception($maxaffir, $minaffir, 'affirmations');
		checkmaxminexception($maxaffirdensity, $minaffirdensity, 'affirmation dentsity');
	}
	
	if($prof){
		print "MAX & MIN prof mentions: $maxprofmentions \t $minprofmentions\n";
		print "MAX & MIN prof density: $maxavgprofmentions \t $minavgprofmentions\n";
		print "MAX & MIN staff mentions: $maxstaffmentions \t $minstaffmentions\n";
		print "MAX & MIN staff density: $maxavgstaffmentions \t $minavgstaffmentions\n";
		checkmaxminexception($maxprofmentions, $minprofmentions, 'profmentions');
		checkmaxminexception($maxavgprofmentions, $minavgprofmentions, 'avgprofmentions');
		checkmaxminexception($maxstaffmentions, $minstaffmentions, 'staffmentions');
		checkmaxminexception($maxavgstaffmentions, $minavgstaffmentions, 'staffavgmentions');
	}
	
	if($pdtb){
		print "MAX & MIN pdtb e: $maxpdtbexpansion \t $minpdtbexpansion\n";
		print "MAX & MIN pdtb c: $maxpdtbcontingency \t $minpdtbcontingency\n";
		print "MAX & MIN pdtb com: $maxpdtbcontrast \t $minpdtbcontrast\n";
		print "MAX & MIN pdtb tem: $maxpdtbtemporal \t $minpdtbtemporal\n";
		# checkmaxminexception($maxprofmentions, $minprofmentions, 'profmentions');
		# checkmaxminexception($maxavgprofmentions, $minavgprofmentions, 'avgprofmentions');
		# checkmaxminexception($maxstaffmentions, $minstaffmentions, 'staffmentions');
		# checkmaxminexception($maxavgstaffmentions, $minavgstaffmentions, 'staffavgmentions');
	}
	
	my %nontermfeatures = ();
	my $maxtermfeaturecount = 0;
	
	# find maxnumber of unigram features.
	foreach my $category_id (keys %$threadcats){
		my $tftab			 =	$threadcats->{$category_id}{'tftab'};
		my $max_termid 		+= @{$dbh->selectcol_arrayref("select max(termid) from $tftab")}[0];
		$maxtermfeaturecount = ($max_termid > $maxtermfeaturecount) ? $max_termid : $maxtermfeaturecount;
	}
	print "\n Maxtermfeaturecount: $maxtermfeaturecount";
	
	# compute tf-IDFs
	my $termWeights;
	if($unigrams){
		if($normalize){
			my $total_num_courses = scalar (@courses);
			my $normalisedTermIDFs = averageTermIDF($terms,$total_num_courses);
		
			$termWeights = computeTFIDFs(	\%termFrequencies, 
											$normalisedTermIDFs,	## sends averaged IDF weights
											$term_course,
											$num_threads_coursewise,
											$corpus_type, 
											$dbh,
											$tftype,	#uses normalised tf without idf
											$coursewiseIDF,
											$normalize
										);
		}
		else{
			$termWeights = computeTFIDFs(	\%termFrequencies,
											$terms, 	## sends per course IDF weights 
											$term_course,
											$total_num_threads,
											$corpus_type,
											$dbh,
											$tftype,	#uses normalised tf without idf
											$coursewiseIDF,
											$normalize
										);
		}
		
		if (keys %{$termWeights} ==0 ){
			die "\n termweights matrix is empty ";
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
			print LOG "\n Warning: Second pass! No threads found $category_id";
			print "\n Warning: Second pass! No threads found $category_id";
			exit(0);
		}
		
		print $FEXTRACT "Writing feature file for potentially " .
											scalar(@$threads)	.
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
				print LOG "\n $category_id $posttable $commenttable $threads";
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
					#warn "\nterm_vector is empty! for $threadid in $courseid";
					print $FEXTRACT "term_vector is empty! for $threadid in $courseid\n";
					next;
				}
			}
			
			my $nontermfeaturecount = $maxtermfeaturecount;
			
			if($tprop){
				#print "adding thread length feature..\n";
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
				##print "calling normalize for $threadid \t $courseid \t $label \t $docid \n";
				$sum_of_squares = sumOfSquares( $term_vector );
			
				if ( $sum_of_squares == 0 || !defined $sum_of_squares ){
					print $FEXTRACT "Exception: sum_of_square is undef or zero $threadid \t $courseid \t $label \t $docid";
					die "Exception: sum_of_square is undef or zero $threadid \t $courseid \t $label \t $docid";
				}
				
				#lnorm of term vector. Scales to 0 to 1 range. 
				#Turns term vector into a unit vector
				$sum_of_squares = sqrt($sum_of_squares);		
				foreach my $tid (keys %$term_vector){
					$term_vector->{$tid} = $term_vector->{$tid} / $sum_of_squares;
				}
			}

		    if($pdtb){
				#print "adding pdtb relation feature..\n";
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
						print "\n bad relation. $docid \t $nontermfeaturecount \t $relation";
						exit(0);
					}
				}
			}
			
			if($titlewords){
				$titlewordsth->execute($docid) or die "execute failed titlewordqry";
				my $title = @{$titlewordsth->fetchrow_arrayref()}[0];				
				my $titleunigrams		= extractNgrams($title, 1, 0, 1);
				if(keys %$titleunigrams <= 0) {	
					$nontermfeaturecount++;
					$term_vector->{$nontermfeaturecount}	= undef;
					$nontermfeaturecount++;
					$term_vector->{$nontermfeaturecount}	= undef;
				}
				else{
					my ($lecture,$otherwords)	= getTitleFeatures($titleunigrams);
					$nontermfeaturecount++;
					$term_vector->{$nontermfeaturecount}	= $lecture;
					$nontermfeaturecount++;
					$term_vector->{$nontermfeaturecount}	= $otherwords;
				}
				$nontermfeatures{$nontermfeaturecount}	= 'title_words';
			}
			
			if($tlength){
				$nontermfeaturecount++;
				$term_vector->{$nontermfeaturecount}	= $numposts{$docid};
				$nontermfeatures{$nontermfeaturecount}	= 'tlen:#posts+#comments';
			}
			
			if($forumid){
				$nontermfeaturecount++;
				#$term_vector->{$nontermfeaturecount}	= $forumid_number;
				$nontermfeatures{$nontermfeaturecount}	= 'forumid';
				foreach my $code (%forumid_vector){
					$nontermfeaturecount++;
					$term_vector->{$nontermfeaturecount} = ($code == $forumid_number)? 1: 0;
				}
			}
			
			if($threadtime){
				$nontermfeaturecount++;
				if (defined $threadStartTime{$docid}){
					my $normalised = $threadStartTime{$docid};
					if(($maxthreadStartTime - $minthreadStartTime) != 0){
						$normalised = ($threadStartTime{$docid} - $minthreadStartTime)/ 
										($maxthreadStartTime - $minthreadStartTime);
					}
					$term_vector->{$nontermfeaturecount}	= $normalised;
				}
				$nontermfeatures{$nontermfeaturecount}	= 'thread_stime';
				
				$nontermfeaturecount++;
				if (defined $threadEndTime{$docid}){
					my $normalised	= $threadEndTime{$docid};
					if(($maxthreadStartTime - $minthreadStartTime) != 0){
						$normalised = ($threadEndTime{$docid} - $minthreadEndTime)/ 
										($maxthreadEndTime - $minthreadEndTime);
					}
					$term_vector->{$nontermfeaturecount}	= $normalised;
				}
				$nontermfeatures{$nontermfeaturecount}	= 'thread_etime';
			}
			
			if($userfeatures){
				my $isOPanaon = isThreadOPAnon($dbh,$docid);
				$nontermfeaturecount++;
				if(defined $isOPanaon){
					$term_vector->{$nontermfeaturecount}	= $isOPanaon;
				}else{
					$term_vector->{$nontermfeaturecount}	= undef;
				}
				$nontermfeatures{$nontermfeaturecount}	= 'user_anaon';
			}
			
			if($solvedness){
				my $approved = isThreadApproved($dbh,$docid);
				my $resolved = isThreadResolved($dbh,$docid);
				my $deleted  = isThreadDeleted($dbh,$docid);
				$nontermfeaturecount++;
				if(defined $approved){
					$term_vector->{$nontermfeaturecount}	= $approved;
				}
				else{
					$term_vector->{$nontermfeaturecount}	=  undef;
				}
				$nontermfeatures{$nontermfeaturecount}	= 'approved';
				
				$nontermfeaturecount++;
				if(defined $resolved){
					$term_vector->{$nontermfeaturecount}	= $resolved;
				}
				else{
					$term_vector->{$nontermfeaturecount}	=  undef;
				}
				$nontermfeatures{$nontermfeaturecount}	= 'resolved';
				
				$nontermfeaturecount++;
				if(defined $deleted){
					$term_vector->{$nontermfeaturecount}	= $deleted;
				}
				else{
					$term_vector->{$nontermfeaturecount}	=  undef;
				}
				$nontermfeatures{$nontermfeaturecount}	= 'deleted';
			}
			
			if($forumtype){
				#print "\nadding forumtype feature.";
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

			if($assessmentwc){
				$nontermfeaturecount++;
				if(defined $assessmentwords{$docid}){
					my $normalised = $assessmentwords{$docid};
					if(($maxassessmentwords - $minassessmentwords) != 0){
						$normalised = ($assessmentwords{$docid} - $minassessmentwords) / 
											($maxassessmentwords - $minassessmentwords);
					}
					$term_vector->{$nontermfeaturecount}	= $normalised;
				}
				else{
					$term_vector->{$nontermfeaturecount}	= undef;
				}
				$nontermfeatures{$nontermfeaturecount}	= 'wc_asses';
			}
			
			if($problemwc){
				$nontermfeaturecount++;
				if(defined $problemwords{$docid}){
					my $normalised = $problemwords{$docid};
					if(($maxproblemwords - $minproblemwords) != 0){
						$normalised = ($problemwords{$docid} - $minproblemwords) / 
												($maxproblemwords	- $minproblemwords);
					}
					$term_vector->{$nontermfeaturecount}	= $normalised;
				}
				else{
					$term_vector->{$nontermfeaturecount}	= undef;
				}
				$nontermfeatures{$nontermfeaturecount}	= 'wc_problem';
			}
			
			if($conclusionwc){
				$nontermfeaturecount++;
				if(defined $conclusionwords{$docid}){
					my $normalised = $conclusionwords{$docid};
					if(($maxconclusionwords - $minconclusionwords) != 0){
						$normalised = ($conclusionwords{$docid} - $minconclusionwords) /
											($maxconclusionwords - $minconclusionwords);
					}
					$term_vector->{$nontermfeaturecount}	= $normalised;
				}
				else{
					$term_vector->{$nontermfeaturecount}	= undef;
				}
				$nontermfeatures{$nontermfeaturecount}	= 'wc_conc';

			}
			
			if($requestwc){
				$nontermfeaturecount++;
				if(defined $requestwords{$docid}){
					my $normalised = $requestwords{$docid};
					if(($maxrequestwords - $minrequestwords) != 0){
						$normalised = ($requestwords{$docid} - $minrequestwords) /
												($maxrequestwords - $minrequestwords);
					}
					$term_vector->{$nontermfeaturecount}	= $normalised;
				}
				else{
					$term_vector->{$nontermfeaturecount}	= undef;
				}
				$nontermfeatures{$nontermfeaturecount}	= 'wc_request';
			}
			
			if($numw){
				#print "\nadding num_words feature.";
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
				#print "\n Adding num_sentences feature";
				if($normalize){
					$nontermfeaturecount++;
					$nontermfeatures{$nontermfeaturecount} = 'numw:# sentences';
					if(defined $numsentences{$docid}){
						$term_vector->{$nontermfeaturecount} = 
											  maxminNorm(
															$numsentences{$docid}, 
															$maxnumsentences{$courseid}, 
															$minnumsentences{$courseid}
														);
					}
					
					$nontermfeaturecount++;
					$nontermfeatures{$nontermfeaturecount} = 'numw:avg. # sentences per post';
					if(defined $avgnumsentences{$docid}){
						$term_vector->{$nontermfeaturecount} = 
												maxminNorm(
															$avgnumsentences{$docid}, 
															$maxavgnumsentences{$courseid}, 
															$minavgnumsentences{$courseid}
														  );
					}
					
					$nontermfeaturecount++;
					$nontermfeatures{$nontermfeaturecount} = 'numw:# sentences in 1st post';
					if(defined $numsentences_first{$docid}){
						$term_vector->{$nontermfeaturecount} = 
												maxminNorm(
															$numsentences_first{$docid}, 
															$maxnumsentences_first{$courseid}, 
															$minnumsentences_first{$courseid}
														  );
					}
				}
				else{
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
			}
			
			if($posttime){
				#print "\n adding posttime feature";
				$nontermfeaturecount++;
				if(defined $meanposttimediff{$docid}){
					my $normalised_timediff = ($meanposttimediff{$docid} - $mintimediff)/ ($maxtimediff - $mintimediff);
					$term_vector->{$nontermfeaturecount} = $normalised_timediff;
				}
				$nontermfeatures{$nontermfeaturecount}	= 'time_diffs';
			}

			if($question){
				print "\n adding question feature";
				$nontermfeaturecount++;
				$term_vector->{$nontermfeaturecount} = $isfirstpostquestion;
				$nontermfeatures{$nontermfeaturecount} = 'ques: 1st post question';
			}
			
			if($numq){
				print "\n adding num questions feature";
				$nontermfeaturecount++;
				$nontermfeatures{$nontermfeaturecount} = 'ques:# questions';
				if(defined $numquestions{$docid}){
					my $normalised = ($numquestions{$docid} - $minquestionmarks)/ ($maxquestionmarks - $minquestionmarks);
					$term_vector->{$nontermfeaturecount} = $normalised;
				}
			}
			
			if($senti){
				print "\n adding sentiment feature";
				$nontermfeaturecount++;
				$nontermfeatures{$nontermfeaturecount} = 'senti: #succesive ? or letters ';
				my $max_minus_min = ($maxmultiquestionmarks - $minmultiquestionmarks);
				if(defined $multiquestions{$docid} && $max_minus_min ne 0){
					my $normalised = ($multiquestions{$docid} - $minmultiquestionmarks)/$max_minus_min;
					$term_vector->{$nontermfeaturecount} = $normalised;
				}
			}
			
			if($numquotes){
				#print "\n adding num quotes feature";
				$nontermfeaturecount++;
				$nontermfeatures{$nontermfeaturecount} = 'quotes:# quotes';
				if(defined $numquotes{$docid}){
					my $normalised = ($numquotes{$docid} - $minquotationmarks)/ ($maxquotationmarks - $minquotationmarks);
					$term_vector->{$nontermfeaturecount} = $normalised;
				}
			}
			
			if($prof){
				#print "\n adding prof mention feature";
				$nontermfeaturecount++;
				$nontermfeatures{$nontermfeaturecount} = 'profmention';
				if(defined $profmention{$docid}){
					my $normalised = ($profmention{$docid} - $minprofmentions)/ ($maxprofmentions-$minprofmentions);
					$term_vector->{$nontermfeaturecount} = $normalised;
					$nontermfeaturecount++;
				
					$normalised = ($profmentiondensity{$docid} - $minavgprofmentions)/ ($maxavgprofmentions-$minavgprofmentions);
					$term_vector->{$nontermfeaturecount} = $normalised;
					$nontermfeatures{$nontermfeaturecount} = 'profmentiondensity';
				}
				
				$nontermfeaturecount++;
				$nontermfeatures{$nontermfeaturecount} = 'staffmention';
				if(defined $staffmention{$docid}){
					my $normalised = ($staffmention{$docid} - $minstaffmentions)/ ($maxstaffmentions-$minstaffmentions);
					$term_vector->{$nontermfeaturecount} = $normalised;
					$nontermfeaturecount++;
				
					$normalised = ($staffmentiondensity{$docid} - $minavgstaffmentions)/ ($maxavgstaffmentions-$minavgstaffmentions);
					$term_vector->{$nontermfeaturecount} = $normalised;
					$nontermfeatures{$nontermfeaturecount} = 'staffmentiondensity';
				}
			}
			
			if($courseref){
				#print "\n adding courseref mention feature";
				if ($normalize){
					$nontermfeaturecount++;
					$nontermfeatures{$nontermfeaturecount} = 'courseref_all';
					if(defined $coursematerialterms{$docid}){
						$term_vector->{$nontermfeaturecount} = maxminNorm(
																			$coursematerialterms{$docid}, 
																			$maxcoursematerail{$courseid}, 
																			$mincoursematerail{$courseid}
																		 );
						
						$nontermfeaturecount++;

						$term_vector->{$nontermfeaturecount} = maxminNorm(
																			$coursematerialtermdensity{$docid}, 
																			$maxcoursemateraildensity{$courseid}, 
																			$mincoursemateraildensity{$courseid}
																		 );
						$nontermfeatures{$nontermfeaturecount} = 'courserefdensity';
					}
					else{
						$nontermfeaturecount++;
						$nontermfeatures{$nontermfeaturecount} = 'courserefdensity';
					}
					
					$nontermfeaturecount++;
					$nontermfeatures{$nontermfeaturecount} = 'courseref_nkd';
					if(defined $coursematerialterms_nkd{$docid}){
						$term_vector->{$nontermfeaturecount} = maxminNorm(
																			$coursematerialterms_nkd{$docid}, 
																			$maxcoursematerail_nkd{$courseid}, 
																			$mincoursematerail_nkd{$courseid}
																		 );
						$nontermfeaturecount++;
						
						$term_vector->{$nontermfeaturecount} = maxminNorm(
																			$coursematerialtermdensity_nkd{$docid},
																			$maxcoursemateraildensity_nkd{$courseid}, 
																			$mincoursemateraildensity_nkd{$courseid}
																		 );
						$nontermfeatures{$nontermfeaturecount} = 'courserefdensity_nkd';
					}
					else{
						$nontermfeaturecount++;
						$nontermfeatures{$nontermfeaturecount} = 'courserefdensity_nkd';
					}
					
					$nontermfeaturecount++;
					$nontermfeatures{$nontermfeaturecount} = 'courseref_pfx';
					if(defined $coursematerialterms_pfx{$docid}){
						$term_vector->{$nontermfeaturecount} = maxminNorm(
																			$coursematerialterms_pfx{$docid},
																			$maxcoursematerail_pfx{$courseid},
																			$mincoursematerail_pfx{$courseid}
																		 );						
						
						$nontermfeaturecount++;

						$term_vector->{$nontermfeaturecount} = maxminNorm(
																			$coursematerialtermdensity_pfx{$docid},
																			$maxcoursemateraildensity_pfx{$courseid},
																			$mincoursemateraildensity_pfx{$courseid}
																		 );
						$nontermfeatures{$nontermfeaturecount} = 'courserefdensity_pfx';
					}
					else{
						$nontermfeaturecount++;
						$nontermfeatures{$nontermfeaturecount} = 'courserefdensity_pfx';
					}
					
					$nontermfeaturecount++;
					$nontermfeatures{$nontermfeaturecount} = 'courseref_sfx';
					if(defined $coursematerialterms_sfx{$docid}){
						$term_vector->{$nontermfeaturecount} = maxminNorm(
																			$coursematerialterms_sfx{$docid},
																			$maxcoursematerail_sfx{$courseid},
																			$mincoursematerail_sfx{$courseid}
																		 );
						$nontermfeaturecount++;
						
						$term_vector->{$nontermfeaturecount} = maxminNorm(
																			$coursematerialtermdensity_sfx{$docid},
																			$maxcoursemateraildensity_sfx{$courseid},
																			$mincoursemateraildensity_sfx{$courseid}
																		 );
						$nontermfeatures{$nontermfeaturecount} = 'courserefdensity_sfx';
					}
					else{
						$nontermfeaturecount++;
						$nontermfeatures{$nontermfeaturecount} = 'courserefdensity_sfx';
					}
				}
				else {
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
			}
			
			if($nonterm_courseref){
				#print "\n adding nonterm_courseref mention feature";
				if($normalize){
					$nontermfeaturecount++;
					$nontermfeatures{$nontermfeaturecount} = 'urlref';
					if(defined $num_urlref{$docid}){
						$term_vector->{$nontermfeaturecount} = maxminNorm(
																			$num_urlref{$docid},
																			$maxnum_urlref{$courseid},
																			$minnum_urlref{$courseid}
																		 );
					}				
					
					$nontermfeaturecount++;
					$nontermfeatures{$nontermfeaturecount} = 'timeref';				
					if(defined $num_timeref{$docid}){
						$term_vector->{$nontermfeaturecount} = maxminNorm(
																			$num_timeref{$docid},
																			$maxnum_timeref{$courseid},
																			$minnum_timeref{$courseid}
																		 );
					}
					
					$nontermfeaturecount++;
					$nontermfeatures{$nontermfeaturecount} = 'urlreffirstpost';
					if(defined $num_urlrefinfirstpost{$docid}){
						$term_vector->{$nontermfeaturecount} = maxminNorm(
																			$num_urlrefinfirstpost{$docid},
																			$maxnum_urlreffirstpost{$courseid},
																			$minnum_urlreffirstpost{$courseid} 
																		 );
					}

					$nontermfeaturecount++;
					$nontermfeatures{$nontermfeaturecount} = 'timereffirstpost';
					if(defined $num_timereffirstpost{$docid}){
						$term_vector->{$nontermfeaturecount} = maxminNorm(
																			$num_timereffirstpost{$docid}, 
																			$maxnum_timereffirstpost{$courseid},
																			$minnum_timereffirstpost{$courseid}
																		 );
					}

					$nontermfeaturecount++;
					$nontermfeatures{$nontermfeaturecount} = 'equation';				
					if(defined $hasEquation{$docid}){					
						$term_vector->{$nontermfeaturecount} = $hasEquation{$docid};
					}
				}
				else{
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
						if ($denom != 0){
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
						if ($denom != 0){
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
			}
						
			if($affir){
				#print "\n adding affir mention feature";
				if($normalize){
					$nontermfeaturecount++;
					$nontermfeatures{$nontermfeaturecount} = '#affirmations';
					if(defined $affirmations{$docid}){
						$term_vector->{$nontermfeaturecount} = maxminNorm(
																			$affirmations{$docid},
																			$maxaffir{$courseid},
																			$minaffir{$courseid}
																		 );						
						$nontermfeaturecount++;
						$nontermfeatures{$nontermfeaturecount} = 'affirmations density';

						$term_vector->{$nontermfeaturecount} = maxminNorm(
																			$affirtermdensity{$docid},
																			$maxaffirdensity{$courseid},
																			$minaffirdensity{$courseid}
																		 );
					}
					else{
						$nontermfeaturecount++;
						$nontermfeatures{$nontermfeaturecount} = 'affirmations density';
					}
				}
				else{
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
			}
			
			if($agree){
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
			
			if($hedging){
				$nontermfeaturecount++;
				$nontermfeatures{$nontermfeaturecount} = '#hedging';
				if(defined $hedgeterm{$docid}){
					my $normalised = ($hedgeterm{$docid} - $minhedging) / ($maxhedging - $minhedging);
					$term_vector->{$nontermfeaturecount} = $normalised;
					$nontermfeaturecount++;
					$nontermfeatures{$nontermfeaturecount} = 'hedgingdensity';
					$normalised = ($hedgetermdensity{$docid} - $minhedgingdensity)/ ($maxhedgingdensity - $minhedgingdensity);
					$term_vector->{$nontermfeaturecount} = $normalised;				
				}
				else{
					$nontermfeaturecount++;
					$nontermfeatures{$nontermfeaturecount} = 'hedgingdensity';
				}
			}
			
			# if($votes){
				# $votes->{$courseid}{$forumid}{$threadid}{$postid}
			# }
			#record the term vector for this thread
			$termvectors_collated{$docid} = [$serialid, $label, $forumname, $term_vector];
		}
		
	}
	
	# print all features for this thread to file
	my $thread_couter = 0; my $inter_thread_couter	= 	0;
	
	foreach my $docid ( sort {$a<=>$b} keys %termvectors_collated){
		my @thread 		= @{$termvectors_collated{$docid}};
		my $serialid	= $thread[0];
		my $label 		= $thread[1];
		my $forumname	= $thread[2];
		my $term_vector = $thread[3];
		my @actions		= (1,2,3,4);
		
		$thread_couter++;
		if ($print_format eq 'vw'){
			
			if($label eq '-1')	{	next;	}
			
			my $probability_of_intervention		= sprintf ("%.3f",(1/4));
			my $current_action_cost	= 	($label eq '+1')? 0 : 1;
			
			my $current_action		= 	($forumname eq 'Errata')? 1 :(
											($forumname eq 'Exam')? 2:(
												($forumname eq 'Lecture')? 3 :(
													($forumname eq 'Homework')? 4 :'Excepetion-- $forumname')));
											
			print $FH "$current_action\:$current_action_cost\:$probability_of_intervention";
			foreach my $action (@actions){
				if($action == $current_action){
					next;
				}
				print $FH "$action\:1\:$probability_of_intervention";
			}
				print $FH " |";
				
			#print features
			foreach my $tid (sort { $a <=> $b } (keys %$term_vector)){
				#printf $FH "$terms->{$tid}{'term'}:%.3f ",$term_vector->{$tid};
				printf $FH "$tid:%.3f ",$term_vector->{$tid};
			}
		}
		elsif ($print_format eq 'oaa'){
			#if($label eq '-1')	{	next;	}
			my $current_action		= 	($forumname eq 'Errata')? 1 :(
											($forumname eq 'Exam')? 2:(
												($forumname eq 'Lecture')? 3 :(
													($forumname eq 'Homework')? 4 :'Excepetion-- $forumname')));
			print $FH "$current_action";
			print $FH " |";
				
			#print features
			foreach my $tid (sort { $a <=> $b } (keys %$term_vector)){
				#printf $FH "$terms->{$tid}{'term'}:%.3f ",$term_vector->{$tid};
				printf $FH "$tid:%.3f ",$term_vector->{$tid};
			}

		}
		else{
			print $FH "$docid\t $label\t";
			foreach my $tid (sort { $a <=> $b } (keys %$term_vector)){
				if(!defined $term_vector->{$tid}){	next;	}
				printf $FH  "$tid:%.3f\t",$term_vector->{$tid};
			}
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

sub getThreadEtime{
	my ($dbh,$threadid, $courseid,$label) = @_;
	
	my $posttable = 'post'; 
	my $cmnttable = 'comment';
	
	if ($label eq '+1'){
		$posttable = 'post2';
		$cmnttable = 'comment2';
	}
	my $lastposttime = @{$dbh->selectcol_arrayref("select post_time from $posttable where thread_id = $threadid and courseid = \'$courseid\'  order by post_time desc limit 1")}[0];
	my $lastcmnttime = @{$dbh->selectcol_arrayref("select post_time from $cmnttable where thread_id = $threadid and courseid = \'$courseid\'  order by post_time desc limit 1")}[0];
	if(!defined $lastcmnttime){ $lastcmnttime = 0;}
	
	if(!defined $lastposttime && $label eq '+1'){
		print "\nWarning: No thread-END-time for $threadid \t $courseid \t in $posttable";
		return 0;
	}
	if(!defined $lastposttime && $label eq '-1'){ 
		print "\nException: No thread-end-time for $threadid \t $courseid \t in $posttable";
		exit(0);
	}
	#elsif(!defined $lastposttime && !defined $lastcmnttime){return 0;}
	my $time = ($lastposttime > $lastcmnttime)? $lastposttime :$lastcmnttime;
	return $time;
}

sub getThreadStime{
	my ($dbh,$threadid, $courseid,$label) = @_;
	my $posttable = 'post'; ;
	
	if ($label eq '+1'){
		$posttable = 'post2';
	}	
	my $time = @{$dbh->selectcol_arrayref("select post_time from $posttable where thread_id = $threadid and courseid = \'$courseid\' and original =1")}[0];
	
	if(!defined $time && $label eq '+1'){
		print "\nWarning: No thread-START-time for $threadid \t $courseid \t in $posttable";
		return 0;
	}
	elsif(!defined $time && $label eq '-1'){
		print "\nException: No thread-start-time for $threadid \t $courseid \t in $posttable";
		exit(0);
	}
	
	return $time;
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
	my ($termFrequencies, $termIDFs, $term_course, $num_threads, $corpus_type, $dbh, $tftype, $coursewiseIDF, $normlize) = @_;
	
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
	my $tot_num_threads = 0;
	my $tot_num_courses = 0;
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
	
	if ( $normlize ){	
		my @courses = keys %$termFrequencies;
		$max_tfs = getMaxTfs($dbh, \@courses);
	}
	
	if ( !$normlize ){	
		$tot_num_courses = keys %$termFrequencies;
		foreach my $courseid ( keys %$termFrequencies ){
			$tot_num_threads += keys %{$termFrequencies->{$courseid}};
		}
	}
	
	foreach my $courseid ( keys %$termFrequencies ){
		foreach my $threadid ( keys $termFrequencies->{$courseid} ){
			my $maxtf_this_thread = $maxtf_thread->{$courseid}{$threadid};
			if(keys $termFrequencies->{$courseid}{$threadid} == 0){
				print "\nNo terms found for $courseid \t $threadid";
			}
			foreach my $termid ( keys $termFrequencies->{$courseid}{$threadid} ){
				###tf
				my $tf = $termFrequencies->{$courseid}{$threadid}{$termid};
				
				if ( $booltf ){
					$tf = 1;
				}elsif ( $puretf ){
					$tf = $puretf_param +  ( (1-$puretf_param) * ($tf / $maxtf_this_thread) )
				}elsif( $logtf){
					$tf = 1 +  log($tf)/log(10);
				}elsif ( $normlize ){
					$tf = $tf / $max_tfs->{$courseid}{$termid};
				}
				
				###idf
				if ($normlize){
					my $idf = $termIDFs->{$courseid}{$termid};
					if (!defined $idf){
						die "\n computeTFIDFs: Exception: idf not read. $courseid. $normlize ";
					}
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
				elsif($coursewiseIDF){
					print "\n doing cswise IDF\n"; exit(0);
					foreach my $term (keys %{$termIDFs->{$courseid}{$termid}}){
						my $idf = 0;
						my $df = $termIDFs->{$courseid}{$termid}{$term};
						my $num_threads = keys %{$termFrequencies->{$courseid}};
						my $num_courses_term = keys %{$term_course->{$termid}};
						if ($df == 0){	$idf = 0;	}
						else {	
							my $course_spread_factor = $alpha * ($num_courses_term / $tot_num_courses);
							if ($alpha == 0){
								$idf =  ($num_threads/$df);
								#$idf =  log($num_threads/$df)/log(10);
							}
							#elsif ($alpha == 1){
								# $idf =  $course_spread_factor;
							#}
							else{
								$idf =  ($num_threads/$df) * $course_spread_factor;
							}
						}
						
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
				else{
					my $df = $termIDFs->{$termid}{'sumdf'};
					my $term = $termIDFs->{$termid}{'term'};
					my $num_courses_term = keys %{$term_course->{$termid}};
					my $course_spread_factor = ($num_courses_term / $tot_num_courses);
					my $idf = 0;
					
					if (!defined $term){
						print "\n computeTFIDFs: Exception: $termid \t $term not found in TF table ";
						exit(0);
					}
					if (!defined $df){
						print "\n computeTFIDFs: Exception: idf not read. $termid \t $term \t $courseid \t $normlize ";
						exit(0);
					}

					if (!defined $num_threads){
						die "\n computeTFIDFs: Exception: num_threads not read. $courseid. $normlize. ";
					}
					
					if ($df == 0){	$idf = 0;	}
					elsif($tot_num_threads==0 || $course_spread_factor ==0){
						print "\nZero exception:  \t $course_spread_factor \t $num_courses_term";
					}
					else{
						#$idf = log($tot_num_threads/ $df)/log(10);
						$idf = $tot_num_threads / $df;
						#$idf = log( ($tot_num_threads/ $df) * $course_spread_factor)/log(10);
						#$idf = ($tot_num_threads/ $df) * $course_spread_factor;
					}

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
				#debug
				# open (my $DEBUG, ">$path/debug_file.txt") or die "cannot open $path/debug_file.txt";
				# print $DEBUG "\n $termid \t $term \t $df \t $idf";
				# close $DEBUG;
				# exit(0);
				#debug ends
			}
			
		}
	}
	return \%termWeights;
}

sub getMaxThreadTF{
	my ($termFrequencies) = @_;
	my %maxtf = ();
		foreach my $courseid ( keys %$termFrequencies ){
			foreach my $threadid ( keys $termFrequencies->{$courseid} ){
				my $maxtf = 0.0;
				foreach my $termid ( keys $termFrequencies->{$courseid}{$threadid} ){
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
	
	# my @post_times = @{$posttimesth->fetchall_arrayref()};
	# my @comment_times = @{$commenttimesth->fetchall_arrayref()};
	
	# if ( scalar @post_times == 0){ 
		# return -1;
	# }
	
	# my @times;
	# foreach (@post_times){		
		# push @times, $_->[0];
	# }
	
	# foreach (@comment_times){
		# push @times, $_->[0];
	# }
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

1;
