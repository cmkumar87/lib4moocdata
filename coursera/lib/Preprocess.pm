package Preprocess;

use strict;
use warnings;

##
#
# Author : Muthu Kumar C
# Created in March, 2014
#
##

# Dependencies
#use Lingua::EN::Segmenter::TextTiling qw(segments);
#use Lingua::EN::StopWords qw(%StopWords); #174 words #fails on linux centos 6
use Lingua::EN::Sentence qw( get_sentences add_acronyms );
use Lingua::EN::Tokenizer::Offsets qw(token_offsets get_tokens);
use Lingua::StopWords  qw( getStopWords );
use Lingua::EN::StopWordList;
use Lingua::Stem::Snowball;
use Lingua::EN::Bigram;	#fails on linux centos 6
use Lingua::EN::Tagger;
use Lingua::EN::PluralToSingular 'to_singular';
use Config::Simple;

sub stem{
	my($tokens, $type) = @_;
	
	if(!defined $tokens || !defined $type){
		warn "Undefined tokens or stemmer type!";
		return $tokens;
	}
	
	my @stemmed_tokens;
	
	if('snow'){
		my $stemmer = Lingua::Stem::Snowball->new(
			lang     => 'en', 
			encoding => 'UTF-8',
		);
		die $@ if $@;
		foreach (@$tokens){
			push @stemmed_tokens, $stemmer->stem($_);
		}	
	}
	elsif('singular'){
		foreach (@$tokens){
			push @stemmed_tokens, to_singular($_);
		}
	}
	
	return \@stemmed_tokens;
}

# Initialization
sub getTokens{
	my($sentences, $strictness) = @_;
	my $tokens;

	foreach (@$sentences){
		if( $strictness eq 1 ){
			my $splitter = new Lingua::EN::Splitter;
			my @words = undef;
			push (@words, $splitter->words($_));

			foreach my $word (@words){
				foreach my $w (@$word){
					push @{$tokens}, $w;
				}
			}		
		}
		else{
				push @{$tokens}, @{get_tokens($_)} ;
		}
	}
	
	return $tokens;
}

sub getSentences{
	my ( $text ) = @_;
	my $sentences = get_sentences($text);
	return $sentences;
}

sub removeStopWords{
	my ( $tokens, $strictness) = @_;
	my @unstopped;
	my $sentid = 1;
	my %Config = ();
	Config::Simple->import_from('app.ini', \%Config)
								or die Config::Simple->error();
	#sanity check
	if( keys %Config eq 0){
		print "\n Exception: app.ini config not read properly"; exit(0);
	}
	
	#local variable
	my $stopwords;
	if( $strictness == 4){
		my %nonstopword = ();
		my $stopwordfile = $Config{'stopwordfile'};
		if(!defined $stopwordfile){
			die "Exception. Stopword file location not configured";
		}
		
		open (my $NONSTOP ,"<$stopwordfile") 
					or die "removeStopWords: cannot open nonstopwords.dict for reading";
		while (<$NONSTOP>){
			$_ =~ s/\s*(.*)\s*/$1/g;
			$nonstopword{$_} = 1;
		}
		
		#sanity check
		if (keys %nonstopword == 0){ 
			print "Exception: nonstopwords.dict not read into the hashtable. Exiting..\n"; 
			exit(0);
		}
		
		my %stopwordhash = ();
		foreach (@{Lingua::EN::StopWordList -> new -> words}){
			$_ =~ s/\s*(.*)\s*/$1/g;
			if (!exists $nonstopword{$_}){
				$stopwordhash{$_} =1;
			}
		}

		$stopwords = \%stopwordhash;
	}
	elsif( $strictness == 3){
		#Lingua::EN::StopWordList is a pure Perl module.
		#It returns a sorted arrayref of 659 English stop words.
		my %stopwordhash = ();
		foreach (@{Lingua::EN::StopWordList -> new -> words}){
			$stopwordhash{$_} =1;
		}
		$stopwords = \%stopwordhash;
	}
	# elsif( $strictness == 2 ){
		# $stopwords = \%StopWords;
	# }
	else{
		$stopwords = getStopWords('en');
	}
	
	foreach my $token (@$tokens){
		if ( !exists $stopwords->{$token} )
		{
			push @unstopped,$token;
		}
	}
	return \@unstopped ;
}

sub splitParentheses{
	my ($s) = @_;
	my @new_sentences;
	foreach (@$s){
		if( my @matches = $_ =~ /\((\s*[A-Za-z0-9].*?)\)/g ){
			$_ =~ s/\((\s*[A-Za-z0-9].*?)\)/ /g;
			foreach (@matches){
				if ( $_ !~ /(\s*[0-9].*?)/ ){
					#print "--$_\n";
					push @new_sentences,$_;
				}
			}
		}
	}
	return \@new_sentences;
}
	
sub removeOrphanCharacters{
	my($tokens) = @_;
	my @tokens_large;
	
	foreach (@$tokens){
		$_ =~ s/\s*//g;
		if(length ($_) >  1){
			push @tokens_large,$_;
		}
	}
	return \@tokens_large;
}

sub fixApostrophe{
	my ($sentences) = @_;

	foreach my $sentence (@$sentences){
		$sentence =~ s/([Nn])\'([tT])/$1$2/g;
		$sentence =~ s/([Ss])\'([sS])/$1$2/g;
		$sentence =~ s/([eE])\'([sS])/$1$2/g;
	}
	
	return $sentences;
}

sub getQuotes{
	my( $sentences ) = @_;
	my @quotes;
	foreach my $sentence (@$sentences) {
		if ($sentence =~ /\"(.*\s?.*)?\"/){
			push @quotes,$1;
		}
	}
	return \@quotes;
}

sub getURLs{
	my( $sentences ) = @_;
	my @urls = $sentences =~ /URL/g;
	return \@urls;
}

sub replaceTimeReferences{
	my ($text) = @_;
	$text =~ s/[0-9][0-9]?:[0-9][0-9]?/<TIMEREF>/g ;
	return $text;
}


sub replaceMath{
	my ($text) = @_;
	$text =~ s/\$\$.*?\$\$/ <MATH> /g ;
	$text =~ s/\(.*\(.*?\=.*?\)\)/ <MATH> /g ;
	$text =~ s/\\\(\\mathop.*?\\\)/ <MATH> /g;
	$text =~ s/\\\[\\mathop.*?\\\]/ <MATH> /g;
	$text =~ s/[A-Za-z]+\(.*?\)/ <MATH> /g;	#math functions
	$text =~ s/[A-Za-z]+\[.*?\]/ <MATH> /g;	#math functions
	$text =~ s/[0-9][\+\*\\\/\~][0-9]/ <MATH> /g; #binary expressions with operators
	$text =~ s/<MATH>\s*[\+\-\*\\\/\~][0-9]/ <MATH> /g; 
	
	$text =~ s/<MATH>\s*[\+\-\*\\\/\~\=]/ <MATH> /g;
	$text =~ s/[\+\-\*\\\/\~\=]\s*<MATH>/ <MATH> /g;
	
	$text =~ s/[\+\*\\\/\~]/ <MATH> /g;	#lone  math operators
	$text =~ s/(<MATH>\s*)+/ <MATH> /g;
	return $text;
}

sub replaceURL{
	my( $text ) = @_;	
	$text =~ s/https?\:\/\/[a-zA-Z0-9][a-zA-Z0-9\.\_\?\=\/\%\-\~\&]+/<URLREF>/g;
	return $text;
}

sub removeMarkers{
	my ($sentences) = @_;
	foreach my $sentence (@$sentences) {
		$sentence =~ s/\<PAR\>//g;
		$sentence =~ s/\<MATH\>/ MATH /g;
		$sentence =~ s/\<MATHFUNC\>/ MATHFUNC /g;
		$sentence =~ s/\<TIMEREF\>/ TIMEREF /g;
		$sentence =~ s/\<URLREF\>/ URLREF /g;
	}
	return $sentences;
}

sub removePunctuations{
	my ($sentences) = @_;
	foreach my $sentence (@$sentences){
		$sentence =~ s/[\"\,\'\?\!\_\&\=\:\\\/\<\>\(\)\[\]\{\}\%\@\#\!\*\+\-\^\.;\~\`]/ /g;
	}
	return $sentences;
}

sub normalizeParaMarker{
	my ($text) = @_;
	$text =~ s/<PAR>\s*<PAR>/<PAR>/g;
	$text =~ s/<PAR>/ <PAR> /g;
	return $text;
}

sub normalizeSpace{
	my ($text) = @_;
	$text =~ s/\s+/ /g;
	return $text;
}

sub normalizePeriods{
	my ($text) = @_;
	$text =~ s/\.+/\./g;
	$text =~ s/[\?\!]\.+/\./g;
	return $text;
}
1;