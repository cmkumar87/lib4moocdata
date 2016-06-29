#!/usr/bin/perl -w
use strict;
require 5.0;

##
#
# Author : Muthu Kumar C
# Created in June, 2014
#
##

use DBI;
use FindBin;
use Getopt::Long;
use Lingua::EN::Ngram;
use utf8::all;

my $path;	# Path to binary directory

BEGIN{
	if ($FindBin::Bin =~ /(.*)/) 
	{
		$path  = $1;
	}
}

use lib "$path/../lib";
use Preprocess;
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
  	print STDERR "       $progname -dbname [-density -thread -project -q ]\n";
	print STDERR "Options:\n";
	print STDERR "\t-thread \t specify a threadtype: 'inst' or 'nota'.\n";
	print STDERR "\t-q \tQuiet Mode (don't echo license).\n";
	print STDERR "\t-debug \tPrint additional debugging info to the terminal.\n";
}

my $help				= 0;
my $quite				= 0;
my $dbname				= undef;
my $courseid			= undef;
my $threadtype			= 'inst';
my $forumtype;
my $density_calculation = 1;
my $posttable 			= undef;
my $commenttable		= undef;
my $project				= 'intervention';
my $forumname;

$help = 1 unless GetOptions(
				'dbname=s'	=> \$dbname,
				'course=s'	=> \$courseid,
				'project=s'	=> \$project,
				'density'	=> \$density_calculation,
				'forum=s'	=> \$forumname,
				'thread=s'	=> \$threadtype,
				'h' 		=> \$help,
				'q' 		=> \$quite
			);

if ( $help || !defined $threadtype){
	Help();
	exit(0);
}

if (!$quite){
	License();
}

open (my $log, ">$path/../logs/$progname.log") 
				or die "cannot open file $path/../logs/$progname.log for writing";

if(!defined $courseid){
	print $log "\n Exception: courseid not defined"; 
	print "\n Exception: courseid not defined"; exit(0);
}
				
if(!defined $dbname){
	print $log "\n Exception: dbname not defined"; 
	print "\n Exception: dbname not defined"; exit(0);
}

my $dbh 			= Model::getDBHandle("$path/../data",1,undef,$dbname);

if(!defined $dbh){
	print $log "\n Exception: dbhandle not defined"; 
	print "\n Exception dbhandle not defined"; exit(0);
}

print $log "\n Using database file at $path/../data/$dbname";

my $forumidsquery	= "select id,courseid,forumname from forum ";
my @courses;

if ($project eq 'all'){
	print $log "\n Using data from all forumtypes";
	print "\n Using data from all forumtypes";
	$forumidsquery	.= "	where forumname in('General','Lecture','Homework','Exam',
											'Discussion','PeerA', 'Project')";
}
elsif ($project eq 'intervention'){
	print $log "\n Using data from Errata, Lecture, Homework, Exam forumtypes";
	print "\n Using data from Errata, Lecture, Homework, Exam forumtypes";
	$forumidsquery	.= "	where forumname in('Errata','Lecture','Homework','Exam')";
}
else{
	print $log "\n Using data from $forumname forumtypes";
	print "\n Using data from $forumname forumtypes";
	$forumidsquery	.= "	where forumname in(\'$forumname\')";
}

if(defined $courseid){
	push( @courses, $courseid );
	$forumidsquery = Model::appendListtoQuery($forumidsquery, \@courses, 'courseid ', 'and ');
}

my $forumrows = $dbh->selectall_arrayref($forumidsquery) 
								or die "Query failed! $DBI::errstr \n $forumidsquery \n";

if (!defined $forumrows || scalar @$forumrows eq 0){
	print $log "\n No forums selected. Exisitng..."; exit(0);
}

if($density_calculation){
	Model::updateInterventionDensity($dbh);
}

my $instpostqry = "select u.postid, p.post_time from user u, post p
					where user_title in (\"Instructor\",\"Staff\",
										 \"Coursera Staff\", \"Community TA\", 
										 \"Coursera Tech Support\"
										)
										and u.threadid = p.thread_id and u.courseid = p.courseid
										and u.id = p.user and u.forumid = p.forumid	and u.postid = p.id
										and u.threadid = ?
										and u.courseid = ?
										and u.forumid = ?";
										
my $instpoststh = $dbh->prepare($instpostqry)
						or die "Couldn't prepare user insert statement: " . $DBI::errstr;

my $instcmntqry = "select u.postid, p.post_time from user u, comment p
					where user_title in (\"Instructor\",\"Staff\",
										 \"Coursera Staff\", \"Community TA\", 
										 \"Coursera Tech Support\"
										) 
										and u.threadid = p.thread_id and u.courseid = p.courseid
										and u.id = p.user and u.forumid = p.forumid and u.postid = p.id
										and u.threadid = ? 
										and u.courseid = ?	
										and u.forumid = ?";
										
my $instcmntsth = $dbh->prepare($instcmntqry)
						or die "Couldn't prepare user insert statement: " . $DBI::errstr;						

my $postsquery = "select id, thread_id, original, post_order, url, post_text,
					votes, user, post_time, forumid, courseid
								  from post
								  where thread_id= ? 
								  and courseid=? and forumid=?
								  and post_time < ?
								  order by post_time";
		  
my $poststh = $dbh->prepare($postsquery)
					or die "Couldn't prepare user insert statement: " . $DBI::errstr;

my $commentsquery = "select id, post_id, thread_id, forumid, url, comment_text,
					  votes, user, post_time, user_name, courseid
					 from comment
					 where post_id=? and thread_id=? 
					 and courseid=? and forumid=? 
					 and post_time < ?
					 order by post_time";
	 
my $commentssth = $dbh->prepare($commentsquery) 
						or die "Couldn't prepare user insert statement: " . $DBI::errstr;
						
##############
if($threadtype eq 'inst'){
	$posttable = 'post2';
	$commenttable = 'comment2';
}
elsif($threadtype eq 'nota'){
	$posttable = 'post3';
	$commenttable = 'comment3';
}

my $countofthreads= "select count(1) from thread where forumid=? and courseid=?";
					 
my $postinsertquery = "Insert into $posttable (id,thread_id,original,post_order,
											url,post_text,votes,user,post_time,
											forumid,courseid) 
											values (?,?,?,?,?,?,?,?,?,?,?)";
my $postinsertsth = $dbh->prepare($postinsertquery) 
							or die "Couldn't prepare statement: $DBI::errstr\n";

my $comminsertquery = "Insert into $commenttable (id,post_id,thread_id,forumid,url,comment_text,votes,
												user,post_time,user_name,courseid) 
												values (?,?,?,?,?,?,?,?,?,?,?)";
my $comminsertsth = $dbh->prepare($comminsertquery) 
							or die "Couldn't prepare statement: $DBI::errstr\n";

my $notathreadqry = "select distinct threadid from user u
							where u.courseid = ? 
									and u.forumid = ?
									and u.user_title in (\"Instructor\", \"Staff\")";
my $notathreadsth = $dbh->prepare($notathreadqry)
							or die "Couldn't prepare statement: $DBI::errstr\n";

##############

foreach my $forumrow ( @$forumrows ){
	my $forumid		= @$forumrow[0];
	my $coursecode	= @$forumrow[1];
	$forumtype		= @$forumrow[2];
	my @threads;
	
	my $number_of_threads = @{$dbh->selectcol_arrayref($countofthreads,undef,$forumid,$coursecode)}[0];
	if( $number_of_threads == 0){ print $log "\n Skipping $coursecode \t $forumid "; next; }
	
	#Get all threads intervened by instructor or a Ta at least once
	if($threadtype eq 'inst'){
		@threads = @{Model::Getthreadids($dbh, $coursecode, $forumid, "inst_replied=1")};
	}
	elsif($threadtype eq 'nota'){
		$notathreadsth->execute($coursecode, $forumid) 
								or die "Couldn't execute statement: $DBI::errstr\n";
		@threads = @{$notathreadsth->fetchall_arrayref()};
	}
	
	print $log "\n Doing $forumid of $coursecode";
	
	foreach my $thread (@threads){
		my $threadid = $thread->[0];
		
		## Get instructor / TA posts for this thread
		$instpoststh->execute($threadid,$coursecode,$forumid) 
											or die $DBI::errstr;
		my $instposts = $instpoststh->fetchall_hashref('postid');
		
		$instcmntsth->execute($threadid,$coursecode,$forumid)
											or die $DBI::errstr; 
		
		my $instcmnts = $instcmntsth->fetchall_hashref('postid');
		
		if (!defined $instposts && !defined $instcmnts){
			print $log "Perhaps instposts and instcmnts query returned null for $forumid";
			next;
		}
		elsif ( (keys %$instposts == 0) && (keys %$instcmnts == 0) ){
			print $log "instposts and instcmnts query returned 0 for $forumid";
			next;
		}
		
		my $firstpostTime	= 99999999999;
		my $firstpost		= 999999999;
		foreach my $post (keys %$instposts){
			my $postTime 	= $instposts->{$post}->{'post_time'};
			($firstpostTime,$firstpost) = ($postTime < $firstpostTime) ? ($postTime,$post): ($firstpostTime,$firstpost)
		}
		print $log "\n $coursecode \t $threadid \t $firstpostTime \t $firstpost";
		
		foreach my $post (keys %$instcmnts){
			my $postTime 				= $instcmnts->{$post}->{'post_time'};
			($firstpostTime,$firstpost) = ($postTime < $firstpostTime) ? ($postTime,$post): ($firstpostTime,$firstpost);	
		}
		
		$poststh->execute($threadid,$coursecode,$forumid,$firstpostTime) 
											or die $DBI::errstr;
		my $posts = $poststh->fetchall_hashref('id');

		if ($poststh->rows == 0){
			print $log "\n No post records for $threadid $coursecode $forumid";
			next;
		}
		
		foreach my $post ( keys %$posts ){
			print $log "\n $coursecode ||$posts->{$post}->{'thread_id'} ||$posts->{$post}->{'id'}";
			 $postinsertsth->execute( $posts->{$post}->{'id'}, $threadid, 
									  $posts->{$post}->{'original'}, $posts->{$post}->{'post_order'}, 
									  $posts->{$post}->{'url'}, $posts->{$post}->{'post_text'}, 
									  $posts->{$post}->{'votes'}, $posts->{$post}->{'user'}, 
									  $posts->{$post}->{'post_time'}, $forumid, $coursecode						
								)
								or die "Couldn't execute statement ".
										"$post || $threadid || $coursecode || $forumid " . $DBI::errstr;
										
			$commentssth->execute($posts->{$post}->{'id'},$threadid,$coursecode,$forumid,$firstpostTime) 
									or die $DBI::errstr;
			my $comments = $commentssth->fetchall_hashref('id');
			if (keys %$comments == 0) {
				print $log "\n No cmnt records for $threadid $coursecode $forumid";
				next;
			}
			foreach my $comment ( keys %$comments ){
				print $log "\n $coursecode || $comments->{$comment}->{'id'} ||$comments->{$comment}->{'thread_id'} ||$comments->{$comment}->{'post_id'}";
				if ( !defined $coursecode || !defined $comments->{$comment}->{'id'} || !defined $comments->{$comment}->{'thread_id'} || !defined $comments->{$comment}->{'post_id'}){
					exit(0);
				}
				$comminsertsth->execute($comments->{$comment}->{'id'}, $comments->{$comment}->{'post_id'}, 
										$comments->{$comment}->{'thread_id'}, $forumid, 
										$comments->{$comment}->{'url'}, $comments->{$comment}->{'comment_text'}, 
										$comments->{$comment}->{'votes'},$comments->{$comment}->{'user'},
										$comments->{$comment}->{'post_time'}, 
										$comments->{$comment}->{'user_name'}, $coursecode
									)
					or die "Couldn't execute statement || ".
							"$forumid|| $comments->{$comment}->{'thread_id'} || ".
							"$comments->{$comment}->{'post_id'} || $comments->{$comment}->{'id'} ||" . 
							"$comments->{$comment}->{'url'}".$DBI::errstr;
			}
		}
	}
}

print $log "\n #Done#";
Utility::exit_script($progname,\@ARGV);
print "\n #Done#"