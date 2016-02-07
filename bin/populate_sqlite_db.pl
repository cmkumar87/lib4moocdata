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

# Run without -stem flag first and then with -stem flag
# to get a vector with all + stemmed versions

use DBI;
use FindBin;
use Getopt::Long;
use Encode;
use HTML::Entities;

my $path;	# Path to binary directory

BEGIN{
	if ($FindBin::Bin =~ /(.*)/) 
	{
		$path  = $1;
	}
}

use lib "$path/../lib";
use Model;

### USER customizable section
$0 =~ /([^\/]+)$/; my $progname = $1;
my $outputVersion = "1.0";
### END user customizable section

sub License{
	print STDERR "# Copyright 2015 \251 Muthu Kumar Chandrasekaran <cmkumar087\@gmail.com>\n";
}

sub Help{
	print STDERR "Usage: $progname -h\t[invokes help]\n";
  	print STDERR "       $progname [-q -debug]\n";
	print STDERR "Options:\n";
	print STDERR "\t-q \tQuiet Mode (don't echo license).\n";
	print STDERR "\t-debug \tPrint additional debugging info to the terminal.\n";
}

my $help				= 0;
my $quite				= 0;
my $debug				= 0;
my $in_dbname			= undef;
my $out_dbname			= undef;
my $courseid;

$help = 1 unless GetOptions(
				'indb=s'		=> \$in_dbname,
				'outdb=s'		=> \$out_dbname,
				'course=s'		=> \$courseid,
				'debug'			=> \$debug,
				'h' 			=> \$help,
				'q' 			=> \$quite
			);
		
if ( $help ){
	Help();
	exit(0);
}

my $dbh			= Model::getDBHandle(undef,1,'mysql',$in_dbname);
my $litedbh		= Model::getDBHandle("$path/../data/$out_dbname",0,undef,$out_dbname);

if(!defined $in_dbname){
	print "Exception. input dbname is undefined"; exit(0);
}
elsif(!defined $dbh){
	print "Exception. Db handle is undefined"; exit(0);
}

if(!defined $out_dbname){
	print "Exception. output dbname is undefined"; exit(0);
}
elsif(!defined $litedbh){
	print "Exception. Db handle is undefined"; exit(0);
}

if(!defined $courseid){
	print "Exception. courseid not defined"; exit(0);
}

my $threadsqry = "select id, forum_id, title, num_views, num_posts, votes, instructor_replied, 
												is_spam from forum_threads where forum_id = ?";								
my $threadsth	= $dbh->prepare("$threadsqry")
								or die "Couldn't prepare statement \n $threadsqry \n $DBI::errstr\n";

my $threadinsert_query = "Insert into thread (id, url, forumid, title, num_views, num_posts, 
												votes, inst_replied, courseid, is_spam) 
							values (?,?,?,?,?,?,?,?,?,?)";
							
my $threadinsertsth = $litedbh->prepare($threadinsert_query)
								or die "Couldn't prepare statement \n threadinsert_query \n $DBI::errstr\n";

my $postinsert_query = "Insert into post (id,thread_id,original,post_order,
											post_text,votes,user,
											post_time,forumid,courseid) 
											values (?,?,?,?,?,?,?,?,?,?)";
my $postinsertsth = $litedbh->prepare($postinsert_query) 
								or die "Couldn't prepare statement: $DBI::errstr\n";

my $comminsert_query = "Insert into comment (id,post_id,thread_id,forumid,comment_text,votes,
												user,post_time,user,courseid) 
												values (?,?,?,?,?,?,?,?,?,?)";
my $comminsertsth = $litedbh->prepare($comminsert_query)
								or die "Couldn't prepare statement \n $comminsert_query \n $DBI::errstr\n";

my $userinsert_qurey = "insert into user (id,postid,threadid,courseid) values (?,?,?,?)";
my $userinsertsth = $litedbh->prepare($userinsert_qurey)
								or die "Couldn't prepare statement \n $userinsert_qurey \n $DBI::errstr\n";;

my $foruminsert_query	= "insert into forum (id, forumname) values(?,?)";
my $forumsth			= $litedbh->prepare($foruminsert_query) 
								or die "Couldn't prepare statement \n $foruminsert_query \n $DBI::errstr\n";
			
my $forum_query	= "select id, forumname from forum where courseid = \'$courseid\'";
my $forums		= $litedbh->selectall_hashref($forum_query,'id') 
							or die "Couldn't prepare statement \n $forum_query \n $DBI::errstr\n";

open( my $log, ">$path/../logs/$progname.err.log") 
		or die "\n Cannot open $path/../logs/$progname.err.log";
							
foreach my $forum_id (sort keys %$forums){
	# we are only interested in threads from lecture, exam, errata 
	# and homework (aka. assignment) threads
	if( $forum_id == 0 || $forum_id == -2 || $forum_id == 10001) {	next;	}
	
	print "\n $forum_id";
	
	$threadsth->execute($forum_id)
		or die "\n Couldn't execute statement \n $threadsqry \n $DBI::errstr";
						
	my $threads	= $threadsth->fetchall_hashref('id');
	
	if (keys %$threads < 1){
		print $log "\n No threads found. Skipping $forum_id";
		next;	
	}
	
	print "\n # of threads " . (keys %$threads);
	
	foreach my $threadid (keys %$threads){
		my $url = "https://class.coursera.org/$courseid/forum/thread?thread_id=$threadid";
		$threadinsertsth->execute($threadid,  $url, $forum_id, 
							$threads->{$threadid}{'title'}, $threads->{$threadid}{'num_views'},
							$threads->{$threadid}{'num_posts'}, $threads->{$threadid}{'votes'},
							$threads->{$threadid}{'instructor_replied'},
							$courseid, $threads->{$threadid}{'is_spam'} )
							or die "Couldn't execute statement ";
		
		my $posts 	 = $dbh->selectall_hashref("select * from forum_posts where thread_id = $threadid", 'id');
		print "\n $threadid -- \t # of posts " . (keys %$posts);
		foreach my $post (sort {$a<=>$b} keys %$posts){
			my $post_text = $posts->{$post}{'post_text'};
			$post_text =~ s/\<((br)|(BR))\s*\/>/ /g;
			$post_text =~ s/<.*?>/ /g;
			$post_text =~ s/\n|\r//g;
			$post_text =  decode_entities($post_text);
			
			$postinsertsth->execute( $posts->{$post}->{'id'}, $threadid, 
						  $posts->{$post}->{'original'}, $posts->{$post}->{'post_order'}, 
						  $post_text,
						  $posts->{$post}->{'votes'}, $posts->{$post}->{'user_agent'}, 
						  $posts->{$post}->{'post_time'}, $forum_id, $courseid						
					)
					or die "Couldn't execute statement \n  $postinsertquery".
							"\n args: post-$post || thread-$threadid || course-$courseid || forum-$forum_id " . $DBI::errstr;
			
			my $comments =  $dbh->selectall_hashref("select * from forum_comments where thread_id = $threadid and post_id = $post", 'id');
			foreach my $comment (sort {$a<=>$b} keys %$comments){
				my $cmnt_text = $comments->{$comment}{'comment_text'};
				$cmnt_text =~ s/\<((br)|(BR))\s*\/>/ /g;
				$cmnt_text =~ s/<.*?>/ /g;			
				$cmnt_text =~ s/\n|\r//g;
				$cmnt_text =  decode_entities($cmnt_text);

				$comminsertsth->execute($comments->{$comment}->{'id'}, $comments->{$comment}->{'post_id'}, 
						$threadid, $forum_id,
						$comments->{$comment}->{'comment_text'}, 
						$comments->{$comment}->{'votes'},$comments->{$comment}->{'user'},
						$comments->{$comment}->{'post_time'}, 
						$comments->{$comment}->{'user_agent'}, $courseid
					)	or die "Couldn't execute statement \n $comminsertquery".
								"\n args: $comments->{$comment}->{'id'}	|| 
									$comments->{$comment}->{'post_id'}	|| 
									thread-$comments->{$comment}->{'thread_id'}	|| 
									forum-$forum_id \n" . $DBI::errstr;
								
				$userinsertsth->execute($comments->{$comment}{'user_id'},$post,$threadid,$courseid) 
									or die "Couldn't execute statement \n $userinsertqry".
									"\n args: user-$comments->{$comment}{'user_id' || 
											  post-$post|| thread-$threadid || 
											  course-$courseid \n" . $DBI::errstr;
			}
			$userinsertsth->execute($posts->{$post}{'user_id'},$post,$threadid,$courseid) 
								or die "Couldn't execute statement \n $userinsertqry". 
									"\n args: user-$posts->{$post}{'user_id'}||
												post-$post|| thread-$threadid ||
												course-$courseid \n" . $DBI::errstr;
		}
	}
}

close $log;