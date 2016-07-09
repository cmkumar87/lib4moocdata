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
my $coursera_dump_version = 2; #default
my $courseid;

$help = 1 unless GetOptions(
				'indb=s'		=> \$in_dbname,
				'outdb=s'		=> \$out_dbname,
				'course=s'		=> \$courseid,
				'version=i'		=> \$coursera_dump_version, 
				'debug'			=> \$debug,
				'h' 			=> \$help,
				'q' 			=> \$quite
			);
		
if ( $help ){
	Help();
	exit(0);
}

my $dbh			= Model::getDBHandle(undef,1,'mysql',$in_dbname);
my $litedbh		= Model::getDBHandle("$path/../data",1,undef,$out_dbname);

open( my $log, ">$path/../logs/$progname.err.log") 
		or die "\n Cannot open $path/../logs/$progname.err.log";

if(!defined $in_dbname){
	print "Exception. input dbname is undefined"; 
	print $log "Exception. input dbname is undefined"; 
	exit(0);
}
elsif(!defined $dbh){
	print "Exception. Db handle is undefined"; 
	print $log "Exception. Db handle is undefined"; 
	exit(0);
}

if(!defined $out_dbname){
	print "Exception. output dbname is undefined"; 
	print $log "Exception. output dbname is undefined"; 
	exit(0);
}
elsif(!defined $litedbh){
	print "Exception. Db handle is undefined"; 
	print $log "Exception. Db handle is undefined"; 
	exit(0);
}

if(!defined $courseid){
	print "Exception. courseid not defined"; 
	print $log "Exception. courseid not defined"; 
	exit(0);
}

print "Running $progname for course-$courseid \t couseradump version-$coursera_dump_version";
print $log "Running $progname for course-$courseid \t couseradump version-$coursera_dump_version";

my $threads_query		= "select id, forum_id, title, num_views, num_posts, 
									votes, instructor_replied, is_spam 
								from forum_threads where forum_id = ?";								
my $threadsth			= $dbh->prepare("$threads_query")
								or die "Couldn't prepare statement \n $threads_query \n $DBI::errstr\n";

my $threadinsert_query	= "Insert into thread (id, url, forumid, title, num_views, num_posts, 
												votes, inst_replied, courseid, is_spam) 
							values (?,?,?,?,?,?,?,?,?,?)";
							
my $threadinsertsth		= $litedbh->prepare($threadinsert_query)
								or die "Couldn't prepare statement \n $threadinsert_query \n $DBI::errstr\n";

my $postinsert_query	= "Insert into post (id,thread_id,original,post_order,
											post_text,votes,user,
											post_time,forumid,courseid) 
											values (?,?,?,?,?,?,?,?,?,?)";
my $postinsertsth		= $litedbh->prepare($postinsert_query) 
								or die "Couldn't prepare statement: \n $postinsert_query \n $DBI::errstr\n";

my $comminsert_query	= "Insert into comment (id,post_id,thread_id,comment_text,votes,
												user,post_time,forumid,courseid) 
												values (?,?,?,?,?,?,?,?,?)";
my $comminsertsth		= $litedbh->prepare($comminsert_query)
								or die "Couldn't prepare statement \n $comminsert_query \n $DBI::errstr\n";

my $userinsert_query	= "insert into user (id, user_title, postid, forumid, threadid,courseid) values (?,?,?,?,?,?)";
my $userinsertsth		= $litedbh->prepare($userinsert_query)
								or die "Couldn't prepare statement \n $userinsert_query \n $DBI::errstr\n";;
								
my $user_hashmap_select_query;
my $userid_hashvalue_map;

if($coursera_dump_version eq 1){
	# $user_hashmap_select_query	= "select user_id, anon_user_id, session_user_id, forum_user_id from hash_mapping";
	$user_hashmap_select_query	= "select anon_user_id, forum_user_id from hash_mapping";
	$userid_hashvalue_map 		=  $dbh->selectall_hashref($user_hashmap_select_query,'forum_user_id')
										or die "query failed: $user_hashmap_select_query \n $DBI::errstr";
}
elsif($coursera_dump_version eq 2){
	$user_hashmap_select_query	= "select user_id, session_user_id from hash_mapping";
	$userid_hashvalue_map		=  $dbh->selectall_hashref($user_hashmap_select_query,'user_id')
									or die "query failed: $user_hashmap_select_query \n $DBI::errstr";
}

my $user_accessgroup_query;
my $user_accessgroup_map;
if($coursera_dump_version eq 1){
	$user_accessgroup_query = "select anon_user_id, access_group_id from users";
	$user_accessgroup_map 	=  $dbh->selectall_hashref($user_accessgroup_query,'anon_user_id')
										or die "query failed: $user_accessgroup_query \n $DBI::errstr";
}
elsif($coursera_dump_version eq 2 ){
	$user_accessgroup_query = "select session_user_id, access_group_id from users";
	$user_accessgroup_map 	=  $dbh->selectall_hashref($user_accessgroup_query,'session_user_id')
										or die "query failed: $user_accessgroup_query \n $DBI::errstr";
}

my $user_title_query			= "select id, forum_title from access_groups";
my $user_title					=  $dbh->selectall_hashref($user_title_query,'id')
											or die "query failed: $user_title_query \n $DBI::errstr";

my $foruminsert_query			= "insert into forum (id, forumname, courseid) values(?,?,?)";
my $foruminsertsth				= $litedbh->prepare($foruminsert_query)
										or die "Couldn't prepare statement \n $foruminsert_query \n $DBI::errstr\n";
										
my $forum_query	= "select id, name from forum_forums";
my $forums		= $dbh->selectall_hashref($forum_query,'id') 
							or die "Couldn't prepare statement \n $forum_query \n $DBI::errstr\n";							
							
							
foreach my $forum_id (sort keys %$forums){
	# we are only interested in threads from lecture, exam, errata 
	# and homework (aka. assignment) threads
	if( $forum_id == 0 || $forum_id == -2 || $forum_id == 10001) {	next;	}
	
	print "\n Processing $forum_id of $courseid";
	print $log "\n Processing $forum_id of $courseid";
	
	my $forumname	= $forums->{$forum_id}{'name'};
	$foruminsertsth->execute($forum_id,$forumname,$courseid) 
					or die "Couldn't execute statement \n $foruminsert_query". 
						"\n args: 	forum-$forum_id||
									name-$forumname||
									course-$courseid \n" . $DBI::errstr;
	
	$threadsth->execute($forum_id)
		or die "\n Couldn't execute statement \n $threads_query \n $DBI::errstr";
						
	my $threads	= $threadsth->fetchall_hashref('id');
	
	if (keys %$threads < 1){
		print $log "\n No threads found. Skipping $forum_id";
		next;	
	}
	
	print $log "\n # of threads " . (keys %$threads);
	print "\n # of threads " . (keys %$threads);
	
	foreach my $threadid (keys %$threads){
		my $url = "https://class.coursera.org/$courseid/forum/thread?thread_id=$threadid";
		$threadinsertsth->execute($threadid,  $url, $forum_id, 
							$threads->{$threadid}{'title'}, $threads->{$threadid}{'num_views'},
							$threads->{$threadid}{'num_posts'}, $threads->{$threadid}{'votes'},
							$threads->{$threadid}{'instructor_replied'},
							$courseid, $threads->{$threadid}{'is_spam'} )
							or die "Couldn't execute statement ";

		my $posts  = $dbh->selectall_hashref("select * from forum_posts where thread_id = $threadid", 'id');
		print $log "\n $threadid -- \t # of posts " . (keys %$posts);
		foreach my $post (sort {$a<=>$b} keys %$posts){
			my $post_text = $posts->{$post}{'post_text'};
			$post_text =~ s/\<((br)|(BR))\s*\/>/ /g;
			$post_text =~ s/<.*?>/ /g;
			$post_text =~ s/\n|\r//g;
			$post_text =  decode_entities($post_text);
			my $user_id;
			if($coursera_dump_version eq 1){
				$user_id	= $posts->{$post}{'forum_user_id'};
			}
			elsif($coursera_dump_version eq 2 ){
				$user_id	=	$posts->{$post}{'user_id'};
			}
			
			$postinsertsth->execute( $posts->{$post}{'id'}, $threadid, 
						  $posts->{$post}{'original'}, $posts->{$post}{'post_order'}, 
						  $post_text,
						  $posts->{$post}{'votes'}, $user_id, 
						  $posts->{$post}{'post_time'}, $forum_id, $courseid						
					)
					or die "Couldn't execute statement \n  $postinsert_query".
							"\n args: post-$post || thread-$threadid || course-$courseid || forum-$forum_id " . $DBI::errstr;
			
			my $comments =  $dbh->selectall_hashref("select * from forum_comments where thread_id = $threadid and post_id = $post", 'id');
			foreach my $comment (sort {$a<=>$b} keys %$comments){
				my $cmnt_text = $comments->{$comment}{'comment_text'};
				$cmnt_text =~ s/\<((br)|(BR))\s*\/>/ /g;
				$cmnt_text =~ s/<.*?>/ /g;			
				$cmnt_text =~ s/\n|\r//g;
				$cmnt_text =  decode_entities($cmnt_text);
				my $user_id;
				if($coursera_dump_version eq 1){
					$user_id	= $comments->{$comment}{'forum_user_id'};	
				}
				elsif($coursera_dump_version eq 2 ){
					$user_id	= $comments->{$comment}{'user_id'};	
				}
				
				$comminsertsth->execute($comments->{$comment}{'id'}, $comments->{$comment}{'post_id'}, 
						$threadid, $comments->{$comment}{'comment_text'}, 
						$comments->{$comment}{'votes'}, $user_id,
						$comments->{$comment}{'post_time'}, 
						$forum_id, $courseid
					)	or die "Couldn't execute statement \n $comminsert_query".
								"\n args: $comments->{$comment}->{'id'}	|| 
									$comments->{$comment}->{'post_id'}	|| 
									thread-$comments->{$comment}->{'thread_id'}	|| 
									forum-$forum_id \n" . $DBI::errstr;
				
				my $session_user_id_hash;
				if($coursera_dump_version eq 1){
					$session_user_id_hash = $userid_hashvalue_map->{$user_id}{'anon_user_id'};
				}
				elsif($coursera_dump_version eq 2 ){
					$session_user_id_hash = $userid_hashvalue_map->{$user_id}{'session_user_id'};
				}
				
				my $access_group			= $user_accessgroup_map->{$session_user_id_hash}{'access_group_id'};
				my $user_title				= $user_title->{$access_group}{'forum_title'};
				
				$userinsertsth->execute($user_id,$user_title,$post,$forum_id,$threadid,$courseid) 
									or die "Couldn't execute statement \n $userinsert_query".
									"\n args: user-$user_id || 
											  post-$post|| thread-$threadid || 
											  course-$courseid \n" . $DBI::errstr;
			}

			my $session_user_id_hash;
			if($coursera_dump_version eq 1){
				$session_user_id_hash = $userid_hashvalue_map->{$user_id}{'anon_user_id'};
			}
			elsif($coursera_dump_version eq 2 ){
				$session_user_id_hash = $userid_hashvalue_map->{$user_id}{'session_user_id'};
			}
			my $access_group			= $user_accessgroup_map->{$session_user_id_hash}{'access_group_id'};
			my $user_title				= $user_title->{$access_group}{'forum_title'};
			
			$userinsertsth->execute($user_id,$user_title,$post,$forum_id,$threadid,$courseid) 
								or die "Couldn't execute statement \n $userinsert_query". 
									"\n args: 	user-$user_id||
												post-$post|| thread-$threadid ||
												course-$courseid \n" . $DBI::errstr;
		}
	}
}

close $log;

