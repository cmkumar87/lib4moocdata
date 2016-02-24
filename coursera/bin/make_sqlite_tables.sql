/*  # install sqlite
	# run sqlite3 ..\data\<dbname>.db
	# sqlite3> read make_sqlite_tables.sql */

DROP TABLE IF EXISTS "forum";
CREATE TABLE "forum" (id integer, forumname text, courseid, courseraversion integer, downloaded integer, 
dataset text, numthreads integer, numinter integer, 
primary key(id, courseid));

DROP TABLE IF EXISTS "thread";
CREATE TABLE "thread" (id integer,title text,url text,num_views integer,num_posts integer,has_resolved integer,
inst_replied integer,is_spam integer,stickied integer,starter integer,last_poster integer,votes integer, 
courseid text, forumid integer, errorflag int, tagids, docid integer, resolved, deleted, approved, posted_time integer, 
primary key(url));

DROP TABLE IF EXISTS "post";
CREATE TABLE post(id integer, thread_id integer, original integer, post_order integer, url text,post_text text,
votes integer,user integer,post_time real, forumid integer,courseid integer, errorflag int,  
primary key(id,thread_id,forumid,courseid));

DROP TABLE IF EXISTS "comment";
CREATE TABLE comment(id integer, post_id integer, thread_id integer, forumid integer, url text, comment_text text, 
votes integer, user integer ,post_time integer,user_name text, courseid, 
primary key(id,post_id,thread_id,forumid,courseid));

DROP TABLE IF EXISTS "post2";
CREATE TABLE post2(id integer, thread_id integer, original integer, post_order integer, url text,post_text text,
votes integer,user integer,post_time real, forumid integer,courseid integer, errorflag int,  
primary key(id,thread_id,forumid,courseid));

DROP TABLE IF EXISTS "comment2";
CREATE TABLE comment2(id integer, post_id integer, thread_id integer, forumid integer, url text, comment_text text, 
votes integer, user integer,post_time integer,user_name text, courseid, 
primary key(id,post_id,thread_id,forumid,courseid));

DROP TABLE IF EXISTS "user";
CREATE TABLE user (id integer, full_name text, anonymous integer,user_profile text, user_title, postid integer, 
threadid integer, forumid integer, courseid);

DROP TABLE IF EXISTS "tags";
CREATE TABLE tags (tagid integer, tagname text, courseid integer, primary key(tagid,courseid));

DROP TABLE IF EXISTS "termFreqC14inst";
CREATE TABLE termFreqC14inst (termid integer, threadid integer, courseid text, term text, tf integer, type text, 
stem interger, stopword integer, commentid, postid integer, ispost, 
primary key(termid,postid,commentid,threadid,courseid));

DROP TABLE IF EXISTS "termFreqC14noinst";
CREATE TABLE termFreqC14noinst (termid integer, threadid integer, courseid text, term text, tf integer, type text, 
stem interger, stopword integer, commentid, postid integer, ispost, 
primary key(termid,postid,commentid,threadid,courseid));

DROP TABLE IF EXISTS "termdf";
CREATE TABLE termdf (termid integer, term text, df integer, idf real, stem integer, stopword integer, 
courseid, forumid, forumname, primary key (termid,courseid,forumid));

DROP TABLE IF EXISTS "termIDF";
CREATE TABLE termIDF (termid integer, term text, df integer, idf real, stem integer, stopword integer, courseid);