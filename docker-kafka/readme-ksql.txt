to demonstrate:

SET 'timestamp.extractor'='org.apache.kafka.streams.processor.WallclockTimestampExtractor';

list topics;

print 'tweets-topic';

CREATE TABLE twts ( eventType varchar,text varchar, isARetweet VARCHAR \
, author VARCHAR , hashtag VARCHAR  , createdAt VARCHAR, language VARCHAR \
, tweetId VARCHAR, tagFilter VARCHAR, originalTweetId VARCHAR) \
WITH (KAFKA_TOPIC='tweets-topic',VALUE_FORMAT='JSON', KEY='tweetId');


select * from tweets where tagFilter ='devoxxUK';

select * from tweet_count;

select TIMESTAMPTOSTRING(ROWTIME, 'yyyy-MM-dd HH:mm:ss.SSS') AS WINDOW_START \
, likeCount, tweetId from like_counts  ;

select count(*) likeCount, tweetId from tweet_likes window hopping  (size 30 seconds, advance by 10 seconds) group by tweetId;
create table like_count_hopping as select  count(*) likeCount, tweetId from tweet_likes window hopping  (size 30 seconds, advance by 10 seconds) group by tweetId;

select * from like_count_hopping;
===========================




SET 'timestamp.extractor'='org.apache.kafka.streams.processor.WallclockTimestampExtractor';

list topics;

SET 'auto.offset.reset' = 'earliest';

print 'tweets-topic';

CREATE TABLE tweets ( eventType varchar,text varchar, isARetweet VARCHAR, author VARCHAR , hashtag     VARCHAR  , createdAt VARCHAR, language VARCHAR, tweetId VARCHAR, tagFilter VARCHAR, originalTweetId VARCHAR) WITH (KAFKA_TOPIC='tweets-topic',VALUE_FORMAT='JSON', KEY='tweetId');

CREATE STREAM tweets_st (  tagFilter     VARCHAR  , createdAt VARCHAR, tweetId VARCHAR) WITH (KAFKA_TOPIC='tweets-topic',VALUE_FORMAT='JSON', KEY='tweetId');

select * from tweets;
select * from tweets where tagFilter ='java';

create table tweet_count with (partitions=1) as select tagFilter, count(*) as tag_cnt from tweets_st WINDOW tumbling (SIZE 120 minutes) group by tagFilter;

select * from tweet_count;


print 'tweet-like-topic' ;

CREATE STREAM tweet_likes ( tweetId VARCHAR) WITH (KAFKA_TOPIC='tweet-like-topic',VALUE_FORMAT='JSON', KEY='tweetId');

select * from tweet_likes;

select count(*), tweetId from tweet_likes group by tweetId;

create table like_counts  as select count(*) likeCount, tweetId from tweet_likes window tumbling (size 60 seconds) group by tweetId;

select count(*) likeCount, tweetId from tweet_likes window hopping (size 60 seconds, advance by 20 seconds) group by tweetId;

// Web App listens to topic based on windowed_like_counts! 
create table windowed_like_counts  with (partitions=1) as select rowtime as timestamp, TIMESTAMPTOSTRING(ROWTIME, 'yyyy-MM-dd HH:mm:ss.SSS') AS WINDOW_START, likeCount, tweetId from like_counts;

select TIMESTAMPTOSTRING(ROWTIME, 'yyyy-MM-dd HH:mm:ss.SSS') AS WINDOW_START , likeCount, tweetId from like_counts  ;





