Here the link to the Docker Compose for Kafka:

https://gist.github.com/gschmutz/db582679c07c11f645b8cb9718e31209

It includes:

-          1x Zookeeper
-          1x Kafka Broker
-          1x Kafka Connect: http://localhost:8083
-          1x Schema Registry            
-          1x Landoop Schema-Registry UI: http://localhost:8002/
-          1x Landoop Connect UI: http://localhost:8001
-          1x KafkaManager: http://localhost:9000
-          1x KSQL Server

To start it, just do (first in docker-compose.yml replace the IP address of your Docker Host i.e. the Linux VM running the Docker Container):
replace all occurrences of 192.168.188.102 with the IP address assigned to the Docker Host (when using Vagrant, then this is the IP address specified in the Vagrantfile under config.vm.network "private_network)



export DOCKER_HOST_IP=192.168.188.102

docker-compose up -d

docker-compose logs -f


I have used https://gist.github.com/softinio/7e34eaaa816fd65f3d5cabfa5cc0b8ec

to first install vagrant plugin for docker compose
then to adapt vagrant file
then to run vagrant


vagrant ssh

cd /vagrant
docker-compose logs -f

check what is happening inside the DOcker Containers: Kafka is started

from within vagrant:

docker exec -it vagrant_ksql-server_1 /bin/bash

SET 'timestamp.extractor'='org.apache.kafka.streams.processor.WallclockTimestampExtractor';


CREATE TABLE tweets ( eventType varchar,text varchar, isARetweet VARCHAR, author VARCHAR , hashtag     VARCHAR  , createdAt VARCHAR, language VARCHAR, tweetId VARCHAR, tagFilter VARCHAR, originalTweetId VARCHAR) WITH (KAFKA_TOPIC='tweets-topic',VALUE_FORMAT='JSON', KEY='tweetId');

CREATE STREAM tweets_st (  tagFilter     VARCHAR  , createdAt VARCHAR, tweetId VARCHAR) WITH (KAFKA_TOPIC='tweets-topic',VALUE_FORMAT='JSON', KEY='tweetId');

select * from tweets;

SET 'auto.offset.reset' = 'earliest';

// for the current period of 5 minutes, running count the total number of occurrences per tagFilter
    select tagFilter, count(*) as tag_cnt \
from tweets_st window tumbling (size 5 minute) \
group by tagFilter;


// for the last 1 minute, running count the total number of occurrences per hashtag and update every 10 seconds
// every 10 seconds, we lose the entries that were gathered 1 minute ago 
// whenever a number tweet arrives for a categeory, it is added
select tagFilter, count(*) as tag_cnt \
from tweets WINDOW HOPPING (SIZE 1 minute, ADVANCE BY 30 SECONDS) \
group by tagFilter;


create table tweet_count_h with (partitions=1) as \
select tagFilter, count(*) as tag_cnt \
from tweets_st WINDOW HOPPING (SIZE 60 seconds, ADVANCE BY 10 SECONDS) \
group by tagFilter;

create table tweet_count with (partitions=1) as select tagFilter, count(*) as tag_cnt from tweets_st WINDOW tumbling (SIZE 120 minutes) group by tagFilter;

select * from tweet_count;



create table top3 as \
SELECT hashtag, TOPK(tag_cnt,3) top3_count FROM tweet_count GROUP BY hashtag;


note: drpo table or stream does not drop the underlying topic; if a table is created with the same name, the topic is reused - a new partition is created
dropping a topic for real is done in the Kafka Commandline tool

docker exec -it vagrant_broker-1_1 /bin/bash

kafka-topics --delete --zookeeper zookeeper:2181 --topic TWEET_COUNT 

CREATE STREAM tweet_likes ( tweetId VARCHAR) WITH (KAFKA_TOPIC='tweet-like-topic',VALUE_FORMAT='JSON', KEY='tweetId');
CREATE STREAM tweet_likes2 ( tweetId VARCHAR) WITH (KAFKA_TOPIC='tweet-like-topic',VALUE_FORMAT='JSON');

create stream enriched_likes as select tl.tweetId, t.text, t.rowtime from tweet_likes tl LEFT JOIN tweets t on tl.tweetId = t.tweetId; 
create stream enriched_likes2 with ( partitions=1) as select tl.tweetId, t.text, t.rowtime from tweet_likes tl LEFT JOIN tweets t on tl.tweetId = t.tweetId; 

select tl_tweetId, text, count(*) as like_count from enriched_likes window hopping (size 30 seconds, advance by 10 seconds) group by tl_tweetId, text;

select tl_tweetId, text, count(*) as like_count from enriched_likes2 window hopping (size 30 seconds, advance by 10 seconds) group by tl_tweetId, text;

print 'tweet-like-topic';

select * from tweet_likes;



select count(*), tweetId from tweet_likes group by tweetId;

create table like_counts  as select count(*) likeCount, tweetId from tweet_likes window tumbling (size 60 seconds) group by tweetId;

select count(*) likeCount, tweetId from tweet_likes window hopping (size 60 seconds, advance by 20 seconds) group by tweetId;


// Web App listens to topic based on windowed_like_counts! 
create table windowed_like_counts  as select rowtime as timestamp, TIMESTAMPTOSTRING(ROWTIME, 'yyyy-MM-dd HH:mm:ss.SSS') AS WINDOW_START, likeCount, tweetId from like_counts;

select TIMESTAMPTOSTRING(ROWTIME, 'yyyy-MM-dd HH:mm:ss.SSS') AS WINDOW_START , likeCount, tweetId from like_counts  ;

// as many results are produced as there are concurrent windows 
// there are as many concurrent windows as the number of times the hop advance fits in the size 
// the aggregate is returned for each of these windows - the oldest (lasting as long as size), and the lastes (only as long as advance)
create table tweet_like_counts_h  as select count(*) likeCount, tweetId from tweet_likes window hopping (size 60 seconds, advance by 20 seconds) group by tweetId;

select * from tweet_like_counts_h;


// gives null pointer exception:
select twl.likeCount, twl.tweetId, t.text from tweet_like_counts twl  LEFT JOIN tweets t on twl.tweetId = t.tweetId;


// clear topic:

kafka-configs --zookeeper vagrant_zookeeper_1:2181 --entity-type topics --alter --add-config retention.ms=1000 --entity-name tweets-topic

kafka-configs --zookeeper vagrant_zookeeper_1:2181 --entity-type topics --describe --entity-name tweets-topic

kafka-configs --zookeeper vagrant_zookeeper_1:2181 --entity-type topics --alter --delete-config retention.ms  --entity-name tweets-topic
