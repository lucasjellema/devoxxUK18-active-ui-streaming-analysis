var kafka = require('kafka-node');


var tweetLikesAnalyticsListener = module.exports;
var subscribers = [];

tweetLikesAnalyticsListener.subscribeToTweetLikeAnalytics = function( callback) {
  subscribers.push(callback);
}
var kafkaHost = process.env.KAFKA_HOST || "192.168.188.102";
var zookeeperPort = process.env.ZOOKEEPER_PORT || 2181;
var TOPIC_NAME = process.env.KAFKA_TOPIC || 'WINDOWED_LIKE_COUNTS'; // corresponding to KSQL query create table like_counts  as select count(*) likeCount, tweetId from tweet_likes window tumbling (size 60 seconds) group by tweetId;


var consumerOptions = {
  host :kafkaHost+":"+zookeeperPort,
  groupId: 'consume-tweetLikeAnalytics-for-web-app',
    sessionTimeout: 15000,
    protocol: ['roundrobin'],
    encoding: 'buffer',
    fromOffset: 'earliest' // equivalent of auto.offset.reset valid values are 'none', 'latest', 'earliest'
  };
  
var topics = [TOPIC_NAME];
var consumerGroup = new kafka.ConsumerGroup(Object.assign({id: 'consumer1'}, consumerOptions), topics);
consumerGroup.on('error', onError);
consumerGroup.on('message', onMessage);

function onError (error) {
    console.error(error);
    console.error(error.stack);
  }
  
  function onMessage (message) {
    console.log('%s read msg Topic="%s" Partition=%s Offset=%d', this.client.clientId, message.topic, message.partition, message.offset);
    console.log("Message Key "+message.key);
    var likeCount = JSON.parse(message.value)
    console.log(likeCount)
    var tweetId = likeCount.TWEETID;
    var LIKE_COUNT = likeCount.LIKECOUNT;
    var timestamp = likeCount.TIMESTAMP;
//    {"nrs":[{"tweetId":"1495112906610001DCWw","conference":"oow17","count":27,"window":null},{"tweetId":"1496421364049001nhas","conference":"oow17","count":19,"window":null},{"tweetId":"1497393952355001DjRn","conference":"oow17","count":18,"window":null},null]}
    subscribers.forEach( (subscriber) => {
       subscriber(JSON.stringify({"tweetId": tweetId, "count":LIKE_COUNT, "windowTimestamp":timestamp }));
        
    })
  }
  
  process.once('SIGINT', function () {
    async.each([consumerGroup], function (consumer, callback) {
      consumer.close(true, callback);
    });
  });
